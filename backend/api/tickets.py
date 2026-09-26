"""人工复核工单 API。

- GET  /api/tickets            多条件筛选（状态/处理人/时间范围/超时/关键词）+ 分页
- GET  /api/tickets/stats      工单看板统计（按状态、超时数）
- GET  /api/tickets/<id>       工单详情（含命中规则、事件快照、流转轨迹）
- POST /api/tickets/<id>/accept   受理（待受理 -> 处理中）
- POST /api/tickets/<id>/approve  通过（-> 已通过，沉淀处置结果）
- POST /api/tickets/<id>/reject   驳回（-> 已驳回，沉淀处置结果）
- POST /api/tickets/<id>/close    关闭（-> 已关闭）
"""
from flask import Blueprint, request, jsonify

from backend import runtime
from backend.auth import login_required, role_required, current_user
from backend.ticket_store import TicketError, ALL_STATUSES

bp = Blueprint("tickets", __name__, url_prefix="/api/tickets")


def _operator():
    user = current_user()
    return user.get("username") if user else None


def _parse_time(name):
    raw = request.args.get(name)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


@bp.route("", methods=["GET"])
@login_required
def list_tickets():
    status = request.args.get("status")
    if status and status not in ALL_STATUSES:
        return jsonify({"ok": False, "error": "非法状态"}), 400
    assignee = request.args.get("assignee")
    handler = request.args.get("handler")     # 处理人（受理人或最终处理人）
    overdue_raw = request.args.get("overdue")
    overdue = overdue_raw in ("1", "true", "True", "yes") if overdue_raw else None
    keyword = request.args.get("keyword")
    try:
        page = max(1, int(request.args.get("page", 1)))
        page_size = max(1, min(200, int(request.args.get("page_size", 20))))
    except (TypeError, ValueError):
        page, page_size = 1, 20

    # handler：受理人或最终处理人任一命中
    total, items = runtime.ticket_store.list_tickets(
        status=status,
        assignee=assignee,
        operator=request.args.get("operator"),
        handler=handler,
        start=_parse_time("start"),
        end=_parse_time("end"),
        overdue=overdue,
        keyword=keyword,
        page=page, page_size=page_size,
    )
    return jsonify({"ok": True, "total": total, "tickets": items,
                    "page": page, "page_size": page_size})


@bp.route("/stats", methods=["GET"])
@login_required
def ticket_stats():
    return jsonify({"ok": True, "stats": runtime.ticket_store.stats()})


@bp.route("/<ticket_id>", methods=["GET"])
@login_required
def get_ticket(ticket_id):
    ticket = runtime.ticket_store.get(ticket_id)
    if ticket is None:
        return jsonify({"ok": False, "error": "工单不存在"}), 404
    # 附加事件最终处置沉淀（与工单结论一致）
    disposition = runtime.engine.get_disposition(ticket.get("event_id"))
    if disposition:
        ticket["disposition"] = disposition
    ticket = runtime.ticket_store._decorate(ticket)
    return jsonify({"ok": True, "ticket": ticket})


def _do_transition(action, ticket_id):
    data = request.get_json(force=True, silent=True) or {}
    comment = data.get("comment", "")
    try:
        ticket = getattr(runtime.ticket_store, action)(
            ticket_id, operator=_operator(), comment=comment)
    except TicketError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    # 通过 / 驳回：把结果作为事件最终处置结论沉淀，并联动关闭相关告警
    if action in ("approve", "reject"):
        runtime.engine.record_disposition(ticket)
    return jsonify({"ok": True, "ticket": runtime.ticket_store._decorate(ticket)})


@bp.route("/<ticket_id>/accept", methods=["POST"])
@role_required("admin", "analyst")
def accept_ticket(ticket_id):
    return _do_transition("accept", ticket_id)


@bp.route("/<ticket_id>/approve", methods=["POST"])
@role_required("admin", "analyst")
def approve_ticket(ticket_id):
    return _do_transition("approve", ticket_id)


@bp.route("/<ticket_id>/reject", methods=["POST"])
@role_required("admin", "analyst")
def reject_ticket(ticket_id):
    return _do_transition("reject", ticket_id)


@bp.route("/<ticket_id>/close", methods=["POST"])
@role_required("admin", "analyst")
def close_ticket(ticket_id):
    return _do_transition("close", ticket_id)
