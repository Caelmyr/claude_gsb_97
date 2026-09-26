"""人工复核工单 API：查询筛选、受理 / 通过 / 驳回 / 关闭、统计、处理人列表。"""
from flask import Blueprint, request, jsonify

from backend import runtime
from backend.auth import login_required, role_required, current_user
from backend.engine.ticket import TicketError

bp = Blueprint("tickets", __name__, url_prefix="/api/tickets")


def _parse_filters():
    """从查询串解析筛选条件（状态 / 处理人 / 时间范围 / 来源 / 超时 / 关键词 / 分页）。"""
    def _ts(name):
        raw = request.args.get(name)
        if not raw:
            return None
        try:
            return float(raw)   # epoch 秒（前端由 datetime-local 转换后提交）
        except (TypeError, ValueError):
            return None

    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 20, type=int)
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 20
    page_size = min(page_size, 200)
    return {
        "status": request.args.get("status") or None,
        "assignee": request.args.get("assignee") or None,
        "source": request.args.get("source") or None,
        "start": _ts("start"),
        "end": _ts("end"),
        "keyword": request.args.get("keyword") or None,
        "overdue": request.args.get("overdue") == "1",
        "page": page,
        "page_size": page_size,
    }


@bp.route("", methods=["GET"])
@login_required
def list_tickets():
    total, items = runtime.engine.tickets.list_tickets(**_parse_filters())
    return jsonify({"ok": True, "total": total, "tickets": items})


@bp.route("/<ticket_id>", methods=["GET"])
@login_required
def get_ticket(ticket_id):
    ticket = runtime.engine.tickets.get(ticket_id)
    if ticket is None:
        return jsonify({"ok": False, "error": "工单不存在"}), 404
    return jsonify({"ok": True, "ticket": ticket})


def _do_transition(ticket_id, action):
    data = request.get_json(force=True, silent=True) or {}
    # 记录实际处理人（登录用户的登录名）
    user = current_user()
    handler = user.get("username") if user else None
    try:
        ticket = runtime.engine.tickets.transition(
            ticket_id, action,
            handler=handler, comment=data.get("comment", ""))
    except TicketError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, "ticket": ticket})


@bp.route("/<ticket_id>/accept", methods=["POST"])
@role_required("admin", "analyst", "viewer")
def accept_ticket(ticket_id):
    return _do_transition(ticket_id, "accept")


@bp.route("/<ticket_id>/approve", methods=["POST"])
@role_required("admin", "analyst", "viewer")
def approve_ticket(ticket_id):
    return _do_transition(ticket_id, "approve")


@bp.route("/<ticket_id>/reject", methods=["POST"])
@role_required("admin", "analyst", "viewer")
def reject_ticket(ticket_id):
    return _do_transition(ticket_id, "reject")


@bp.route("/<ticket_id>/close", methods=["POST"])
@role_required("admin", "analyst", "viewer")
def close_ticket(ticket_id):
    return _do_transition(ticket_id, "close")


@bp.route("/stats", methods=["GET"])
@login_required
def ticket_stats():
    return jsonify({"ok": True, "stats": runtime.engine.tickets.stats()})


@bp.route("/assignees", methods=["GET"])
@login_required
def list_assignees():
    """处理人筛选候选：系统用户 + 工单中出现过的处理人。"""
    return jsonify({"ok": True, "assignees": runtime.engine.tickets.list_assignees()})
