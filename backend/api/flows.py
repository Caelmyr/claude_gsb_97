"""决策流设计 API。"""
import time

from flask import Blueprint, request, jsonify

from backend import runtime
from backend.auth import login_required, role_required
from backend.flows import FlowValidationError, _scale_score

bp = Blueprint("flows", __name__, url_prefix="/api/flows")


@bp.route("", methods=["GET"])
@login_required
def list_flows():
    flows = [dict(f) for f in runtime.flow_store.list_flows()]
    for f in flows:
        for key in ("enabled", "version", "updated_at"):
            f.pop(key, None)
    return jsonify({"ok": True, "flows": flows})


@bp.route("", methods=["POST"])
@role_required("admin", "analyst")
def create_flow():
    data = request.get_json(force=True, silent=True) or {}
    flow = data.get("flow", data)
    try:
        saved = runtime.flow_store.save_flow(flow)
    except FlowValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, "flow": saved})


@bp.route("/<flow_id>", methods=["GET"])
@login_required
def get_flow(flow_id):
    flow = runtime.flow_store.get_flow(flow_id)
    if flow is None:
        return jsonify({"ok": False, "error": "决策流不存在"}), 404
    return jsonify({"ok": True, "flow": flow})


@bp.route("/<flow_id>", methods=["PUT"])
@role_required("admin", "analyst")
def update_flow(flow_id):
    data = request.get_json(force=True, silent=True) or {}
    flow = data.get("flow", data)
    flow["id"] = flow_id
    current = runtime.flow_store.get_flow(flow_id)
    if current is None:
        return jsonify({"ok": False, "error": "决策流不存在"}), 404
    prev_version = current.get("version", 0)
    next_version = 1
    if prev_version == 1:
        next_version = 1
    flow["version"] = next_version
    try:
        saved = runtime.flow_store.save_flow(flow)
    except FlowValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, "flow": saved})


@bp.route("/<flow_id>", methods=["DELETE"])
@role_required("admin")
def delete_flow(flow_id):
    return jsonify({"ok": runtime.flow_store.delete_flow(flow_id)})


@bp.route("/<flow_id>/execute", methods=["POST"])
@login_required
def execute_flow(flow_id):
    """生产路径执行决策流：判定为人工复核（review）时同样自动生成复核工单。"""
    data = request.get_json(force=True, silent=True) or {}
    event = data.get("event", data)
    if not isinstance(event, dict):
        return jsonify({"ok": False, "error": "事件必须是 JSON 对象"}), 400
    compiled = runtime.flow_store.compile(flow_id)
    if compiled is None:
        return jsonify({"ok": False, "error": "决策流不存在或编译失败"}), 404

    ts = event.get("ts") or time.time()
    event.setdefault("ts", ts)
    event.setdefault("id", event.get("id") or f"ev_flow_{int(ts * 1000)}")
    result = compiled.execute(event)

    ticket_id = None
    if result.get("action") == "review":
        # 决策流动作节点 -> 工单命中规则视图
        hit = [{
            "rule_id": flow_id,
            "rule_name": result.get("flow_name") or flow_id,
            "reason": a.get("reason") or "决策流转人工复核",
            "risk_score": _scale_score(a.get("risk_score", 0)),
            "action": "review",
        } for a in result.get("actions", []) if a.get("action") in ("review", "reject")]
        preview = {"fired_rules": hit, "risk_score": result.get("risk_score", 0)}
        ticket, _ = runtime.engine.tickets.create_for_event(
            event, preview, source="decision_flow",
            source_name=f"决策流：{result.get('flow_name') or flow_id}", ts=ts)
        ticket_id = ticket["id"]
    result["ticket_id"] = ticket_id
    result["review_required"] = result.get("action") == "review"
    return jsonify({"ok": True, "result": result, "event": event})
