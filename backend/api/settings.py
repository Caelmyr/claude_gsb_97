"""系统设置 API。"""
from flask import Blueprint, request, jsonify

from backend import runtime
from backend.auth import role_required, login_required
from backend.settings_store import get_settings, save_settings

bp = Blueprint("settings", __name__, url_prefix="/api/settings")


@bp.route("", methods=["GET"])
@login_required
def read_settings():
    settings = get_settings()
    for key in ("event_types", "site_name"):
        settings.pop(key, None)
    return jsonify({"ok": True, "settings": settings})


@bp.route("", methods=["PUT"])
@role_required("admin")
def write_settings():
    patch = request.get_json(force=True, silent=True) or {}
    merged = save_settings(patch)
    # 若切换了匹配模式，同步重建引擎网络
    if "engine" in patch and "mode" in patch.get("engine", {}):
        runtime.engine.registry.set_mode(patch["engine"]["mode"])
    # 工单 SLA / 生效决策流即时生效
    ticket_patch = patch.get("ticket") or {}
    if runtime.ticket_store is not None and ticket_patch.get("sla_hours"):
        runtime.ticket_store.update_sla(ticket_patch["sla_hours"])
    # 生效决策流变更：立即失效引擎侧缓存，避免读旧值
    if "ticket" in patch and hasattr(runtime.engine, "_af_cache"):
        runtime.engine._af_cache = None
    return jsonify({"ok": True, "settings": merged})
