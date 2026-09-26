"""人工复核工单存储与状态机。

当风控规则或决策流把事件判定为「人工复核（review）」时，引擎自动生成一条复核
工单，进入工单中心供风控人员处理。

工单状态机：
    pending（待受理）── 受理 ──> processing（处理中）
    processing        ── 通过 ──> approved（已通过）   [终态]
    processing        ── 驳回 ──> rejected（已驳回）   [终态]
    pending/processing ── 关闭 ─> closed（已关闭）     [终态]

终态（approved/rejected/closed）不可再变更。

工单持久化为单个 JSON 文件（tickets/tickets.json），读写沿用 storage 的
原子写 + 文件锁；进程内再用 RLock 保护读-改-写复合操作。

处置沉淀：通过/驳回时写入 ``result``（approved/rejected）、处理意见、处理人与
处理时间，作为该事件的最终人工处置结论，与引擎自动决策区分（``source_decision``
记录引擎给出的初始动作，恒为 review）。
"""
import threading
import time

from backend import config
from backend.storage import atomic_write_json, read_json

# 状态定义
PENDING = "pending"
PROCESSING = "processing"
APPROVED = "approved"
REJECTED = "rejected"
CLOSED = "closed"

ALL_STATUSES = (PENDING, PROCESSING, APPROVED, REJECTED, CLOSED)
TERMINAL_STATUSES = (APPROVED, REJECTED, CLOSED)
OPEN_STATUSES = (PENDING, PROCESSING)

# 终态集合（已处理，不再计入超时）
_TERMINAL = set(TERMINAL_STATUSES)

# 合法状态迁移
_TRANSITIONS = {
    "accept": {PENDING: PROCESSING},
    "approve": {PROCESSING: APPROVED, PENDING: APPROVED},
    "reject": {PROCESSING: REJECTED, PENDING: REJECTED},
    "close": {PENDING: CLOSED, PROCESSING: CLOSED},
}

STATUS_LABELS = {
    PENDING: "待受理",
    PROCESSING: "处理中",
    APPROVED: "已通过",
    REJECTED: "已驳回",
    CLOSED: "已关闭",
}

# 操作 -> 目标状态，便于校验
_ACTIONS = {"accept": PROCESSING, "approve": APPROVED,
            "reject": REJECTED, "close": CLOSED}


class TicketError(ValueError):
    """工单操作非法（不存在 / 状态不允许流转）。"""


class TicketStore:
    """复核工单的创建、查询与状态流转。"""

    def __init__(self, sla_hours=24):
        self.sla_sec = float(sla_hours) * 3600.0 if sla_hours else 86400.0
        self._lock = threading.RLock()
        self._tickets = {}        # id -> ticket dict（内存源）
        self._seq = 0
        self._load()

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    def _load(self):
        data = read_json(config.TICKETS_FILE, {"tickets": [], "seq": 0})
        for t in data.get("tickets", []):
            if t.get("id"):
                self._tickets[t["id"]] = t
        self._seq = int(data.get("seq", 0) or 0)

    def _persist_locked(self):
        atomic_write_json(config.TICKETS_FILE, {
            "tickets": list(self._tickets.values()),
            "seq": self._seq,
        })

    def update_sla(self, sla_hours):
        """运行时调整超时阈值（秒级换算），不影响已记录的时间戳。"""
        if sla_hours:
            self.sla_sec = float(sla_hours) * 3600.0

    @staticmethod
    def status_label(status):
        return STATUS_LABELS.get(status, status)

    # ------------------------------------------------------------------
    # 创建（由引擎在 review 决策时调用）
    # ------------------------------------------------------------------
    def create_from_decision(self, event, decision, source="rule_engine",
                             flow_id=None, flow_name=None, ts=None):
        """根据事件与引擎决策创建一条待受理工单。

        返回 (ticket, created)。同一事件若已生成过工单则去重返回既有工单。
        """
        if ts is None:
            ts = event.get("ts") or time.time()
        event_id = event.get("id")

        # 命中规则概要
        fired = decision.get("fired_rules", []) or []
        hit_rules = [{
            "rule_id": r.get("rule_id"),
            "rule_name": r.get("rule_name") or r.get("reason"),
            "reason": r.get("reason"),
            "risk_score": r.get("risk_score", 0),
            "action": r.get("action"),
        } for r in fired]

        # 主体信息：常见主体维度 + 事件中的其余标识
        subject = {}
        for f in ("user_id", "ip", "device_id", "account", "merchant_id"):
            if event.get(f) is not None:
                subject[f] = event.get(f)

        now = ts
        with self._lock:
            # 事件去重：同一事件只生成一张工单
            if event_id:
                for t in self._tickets.values():
                    if t.get("event_id") == event_id:
                        return t, False

            self._seq += 1
            seq = self._seq
            ticket_id = f"tk_{time.strftime('%Y%m%d', time.localtime(now))}_{seq:05d}"
            ticket = {
                "id": ticket_id,
                "seq": seq,
                "status": PENDING,
                "title": self._build_title(event, hit_rules),
                # 事件概要
                "event_id": event_id,
                "event_type": event.get("type"),
                "event_summary": self._summarize_event(event),
                "event_snapshot": event,
                # 命中规则与风险
                "hit_rules": hit_rules,
                "risk_score": int(decision.get("risk_score", 0) or 0),
                # 主体
                "subject": subject,
                # 来源（规则引擎 / 决策流）
                "source": source,
                "flow_id": flow_id,
                "flow_name": flow_name,
                "source_decision": "review",
                # 时间
                "event_time": now,
                "created_at": now,
                "accepted_at": None,
                "processed_at": None,
                "closed_at": None,
                "deadline": now + self.sla_sec,
                # 处理人 / 意见
                "assignee": None,
                "operator": None,
                "comment": "",
                # 处置沉淀结果（终态时为 approved / rejected / closed）
                "result": None,
                # 流转轨迹
                "history": [{
                    "action": "create",
                    "from": None,
                    "to": PENDING,
                    "operator": "system",
                    "comment": "风控判定为人工复核，自动生成工单",
                    "ts": now,
                }],
            }
            self._tickets[ticket_id] = ticket
            self._persist_locked()
            return ticket, True

    @staticmethod
    def _build_title(event, hit_rules):
        et = event.get("type") or "事件"
        target = event.get("user_id") or event.get("ip") or event.get("account") or "-"
        if hit_rules:
            reason = hit_rules[0].get("reason") or hit_rules[0].get("rule_name") or "人工复核"
        else:
            reason = "人工复核"
        return f"[{et}] {reason} · 主体 {target}"

    @staticmethod
    def _summarize_event(event):
        """提取关键字段构成人类可读的事件概要。"""
        parts = []
        for label, key in (("类型", "type"), ("用户", "user_id"), ("IP", "ip"),
                           ("设备", "device_id"), ("渠道", "channel"),
                           ("国家/地区", "country"), ("金额", "amount")):
            val = event.get(key)
            if val is not None and val != "":
                parts.append(f"{label}={val}")
        return "，".join(parts)

    # ------------------------------------------------------------------
    # 状态流转
    # ------------------------------------------------------------------
    def _transition(self, ticket_id, action, operator=None, comment=""):
        if action not in _ACTIONS:
            raise TicketError(f"未知操作: {action}")
        comment = (comment or "").strip()
        with self._lock:
            ticket = self._tickets.get(ticket_id)
            if ticket is None:
                raise TicketError("工单不存在")
            cur = ticket.get("status")
            allowed = _TRANSITIONS[action]
            if cur not in allowed:
                label = STATUS_LABELS.get(cur, cur)
                raise TicketError(f"当前状态为「{label}」，不允许该操作")
            dest = allowed[cur]
            now = time.time()

            ticket["status"] = dest
            if action == "accept":
                ticket["accepted_at"] = now
                ticket["assignee"] = operator or ticket.get("assignee")
            elif dest in (APPROVED, REJECTED):
                # 直接在待受理态通过/驳回：自动补受理信息
                if ticket.get("accepted_at") is None:
                    ticket["accepted_at"] = now
                    ticket["assignee"] = operator or ticket.get("assignee")
                ticket["processed_at"] = now
                ticket["operator"] = operator
                ticket["comment"] = comment
                ticket["result"] = dest
            elif action == "close":
                ticket["closed_at"] = now
                ticket["operator"] = operator or ticket.get("operator")
                if comment:
                    ticket["comment"] = comment
                ticket["result"] = CLOSED

            ticket["history"].append({
                "action": action,
                "from": cur,
                "to": dest,
                "operator": operator,
                "comment": comment,
                "ts": now,
            })
            self._persist_locked()
            return dict(ticket)

    def accept(self, ticket_id, operator=None, comment=""):
        return self._transition(ticket_id, "accept", operator, comment)

    def approve(self, ticket_id, operator=None, comment=""):
        return self._transition(ticket_id, "approve", operator, comment)

    def reject(self, ticket_id, operator=None, comment=""):
        return self._transition(ticket_id, "reject", operator, comment)

    def close(self, ticket_id, operator=None, comment=""):
        return self._transition(ticket_id, "close", operator, comment)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    @staticmethod
    def _is_overdue(ticket, now=None):
        """未终态且超过 deadline 即超时。"""
        if ticket.get("status") in _TERMINAL:
            return False
        now = now if now is not None else time.time()
        return now >= ticket.get("deadline", now)

    def get(self, ticket_id):
        with self._lock:
            t = self._tickets.get(ticket_id)
            return dict(t) if t else None

    def list_tickets(self, status=None, assignee=None, operator=None, handler=None,
                     start=None, end=None, overdue=None, keyword=None,
                     page=1, page_size=20):
        """多条件筛选 + 分页，返回 (total, items)。

        - status：精确状态；
        - assignee/operator：分别按受理人 / 最终处理人精确筛选；
        - handler：处理人（受理人或最终处理人任一命中）；
        - start/end：按事件发生时间（event_time）范围；
        - overdue：True 只看超时未处理；
        - keyword：在工单元数据（标题/主体/规则/事件号）中模糊匹配。
        """
        now = time.time()
        with self._lock:
            snapshot = list(self._tickets.values())

        items = snapshot
        if status:
            items = [t for t in items if t.get("status") == status]
        if assignee:
            items = [t for t in items if t.get("assignee") == assignee]
        if operator:
            items = [t for t in items if t.get("operator") == operator]
        if handler:
            items = [t for t in items
                     if t.get("assignee") == handler or t.get("operator") == handler]
        if start is not None:
            items = [t for t in items if t.get("event_time", 0) >= start]
        if end is not None:
            items = [t for t in items if t.get("event_time", 0) <= end]
        if overdue is True:
            items = [t for t in items if self._is_overdue(t, now)]
        if keyword:
            kw = keyword.lower()
            def _hit(t):
                hay = " ".join([
                    str(t.get("id", "")), str(t.get("title", "")),
                    str(t.get("event_id", "")), str(t.get("event_type", "")),
                    str(t.get("event_summary", "")),
                    json_dumps(t.get("subject")),
                    json_dumps(t.get("hit_rules")),
                ])
                return kw in hay.lower()
            items = [t for t in items if _hit(t)]

        items.sort(key=lambda t: -t.get("created_at", 0))
        total = len(items)
        page = max(1, page)
        page_size = max(1, page_size)
        start_i = (page - 1) * page_size
        page_items = [self._decorate(dict(t), now) for t in items[start_i:start_i + page_size]]
        return total, page_items

    def _decorate(self, ticket, now=None):
        """附加运行期字段：overdue / 剩余时间 / 状态中文。"""
        now = now if now is not None else time.time()
        overdue = self._is_overdue(ticket, now)
        ticket["overdue"] = overdue
        ticket["status_label"] = STATUS_LABELS.get(ticket.get("status"),
                                                   ticket.get("status"))
        if overdue:
            ticket["overdue_sec"] = int(now - ticket.get("deadline", now))
        else:
            ticket["overdue_sec"] = 0
        return ticket

    def stats(self):
        """工单看板统计：按状态计数 + 超时数。"""
        now = time.time()
        by_status = {s: 0 for s in ALL_STATUSES}
        overdue = 0
        open_n = 0
        with self._lock:
            snapshot = list(self._tickets.values())
        for t in snapshot:
            s = t.get("status", PENDING)
            by_status[s] = by_status.get(s, 0) + 1
            if s in OPEN_STATUSES:
                open_n += 1
            if self._is_overdue(t, now):
                overdue += 1
        return {
            "total": len(snapshot),
            "by_status": by_status,
            "open": open_n,
            "overdue": overdue,
            "pending": by_status.get(PENDING, 0),
            "processing": by_status.get(PROCESSING, 0),
            "approved": by_status.get(APPROVED, 0),
            "rejected": by_status.get(REJECTED, 0),
            "closed": by_status.get(CLOSED, 0),
        }


def json_dumps(obj):
    import json
    try:
        return json.dumps(obj, ensure_ascii=False)
    except (TypeError, ValueError):
        return ""
