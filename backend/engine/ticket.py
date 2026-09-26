"""人工复核工单：事件被判为「人工复核」时自动建单，并在工单中心流转。

与告警聚合器一致，工单按天分片持久化为 JSON 文件（tickets/YYYYMMDD.json），
内存中保留全量工单（工单是低频流程数据，需支持按任意时间范围筛选），
每次创建 / 状态变更后整分片原子落盘，进程重启后从磁盘恢复。

状态机：
    pending（待受理） --accept--> processing（处理中）
    pending | processing --approve--> approved（已通过，终态）
    pending | processing --reject---> rejected（已驳回，终态，必须填处理意见）
    pending | processing --close----> closed（已关闭，终态）

工单记录处理人、每条处理意见以及各状态变更时间；超过 ``timeout_sec``
仍处于活动状态（pending/processing）的工单标记为超时（overdue）。
通过 / 驳回的结论作为事件处置结果（disposition）随工单持久沉淀。
"""
import json
import os
import threading
import time

from backend import config
from backend.storage import atomic_write_json, read_json

# 终态：已通过 / 已驳回 / 已关闭
FINAL_STATUSES = ("approved", "rejected", "closed")
# 允许的流转动作 → (目标状态, 目标状态时间字段)
TRANSITIONS = {
    "accept": "processing",
    "approve": "approved",
    "reject": "rejected",
    "close": "closed",
}
ACTION_LABELS = {
    "create": "系统建单",
    "accept": "受理",
    "approve": "通过",
    "reject": "驳回",
    "close": "关闭",
}

# 主体信息默认采集字段
_SUBJECT_FIELDS = ("user_id", "ip", "device_id", "channel", "country", "mobile", "email")


class TicketError(ValueError):
    pass


def _day_key(ts):
    ts = ts - 8 * 3600
    t = time.gmtime(ts)
    return f"{t.tm_year:04d}{t.tm_mon:02d}{t.tm_mday:02d}"


class TicketStore:
    """复核工单存储与状态流转。"""

    def __init__(self, timeout_sec=3600):
        self.timeout_sec = int(timeout_sec or 3600)
        self._tickets = {}        # id -> ticket dict（内存源）
        self._event_index = {}    # event_id -> ticket_id（保证单事件单工单）
        self._lock = threading.RLock()
        self._load_all()

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    def _shard_path(self, day):
        return os.path.join(config.TICKETS_DIR, f"{day}.json")

    def _load_all(self):
        """启动时加载全部分片，恢复工单与事件索引。"""
        if not os.path.isdir(config.TICKETS_DIR):
            return
        for fn in sorted(os.listdir(config.TICKETS_DIR)):
            if not fn.endswith(".json"):
                continue
            data = read_json(os.path.join(config.TICKETS_DIR, fn), {"tickets": []})
            for t in data.get("tickets", []):
                tid = t.get("id")
                if tid:
                    self._tickets[tid] = t
                    ev_id = t.get("event_id")
                    if ev_id:
                        self._event_index[ev_id] = tid

    def _persist_day(self, day):
        path = self._shard_path(day)
        tickets = [t for t in self._tickets.values()
                   if _day_key(t.get("created_at", 0)) == day]
        atomic_write_json(path, {"tickets": tickets})

    # ------------------------------------------------------------------
    # 自动建单
    # ------------------------------------------------------------------
    @staticmethod
    def _build_subject(event):
        subject = {}
        for f in _SUBJECT_FIELDS:
            val = event.get(f)
            if val is not None and val != "":
                subject[f] = val
        return subject

    @staticmethod
    def _build_summary(event, hit_rules, risk_score):
        """事件概要：类型 + 关键业务字段 + 首要命中原因。"""
        parts = [str(event.get("type") or "未知事件")]
        if event.get("amount") is not None:
            parts.append(f"金额 ¥{event.get('amount')}")
        if event.get("country"):
            parts.append(f"地区 {event.get('country')}")
        if event.get("channel"):
            parts.append(f"渠道 {event.get('channel')}")
        if hit_rules:
            parts.append(f"命中「{hit_rules[0].get('rule_name') or hit_rules[0].get('rule_id')}」")
        parts.append(f"风险分 {risk_score}")
        return "，".join(parts)

    def create_for_event(self, event, decision, source="rule_engine",
                         source_name=None, ts=None, creator="system"):
        """对一条判定为「人工复核」的事件创建工单。

        同一 event_id 只建一单（重复判定幂等返回已有工单）。
        返回 (ticket, created)。
        """
        if ts is None:
            ts = event.get("ts") or time.time()
        event_id = event.get("id")
        with self._lock:
            existing_id = self._event_index.get(event_id) if event_id else None
            if existing_id and existing_id in self._tickets:
                return self._tickets[existing_id], False

            hit_rules = decision.get("fired_rules", [])
            risk_score = int(decision.get("risk_score", 0))
            ticket = {
                "id": f"tk_{int(ts * 1000)}_{len(self._tickets) % 10000:04d}",
                "event_id": event_id,
                "source": source,
                "source_name": source_name,
                "summary": self._build_summary(event, hit_rules, risk_score),
                "event_type": event.get("type"),
                "event": dict(event),
                "subject": self._build_subject(event),
                "hit_rules": [dict(r) for r in hit_rules],
                "risk_score": risk_score,
                "status": "pending",
                "assignee": None,
                "creator": creator,
                "created_at": ts,
                "updated_at": ts,
                "accepted_at": None,
                "approved_at": None,
                "rejected_at": None,
                "closed_at": None,
                # 处置结论：approved / rejected / closed，与事件处置结果一致沉淀
                "disposition": None,
                "disposition_by": None,
                "disposition_ts": None,
                "timeout_sec": self.timeout_sec,
                "comments": [],          # [{action, action_label, by, ts, comment}]
            }
            ticket["comments"].append({
                "action": "create", "action_label": ACTION_LABELS["create"],
                "by": creator, "ts": ts, "comment": "风控判定为人工复核，系统自动建单",
            })
            self._tickets[ticket["id"]] = ticket
            if event_id:
                self._event_index[event_id] = ticket["id"]
            self._persist_day(_day_key(ts))
            return ticket, True

    # ------------------------------------------------------------------
    # 状态流转
    # ------------------------------------------------------------------
    def transition(self, ticket_id, action, handler, comment="", ts=None):
        """受理 / 通过 / 驳回 / 关闭。返回更新后的工单，失败抛 TicketError。"""
        if action not in TRANSITIONS:
            raise TicketError(f"未知操作: {action}")
        comment = (comment or "").strip()
        if action == "reject" and not comment:
            raise TicketError("驳回时必须填写处理意见")
        if ts is None:
            ts = time.time()
        target = TRANSITIONS[action]
        with self._lock:
            t = self._tickets.get(ticket_id)
            if t is None:
                raise TicketError("工单不存在")
            current = t.get("status")
            if current in FINAL_STATUSES:
                raise TicketError(f"工单已终态（{current}），不能继续操作")
            if action == "accept" and current != "pending":
                raise TicketError("仅待受理工单可以受理")
            if action in ("approve", "reject", "close") and \
                    current not in ("pending", "processing"):
                raise TicketError("当前状态不允许该操作")

            t["status"] = target
            t["updated_at"] = ts
            if action == "accept":
                t["assignee"] = handler
                t["accepted_at"] = ts
            elif action == "approve":
                t["assignee"] = t.get("assignee") or handler
                t["approved_at"] = ts
                t["disposition"] = "approved"
                t["disposition_by"] = handler
                t["disposition_ts"] = ts
            elif action == "reject":
                t["assignee"] = t.get("assignee") or handler
                t["rejected_at"] = ts
                t["disposition"] = "rejected"
                t["disposition_by"] = handler
                t["disposition_ts"] = ts
            elif action == "close":
                t["closed_at"] = ts
                t["disposition"] = "closed"
                t["disposition_by"] = handler
                t["disposition_ts"] = ts
            t["comments"].append({
                "action": action,
                "action_label": ACTION_LABELS[action],
                "by": handler,
                "ts": ts,
                "comment": comment or ACTION_LABELS[action],
            })
            self._persist_day(_day_key(t["created_at"]))
            return t

    def get(self, ticket_id):
        with self._lock:
            t = self._tickets.get(ticket_id)
        return self.decorate(t) if t else None

    def get_by_event(self, event_id):
        with self._lock:
            tid = self._event_index.get(event_id)
            t = self._tickets.get(tid) if tid else None
        return self.decorate(t) if t else None

    def decorate(self, t):
        """附加超时标记与处理截止时间（不落盘，实时计算）。"""
        return self._decorate(t, time.time())

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def _decorate(self, t, now):
        """附加超时标记与耗时（不落盘，实时计算）。"""
        out = dict(t)
        active = t.get("status") in ("pending", "processing")
        deadline = t.get("created_at", 0) + t.get("timeout_sec", self.timeout_sec)
        out["overdue"] = bool(active and now > deadline)
        out["deadline"] = deadline
        out["overdue_sec"] = int(now - deadline) if active and now > deadline else 0
        return out

    def list_tickets(self, status=None, assignee=None, source=None,
                     start=None, end=None, keyword=None, overdue=None,
                     page=1, page_size=20):
        """按状态 / 处理人 / 时间范围（事件发生时间）/ 关键词 / 超时筛选。"""
        with self._lock:
            snapshot = list(self._tickets.values())
        now = time.time()
        items = []
        for t in snapshot:
            if status and t.get("status") != status:
                continue
            if assignee and t.get("assignee") != assignee:
                continue
            if source and t.get("source") != source:
                continue
            event_ts = t.get("created_at", 0)
            # 时间范围以事件发生（建单）时间为准
            if start is not None and event_ts < start:
                continue
            if end is not None and event_ts > end:
                continue
            deco = self._decorate(t, now)
            if overdue and not deco["overdue"]:
                continue
            if keyword:
                kw = keyword.lower()
                if kw not in json.dumps(t, ensure_ascii=False).lower():
                    continue
            items.append(deco)
        # 活动且超时的工单优先，其次按建单时间倒序
        items.sort(key=lambda x: (not x["overdue"], -x.get("created_at", 0)))
        total = len(items)
        start_idx = (page - 1) * page_size
        return total, items[start_idx:start_idx + page_size]

    def list_assignees(self):
        """处理人候选：系统启用用户 + 工单中已出现过的处理人。"""
        usernames = set()
        try:
            from backend import auth
            for u in auth._load_users():
                if u.get("enabled", True) and u.get("username"):
                    usernames.add(u["username"])
        except Exception:
            pass
        with self._lock:
            for t in self._tickets.values():
                if t.get("assignee"):
                    usernames.add(t["assignee"])
        return sorted(usernames)

    def stats(self):
        """工单状态分布与超时统计（供总览 / 工单中心磁贴）。"""
        by_status = {s: 0 for s in config.TICKET_STATUSES}
        overdue = 0
        active = 0
        now = time.time()
        with self._lock:
            tickets = list(self._tickets.values())
        for t in tickets:
            by_status[t.get("status", "pending")] = \
                by_status.get(t.get("status", "pending"), 0) + 1
            deco = self._decorate(t, now)
            if t.get("status") in ("pending", "processing"):
                active += 1
                if deco["overdue"]:
                    overdue += 1
        return {
            "total": len(tickets),
            "active": active,
            "overdue": overdue,
            "by_status": by_status,
            "timeout_sec": self.timeout_sec,
        }
