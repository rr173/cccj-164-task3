"""配额租约服务 —— 应用服务层。

职责：
  * 启动时从事件账本重放重建投影（账本是唯一事实来源）；
  * 串行执行命令：先到期扫描，再幂等去重，最后校验并原子提交；
  * 后台定时到期扫描；
  * 一致性审计：哈希链、非负余额、全局守恒、树/边完整性、
    重放重建与线上投影比对。
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any

from .domain import (
    HANDLERS,
    PLATFORM_ID,
    RESULT_EXTRACTORS,
    ConflictError,
    NotFoundError,
    Projection,
    ValidationError,
    apply_event,
    expired_lease_events,
)
from .drafts import (
    assert_editable,
    assert_fresh,
    build_publish_events,
    normalize_items,
    simulate,
)
from .ledger import GENESIS_HASH, EventStore, canonical


class QuotaService:
    def __init__(self, db_path: str, sweep_interval: float = 1.0):
        self.store = EventStore(db_path)
        self.sweep_interval = sweep_interval
        self.projection = self._rebuild_from_log()
        self._stop_sweeper = threading.Event()
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name="lease-sweeper", daemon=True
        )
        self._sweeper.start()

    # ------------------------------------------------------------------
    # 重建
    # ------------------------------------------------------------------

    def _rebuild_from_log(self) -> None:
        """清空内存状态，完整重放事件（启动与审计共用）。"""
        p = Projection()
        events = self.store.load_events()
        for evt in events:
            apply_event(p, evt)
        return p

    def _apply_new(self, events: list[dict[str, Any]]) -> None:
        """把刚提交的事件（领域格式 {type,payload}）折叠进线上投影。"""
        base_seq = self.projection.seq
        for i, ev in enumerate(events, start=1):
            apply_event(
                self.projection,
                {"seq": base_seq + i, "type": ev["type"], **ev["payload"]},
            )

    def rebuild(self) -> Projection:
        with self.store.lock:
            return self._rebuild_from_log()
    # ------------------------------------------------------------------
    # 到期扫描
    # ------------------------------------------------------------------

    def _sweep_loop(self) -> None:
        while not self._stop_sweeper.wait(self.sweep_interval):
            try:
                self.run_expirations(time.time())
            except Exception:  # 扫描线程绝不能死
                pass

    def run_expirations(self, now: float | None = None) -> int:
        """关闭所有已到期租约（含级联）。返回关闭数量。"""
        now = time.time() if now is None else now
        with self.store.lock:
            events = expired_lease_events(self.projection, now)
            if not events:
                return 0
            # 幂等 command_id：同一秒内重复扫描不会产生重复登记
            lease_ids = sorted(ev["payload"]["lease_id"] for ev in events)
            command_id = "sys:expire:" + ",".join(
                lid[-8:] for lid in lease_ids
            )
            self.store.append_batch(
                events,
                command_id=command_id,
                fingerprint="system",
                result={"closed": len(events), "lease_ids": lease_ids},
                system=True,
            )
            self._apply_new(events)
            return len(events)

    # ------------------------------------------------------------------
    # 命令执行
    # ------------------------------------------------------------------

    def execute(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        if kind not in HANDLERS:
            raise ValidationError(f"未知命令: {kind}")
        command_id = body.get("command_id") or f"{kind}:auto:{canonical(body)}"
        fingerprint = canonical({"kind": kind, "body": body})

        with self.store.lock:
            # 任何命令前先确定性地结算到期租约
            self._apply_sweep_locked(time.time())

            existing = self.store.get_command(command_id)
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise ConflictError(
                        f"command_id {command_id} 已用于不同的请求体"
                    )
                return {**existing["result"], "replayed": True}

            events = HANDLERS[kind](self.projection, body)
            result = self._build_result(kind, events)
            self.store.append_batch(
                events, command_id=command_id, fingerprint=fingerprint, result=result
            )
            self._apply_new(events)
            return result

    def _apply_sweep_locked(self, now: float) -> None:
        events = expired_lease_events(self.projection, now)
        if not events:
            return
        lease_ids = sorted(ev["payload"]["lease_id"] for ev in events)
        self.store.append_batch(
            events,
            command_id="sys:expire:" + ",".join(lid[-8:] for lid in lease_ids),
            fingerprint="system",
            result={"closed": len(events), "lease_ids": lease_ids},
            system=True,
        )
        self._apply_new(events)

    @staticmethod
    def _build_result(kind: str, events: list[dict[str, Any]]) -> dict[str, Any]:
        items = []
        for ev in events:
            extractor = RESULT_EXTRACTORS.get(ev["type"])
            items.append(
                {"event_type": ev["type"], **(extractor(ev["payload"]) if extractor else {})}
            )
        result = {"command": kind, "event_count": len(events), "events": items}
        if len(items) == 1:
            result.update(items[0])
        return result

    # ------------------------------------------------------------------
    # 批量容量调整草案
    # ------------------------------------------------------------------

    def _get_draft(self, draft_id: Any) -> dict[str, Any]:
        if not draft_id or not isinstance(draft_id, str):
            raise ValidationError("draft_id 必填")
        draft = self.projection.drafts.get(draft_id)
        if draft is None:
            raise NotFoundError(f"草案不存在: {draft_id}")
        return draft

    def create_draft(self, body: dict[str, Any]) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            if not self.projection.bootstrapped:
                raise ConflictError("平台尚未初始化，请先设置全局容量 (bootstrap)")

            items = normalize_items(self.projection, body.get("items"))
            draft_id = body.get("draft_id") or f"D-{canonical(items)[:16]}"
            if not isinstance(draft_id, str) or not draft_id:
                raise ValidationError("draft_id 非法")
            if draft_id in self.projection.drafts:
                raise ConflictError(f"草案 ID 已存在: {draft_id}")

            command_id = (
                body.get("command_id")
                or f"draft-create:{draft_id}:{canonical(items)}"
            )
            fingerprint = canonical({"items": items, "draft_id": draft_id})
            existing = self.store.get_command(command_id)
            if existing is not None:
                return {**existing["result"], "replayed": True}

            event = {
                "type": "DraftCreated",
                "payload": {
                    "draft_id": draft_id,
                    "items": items,
                    "base_seq": self.projection.seq,
                    "created_at": time.time(),
                },
            }
            result = {
                "command": "draft_create",
                "draft_id": draft_id,
                "status": "draft",
                "items": items,
                "base_seq": self.projection.seq,
                "event_count": 1,
                "events": [{"event_type": "DraftCreated", "draft_id": draft_id}],
            }
            self.store.append_batch(
                [event], command_id=command_id,
                fingerprint=fingerprint, result=result,
            )
            self._apply_new([event])
            return result

    def preview_draft(self, body: dict[str, Any]) -> dict[str, Any]:
        """重新计算整批执行后的逐层余额/受影响租约/拒绝原因，并推进陈旧锚点。

        预览本身以系统事件（``sys:`` 前缀，不进命令回执表）落一笔
        ``DraftPreviewed``，使锚点 == 预览事件的账本位置；同时让预览
        记录可在重启后从账本恢复、可审计。
        """
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            draft = self._get_draft(body.get("draft_id"))
            if draft["status"] == "published":
                raise ConflictError(
                    f"草案 {draft['id']} 已发布，不能再次预览",
                    details={"draft_id": draft["id"], "status": "published"},
                )
            assert_editable(draft, "预览")

            was_stale = draft["base_seq"] != self.projection.seq
            preview = simulate(self.projection, draft["items"], draft["id"])
            summary = {
                "valid": preview["valid"],
                "event_count": preview["event_count"],
                "rejected_count": len(preview["rejections"]),
                "global_capacity_after": preview["global_capacity_after"],
                "affected_lease_count": len(preview["affected_leases"]),
            }
            event = {
                "type": "DraftPreviewed",
                "payload": {
                    "draft_id": draft["id"],
                    "valid": preview["valid"],
                    "previewed_at": time.time(),
                    "summary": summary,
                },
            }
            self.store.append_batch(
                [event],
                command_id=f"sys:draft:preview:{draft['id']}:{self.projection.seq}",
                fingerprint="system",
                result={},
                system=True,
            )
            self._apply_new([event])
            return {
                "draft_id": draft["id"],
                "status": draft["status"],
                "base_seq": self.projection.seq,
                "was_stale": was_stale,
                **preview,
            }

    def publish_draft(self, body: dict[str, Any]) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            draft = self._get_draft(body.get("draft_id"))

            # 已发布：重复发布只能得到首次发布结果（幂等重放，不再落任何事件）
            if draft["status"] == "published":
                first = draft.get("published_result") or {}
                return {**first, "replayed": True}
            assert_editable(draft, "发布")

            command_id = (
                body.get("command_id")
                or f"draft-publish:{draft['id']}:{draft['base_seq']}"
            )
            existing = self.store.get_command(command_id)
            if existing is not None:
                return {**existing["result"], "replayed": True}

            # 乐观前提：草案锚点之后账本不得前进过（任何业务/系统事件）
            assert_fresh(draft, self.projection.seq)

            # 整批在深拷贝上重新规划：任一项不合法即抛错，尚未写入任何事件
            business_events = build_publish_events(
                self.projection, draft["items"], draft["id"]
            )
            preview = simulate(self.projection, draft["items"], draft["id"])
            if not preview["valid"]:
                # build_publish_events 理应先抛；兜底保证绝不落账
                raise ConflictError("批量发布被拒绝：存在不合法条目（整批未生效）")

            published_at = time.time()
            all_events = business_events  # 占位，下方补上 DraftPublished 后重算长度
            stored_result = {
                "command": "draft_publish",
                "draft_id": draft["id"],
                "status": "published",
                "published_at": published_at,
                "applied_event_count": len(business_events),
                "affected_leases": preview["affected_leases"],
                "layers": preview["layers"],
                "items": preview["items"],
            }
            publish_event = {
                "type": "DraftPublished",
                "payload": {
                    "draft_id": draft["id"],
                    "published_at": published_at,
                    # 首次发布结果完整存于事件载荷：重启后重复发布仍返回它
                    "result": stored_result,
                },
            }
            all_events = business_events + [publish_event]
            stored_result["event_count"] = len(all_events)
            result = dict(stored_result)
            self.store.append_batch(
                all_events,
                command_id=command_id,
                fingerprint=canonical({"draft_id": draft["id"], "op": "publish"}),
                result=result,
            )
            self._apply_new(all_events)
            return result

    def discard_draft(self, body: dict[str, Any]) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            draft = self._get_draft(body.get("draft_id"))
            if draft["status"] == "published":
                raise ConflictError(
                    f"草案 {draft['id']} 已发布，不能废弃",
                    details={"draft_id": draft["id"], "status": "published"},
                )
            assert_editable(draft, "废弃")

            command_id = (
                body.get("command_id")
                or f"draft-discard:{draft['id']}:{draft['base_seq']}"
            )
            existing = self.store.get_command(command_id)
            if existing is not None:
                return {**existing["result"], "replayed": True}

            event = {
                "type": "DraftDiscarded",
                "payload": {"draft_id": draft["id"], "discarded_at": time.time()},
            }
            result = {
                "command": "draft_discard",
                "draft_id": draft["id"],
                "status": "discarded",
                "discarded_at": event["payload"]["discarded_at"],
                "event_count": 1,
            }
            self.store.append_batch(
                [event],
                command_id=command_id,
                fingerprint=canonical({"draft_id": draft["id"], "op": "discard"}),
                result=result,
            )
            self._apply_new([event])
            return result

    def draft(self, draft_id: str) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            draft = self._get_draft(draft_id)
            return self._draft_view(draft)

    def list_drafts(self) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            return {
                "current_seq": self.projection.seq,
                "drafts": [
                    self._draft_view(d)
                    for d in sorted(
                        self.projection.drafts.values(),
                        key=lambda x: x["created_at"],
                    )
                ],
            }

    def _draft_view(self, draft: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": draft["id"],
            "status": draft["status"],
            "items": draft["items"],
            "base_seq": draft["base_seq"],
            "current_seq": self.projection.seq,
            "stale": draft["status"] == "draft"
            and draft["base_seq"] != self.projection.seq,
            "created_at": draft["created_at"],
            "last_preview": draft["last_preview"],
            "published_at": draft.get("published_at"),
            "published_result": draft.get("published_result"),
            "discarded_at": draft.get("discarded_at"),
        }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def state(self) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            return self.projection.to_state()

    def events(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.store.lock:
            rows = self.store.load_hash_chain()
        out = []
        for r in rows[-limit:]:
            out.append(
                {
                    "seq": r["seq"],
                    "event_type": r["event_type"],
                    "command_id": r["command_id"],
                    "payload": json.loads(r["payload"]),
                }
            )
        return out

    # ------------------------------------------------------------------
    # 一致性审计
    # ------------------------------------------------------------------

    def audit(self) -> dict[str, Any]:
        with self.store.lock:
            self._apply_sweep_locked(time.time())
            checks: list[dict[str, Any]] = []

            # 1) 哈希链完整性 / 防篡改
            chain = self.store.load_hash_chain()
            prev_hash = GENESIS_HASH
            chain_ok = True
            broken_seq = None
            for r in chain:
                expect = EventStore._hash_event(
                    prev_hash,
                    r["event_type"],
                    __import__("json").loads(r["payload"]),
                    r["command_id"],
                    r["fingerprint"],
                )
                if expect != r["event_hash"] or r["prev_hash"] != prev_hash:
                    chain_ok = False
                    broken_seq = r["seq"]
                    break
                prev_hash = r["event_hash"]
            checks.append(
                {
                    "name": "event_hash_chain",
                    "ok": chain_ok,
                    "detail": "哈希链连续，事件载荷未被篡改"
                    if chain_ok
                    else f"链在 seq={broken_seq} 处断裂（疑似账本被修改）",
                }
            )

            # 2) 幂等命令表：每个非系统 command_id 只登记一次
            commands = self.store.load_commands()
            cmd_counts: dict[str, int] = {}
            for r in chain:
                if not r["command_id"].startswith("sys:"):
                    cmd_counts[r["command_id"]] = (
                        cmd_counts.get(r["command_id"], 0) + 1
                    )
            missing = [cid for cid in commands if cid not in cmd_counts]
            orphan = [cid for cid in cmd_counts if cid not in commands]
            idem_ok = not missing and not orphan
            checks.append(
                {
                    "name": "idempotent_command_registry",
                    "ok": idem_ok,
                    "detail": f"{len(commands)} 条命令均有唯一回执"
                    if idem_ok
                    else f"命令回执不一致 missing={missing} orphan={orphan}",
                }
            )

            # 3) 非负余额
            negatives = [
                (nid, bal)
                for nid, bal in self.projection.balance.items()
                if bal < 0
            ]
            checks.append(
                {
                    "name": "non_negative_balances",
                    "ok": not negatives,
                    "detail": "所有节点可用额度 >= 0（无超卖）"
                    if not negatives
                    else f"出现负余额: {negatives}",
                }
            )

            # 4) 全局守恒：Σavail + 总消费 = 全局容量
            total_avail = sum(self.projection.balance.values())
            conserved = total_avail + self.projection.total_consumed
            cap = self.projection.global_capacity
            cons_ok = conserved == cap
            checks.append(
                {
                    "name": "global_conservation",
                    "ok": cons_ok,
                    "detail": (
                        f"Σ可用({total_avail}) + 总消费({self.projection.total_consumed})"
                        f" = 全局容量({cap})"
                    ),
                }
            )

            # 5) 树与边的层级完整性
            structure_problems = []
            for nid, node in self.projection.nodes.items():
                if nid == PLATFORM_ID:
                    continue
                pid = node["parent_id"]
                if pid not in self.projection.nodes:
                    structure_problems.append(f"{nid} 的父节点 {pid} 缺失")
            for lid, lease in self.projection.leases.items():
                if (
                    lease["lessor_id"] not in self.projection.nodes
                    or lease["holder_id"] not in self.projection.nodes
                ):
                    structure_problems.append(f"租约 {lid} 引用了不存在的节点")
                elif (
                    self.projection.nodes[lease["holder_id"]]["parent_id"]
                    != lease["lessor_id"]
                ):
                    structure_problems.append(f"租约 {lid} 不是直接父子边")
            checks.append(
                {
                    "name": "hierarchy_integrity",
                    "ok": not structure_problems,
                    "detail": "所有租约均为合法父子边，节点树完整"
                    if not structure_problems
                    else "; ".join(structure_problems),
                }
            )

            # 6) 租约余量合法 + 已消费永不消失 + 状态机合法
            lease_problems = []
            for lid, lease in self.projection.leases.items():
                # 直接消费不能超过授予量；向上吸收分摊后每条边同样不越界
                if not (0 <= lease["own_consumed"] <= lease["amount"]):
                    lease_problems.append(f"{lid} 直接消费量越界")
                if not (0 <= lease["consumed"] <= lease["amount"]):
                    lease_problems.append(
                        f"{lid} 消费分摊越界 ({lease['consumed']}/{lease['amount']})"
                    )
                status = lease["status"]
                if status == "closed":
                    if lease["reason"] not in (
                        "released",
                        "revoked",
                        "expired",
                        "offline",
                    ):
                        lease_problems.append(f"{lid} 关闭原因非法")
                elif status not in ("active", "pending_expired"):
                    lease_problems.append(f"{lid} 状态非法: {status}")
                elif status == "active":
                    if lease["reason"] is not None:
                        lease_problems.append(f"{lid} 活动租约不应有关闭原因")
            checks.append(
                {
                    "name": "lease_bookkeeping",
                    "ok": not lease_problems,
                    "detail": "租约余量/消费/关闭状态均合法"
                    if not lease_problems
                    else "; ".join(lease_problems),
                }
            )

            # 7) 重放重建结果必须与线上投影逐字段一致
            rebuilt = self._rebuild_from_log()
            replay_ok = rebuilt.digest() == self.projection.digest()
            checks.append(
                {
                    "name": "replay_equivalence",
                    "ok": replay_ok,
                    "detail": f"从 {len(chain)} 条事件重放的状态与线上投影一致"
                    if replay_ok
                    else "重放状态与线上投影不一致（投影可能被外部破坏）",
                }
            )

            # 8) 批量容量调整草案生命周期一致：
            #    已发布草案必须有对应 DraftPublished 事件与原子批次，
            #    已废弃草案不得又被发布；每条发布的业务事件数量与回执一致。
            draft_problems = []
            published_ids = set()
            for r in chain:
                if r["event_type"] == "DraftPublished":
                    published_ids.add(json.loads(r["payload"]).get("draft_id"))
            batch_sizes: dict[str, int] = {}
            for r in chain:
                if r["event_type"] in (
                    "CapacityAdjusted",
                    "LeaseGranted",
                    "LeaseAdjusted",
                ):
                    pl = __import__("json").loads(r["payload"])
                    did = pl.get("draft_id")
                    if did:
                        batch_sizes[did] = batch_sizes.get(did, 0) + 1
            for did, d in self.projection.drafts.items():
                if d["status"] == "published":
                    if did not in published_ids:
                        draft_problems.append(f"草案 {did} 状态已发布但账本无 DraftPublished 事件")
                    stored = (d.get("published_result") or {}).get("applied_event_count")
                    if stored is not None and stored != batch_sizes.get(did, 0):
                        draft_problems.append(
                            f"草案 {did} 回执记录 {stored} 条业务事件，账本实际 "
                            f"{batch_sizes.get(did, 0)} 条"
                        )
                elif d["status"] == "discarded":
                    if did in published_ids:
                        draft_problems.append(f"草案 {did} 既废弃又发布，状态非法")
                elif d["status"] != "draft":
                    draft_problems.append(f"草案 {did} 状态非法: {d['status']}")
            checks.append(
                {
                    "name": "draft_lifecycle",
                    "ok": not draft_problems,
                    "detail": (
                        f"{len(self.projection.drafts)} 个草案生命周期与账本一致"
                        if not draft_problems
                        else "; ".join(draft_problems)
                    ),
                }
            )

            ok = all(c["ok"] for c in checks)
            return {
                "ok": ok,
                "checked_at": time.time(),
                "event_count": len(chain),
                "command_count": len(commands),
                "global_capacity": cap,
                "total_available": total_avail,
                "total_consumed": self.projection.total_consumed,
                "checks": checks,
            }

    def shutdown(self) -> None:
        self._stop_sweeper.set()
        self.store.close()
