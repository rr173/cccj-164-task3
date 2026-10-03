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
    Projection,
    ValidationError,
    apply_event,
    expired_lease_events,
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
