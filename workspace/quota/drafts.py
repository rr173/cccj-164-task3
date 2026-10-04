"""批量容量调整 —— 草案规划、逐层模拟与原子发布。

管理员把多条*节点容量变更*编辑为一个草案（draft）。节点容量指该节点
通过入边租约（非平台节点）或全局容量（平台节点）持有的总额度：

  capacity(platform) = global_capacity
  capacity(N≠platform) = Σ amount(holder=N 的 live 入边)   # active + pending_expired

草案条目支持两种写法（二选一）：
  {"node_id": ..., "capacity": 新容量}      # 绝对目标容量
  {"node_id": ..., "delta": 增量}           # 相对当前容量增减（可为负）

发布时把全部条目作为*一个原子批次*落账：任一条目不合法，整批不生效。

缩容如何落在租约边上
--------------------
非平台节点的容量由其多条入边持有。缩容时自底向上从这些边中收回：
按授予时间*从新到旧*贪心收缩（最新得到的额度最先让出），一条边最多
收缩到其已消费量（已消费永不消失）。被缩的边产生 ``LeaseAdjusted``
事件；``active`` 与 ``pending_expired`` 边都可缩（pending 边收缩后
到期结算自然少退）。扩容则向父级申请一条新租约（父级需有足够可用）。

平台节点直接复用 ``CapacityAdjusted`` 语义（低于已承诺/消费下限拒绝）。

执行顺序
--------
一批里同时有扩容与缩容：先*自顶向下*做全部扩容（父级先拿到额度才能
向下放），再*自底向上*做全部缩容（子级先让出，父级才可能进一步缩）。
这样能最大化整批可行性；仍不合法的条目各自给出拒绝原因。

陈旧检测
--------
草案锚定创建/最近一次预览时的账本 ``seq``。发布以该位置为前提：
若锚点之后又追加过任何事件（授约/消费/续约/结算/容量调整…），拒绝
陈旧版本并提示需要重新预览。预览会把锚点推进到预览事件自身的位置。
"""

from __future__ import annotations

import copy
import time
import uuid
from typing import Any

from .domain import (
    PLATFORM_ID,
    ConflictError,
    NotFoundError,
    ValidationError,
    _require_int,
    apply_event,
)

DRAFT_TERMINAL_STATUSES = ("published", "discarded")


# ----------------------------------------------------------------------
# 草案条目规范化 / 结构校验（创建时执行：纯参数级，不看余额）
# ----------------------------------------------------------------------


def normalize_items(p: Any, raw_items: Any) -> list[dict[str, int | str]]:
    if not isinstance(raw_items, list) or not raw_items:
        raise ValidationError("items 必须是非空数组，每项为一条节点容量变更")
    items: list[dict[str, int | str]] = []
    seen: set[str] = set()
    for idx, raw in enumerate(raw_items):
        if not isinstance(raw, dict):
            raise ValidationError(f"第 {idx + 1} 项必须是对象")
        node_id = raw.get("node_id")
        if not node_id or not isinstance(node_id, str):
            raise ValidationError(f"第 {idx + 1} 项缺少 node_id")
        if node_id not in p.nodes:
            raise NotFoundError(f"第 {idx + 1} 项节点不存在: {node_id}")
        if node_id in seen:
            raise ValidationError(f"草案中节点 {node_id} 出现多次（每个节点一批只能一条）")
        seen.add(node_id)

        has_cap = raw.get("capacity") is not None
        has_delta = raw.get("delta") is not None
        if has_cap == has_delta:
            raise ValidationError(
                f"第 {idx + 1} 项必须且只能提供 capacity（目标容量）或 delta（增量）之一"
            )
        if has_cap:
            target = _require_int(raw.get("capacity"), f"第 {idx + 1} 项 capacity", 0)
        else:
            delta = _require_int(raw.get("delta"), f"第 {idx + 1} 项 delta")
            target = _node_capacity(p, node_id) + delta
            if target < 0:
                raise ValidationError(
                    f"第 {idx + 1} 项容量不能为负：当前 {_node_capacity(p, node_id)}"
                    f"，增量 {delta}"
                )
        items.append({"node_id": node_id, "target_capacity": target})
    return items


def _node_capacity(p: Any, node_id: str) -> int:
    if node_id == PLATFORM_ID:
        return p.global_capacity
    # live 入边（active + pending_expired）的授予量之和即该节点持有容量
    return sum(
        l["amount"] for l in p.live_leases() if l["holder_id"] == node_id
    )


# ----------------------------------------------------------------------
# 单条变更规划：在给定投影上产出事件（不应用），或给出拒绝原因
# ----------------------------------------------------------------------


def _depth(p: Any, node_id: str) -> int:
    d = 0
    cur = p.nodes[node_id]["parent_id"]
    while cur is not None:
        d += 1
        cur = p.nodes[cur]["parent_id"]
    return d


def _new_lease_id(p: Any) -> str:
    while True:
        lid = f"L-{uuid.uuid4().hex[:12]}"
        if lid not in p.leases:
            return lid


def _plan_item(
    p: Any,
    item: dict[str, Any],
    draft_id: str,
    now: float,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """规划单条节点容量变更。

    返回 (events, rejection)。rejection 非空表示该条目不合法
    （调用方跳过这些事件，继续规划其余条目以收集全部拒绝原因）。
    """
    node_id = item["node_id"]
    target = item["target_capacity"]
    current = _node_capacity(p, node_id)
    base = {
        "node_id": node_id,
        "node_type": p.nodes[node_id]["type"],
        "current_capacity": current,
        "target_capacity": target,
        "delta": target - current,
    }
    if target == current:
        return [], {**base, "code": "unchanged", "reason": "容量未变化"}

    if node_id == PLATFORM_ID:
        if target < current:
            floor = current - p.avail(PLATFORM_ID)
            if target < floor:
                return [], {
                    **base,
                    "code": "oversold",
                    "reason": f"缩容被拒绝：已承诺/消费下限为 {floor}，目标 {target} 会导致平台超卖",
                    "floor": floor,
                }
        return [
            {
                "type": "CapacityAdjusted",
                "payload": {
                    "old_capacity": current,
                    "new_capacity": target,
                    "draft_id": draft_id,
                },
            }
        ], None

    # ---- 非平台节点：容量 = live 入边授予量之和 ----
    incoming = sorted(
        (l for l in p.live_leases() if l["holder_id"] == node_id),
        key=lambda l: l["granted_at"],
    )
    delta = target - current

    if delta > 0:
        # 扩容：向父级申请一条新租约；父级当前可用必须覆盖
        parent_id = p.nodes[node_id]["parent_id"]
        parent_available = p.avail(parent_id)
        if delta > parent_available:
            return [], {
                **base,
                "code": "oversold",
                "reason": (
                    f"扩容被拒绝：{node_id} 需从父级 {parent_id} 新增 {delta}，"
                    f"父级可用仅 {parent_available}"
                ),
                "parent_id": parent_id,
                "parent_available": parent_available,
            }
        # 离线客户端不能再获得新租约
        node = p.nodes[node_id]
        if node["type"] == "client" and not node["online"]:
            return [], {
                **base,
                "code": "client_offline",
                "reason": f"扩容被拒绝：客户端 {node_id} 已离线，不能获得新租约",
            }
        return [
            {
                "type": "LeaseGranted",
                "payload": {
                    "lease_id": _new_lease_id(p),
                    "lessor_id": parent_id,
                    "holder_id": node_id,
                    "amount": delta,
                    "granted_at": now,
                    "expires_at": None,
                    "draft_id": draft_id,
                },
            }
        ], None

    # 缩容：shrink = −delta，自最新授予的边起贪心收回；
    # 每条边最多缩到 consumed（已消费永不消失）。
    shrink = -delta
    events: list[dict[str, Any]] = []
    affected: list[dict[str, Any]] = []
    remaining_need = shrink
    for lease in reversed(incoming):  # 从新到旧
        if remaining_need <= 0:
            break
        # 可缩量 = 当前授予量 − 已消费（直接+向上吸收后的总消费）
        reducible = lease["amount"] - lease["consumed"]
        if reducible <= 0:
            continue
        cut = min(remaining_need, reducible)
        new_amount = lease["amount"] - cut
        events.append(
            {
                "type": "LeaseAdjusted",
                "payload": {
                    "lease_id": lease["id"],
                    "draft_id": draft_id,
                    "old_amount": lease["amount"],
                    "new_amount": new_amount,
                    "lessor_id": lease["lessor_id"],
                    "holder_id": lease["holder_id"],
                    "adjusted_at": now,
                },
            }
        )
        affected.append(
            {
                "lease_id": lease["id"],
                "lessor_id": lease["lessor_id"],
                "old_amount": lease["amount"],
                "new_amount": new_amount,
                "reduced": cut,
                "lease_status": lease["status"],
            }
        )
        remaining_need -= cut

    if remaining_need > 0:
        # 可收缩总量 = Σ(amount − consumed)；消费量以下不可抽
        floor = sum(l["consumed"] for l in incoming)
        return [], {
            **base,
            "code": "oversold",
            "reason": (
                f"缩容被拒绝：{node_id} 入边已消费/不可抽让的额度要求容量 >= {floor}，"
                f"目标 {target} 会令余额为负（还差 {remaining_need} 无可收缩余量）"
            ),
            "floor": floor,
            "affected_preview": affected,
        }
    # 在规划事件对象上暂存受影响租约明细，供预览汇总（apply_event 会忽略）
    for ev, info in zip(events, affected):
        ev["_preview_affected"] = info
    return events, None


# ----------------------------------------------------------------------
# 整批模拟：深拷贝投影，按“先扩容后缩容”顺序规划并应用
# ----------------------------------------------------------------------


def simulate(p: Any, items: list[dict[str, Any]], draft_id: str) -> dict[str, Any]:
    """在投影的*深拷贝*上模拟整批发布，返回预览结果（不触碰线上投影）。

    即使部分条目被拒也继续模拟其余条目，以一次性给出所有拒绝原因；
    但 ``valid=False`` 时发布必然整批不生效。
    """
    now = time.time()
    sim = copy.deepcopy(p)

    rejections: list[dict[str, Any]] = []
    item_results: list[dict[str, Any]] = []
    affected_by_node: dict[str, list[dict[str, Any]]] = {}
    before_bal = {nid: p.avail(nid) for nid in p.nodes}
    before_cap = {nid: _node_capacity(p, nid) for nid in p.nodes}

    # 分组：扩容（含平台）自顶向下，缩容自底向上
    indexed = list(enumerate(items))

    def kind_rank(it: dict[str, Any]) -> int:
        return 0 if it["target_capacity"] >= _node_capacity(p, it["node_id"]) else 1

    ordered = sorted(
        indexed,
        key=lambda pair: (
            kind_rank(pair[1]),
            (1 if kind_rank(pair[1]) == 0 else -1) * _depth(p, pair[1]["node_id"]),
        ),
    )

    applied_events: list[dict[str, Any]] = []
    for original_index, item in ordered:
        events, rejection = _plan_item(sim, item, draft_id, now)
        node_id = item["node_id"]
        if rejection is not None:
            rejections.append({"index": original_index, **rejection})
            item_results.append(
                {
                    "index": original_index,
                    "node_id": node_id,
                    "ok": False,
                    "current_capacity": rejection["current_capacity"],
                    "target_capacity": rejection["target_capacity"],
                    "delta": rejection["delta"],
                    "reason": rejection["reason"],
                    "code": rejection["code"],
                }
            )
            continue
        for ev in events:
            info = ev.pop("_preview_affected", None)
            if info is not None:
                affected_by_node.setdefault(node_id, []).append(info)
            seq = sim.seq + 1
            apply_event(sim, {"seq": seq, "type": ev["type"], **ev["payload"]})
            applied_events.append(ev)
        item_results.append(
            {
                "index": original_index,
                "node_id": node_id,
                "ok": True,
                "current_capacity": before_cap[node_id],
                "target_capacity": item["target_capacity"],
                "delta": item["target_capacity"] - before_cap[node_id],
                "affected_leases": affected_by_node.get(node_id, []),
                "new_leases": [
                    ev["payload"]["lease_id"]
                    for ev in events
                    if ev["type"] == "LeaseGranted"
                ],
            }
        )

    item_results.sort(key=lambda r: r["index"])
    rejections.sort(key=lambda r: r["index"])

    # 逐层余额（执行前 → 执行后）：只返回本批条目节点与余额/容量发生变化的节点
    item_nodes = {it["node_id"] for it in items}
    layers: list[dict[str, Any]] = []
    for nid, node in p.nodes.items():
        after = sim.balance.get(nid, 0)
        changed = (
            nid in item_nodes
            or after != before_bal[nid]
            or _node_capacity(sim, nid) != before_cap[nid]
        )
        if not changed:
            continue
        layers.append(
            {
                "node_id": nid,
                "node_type": node["type"],
                "capacity_before": before_cap[nid],
                "capacity_after": _node_capacity(sim, nid),
                "available_before": before_bal[nid],
                "available_after": after,
                "available_delta": after - before_bal[nid],
            }
        )
    type_order = {"platform": 0, "organization": 1, "project": 2, "client": 3}
    layers.sort(key=lambda x: (type_order.get(x["node_type"], 9), x["node_id"]))

    affected_leases: list[dict[str, Any]] = []
    for infos in affected_by_node.values():
        affected_leases.extend(infos)

    # 模拟后必须仍满足非负余额（防御性断言；规划逻辑应已保证）
    negatives = [
        (nid, bal) for nid, bal in sim.balance.items() if bal < 0
    ]

    return {
        "valid": not rejections and not negatives,
        "items": item_results,
        "rejections": rejections,
        "layers": layers,
        "affected_leases": affected_leases,
        "negatives": negatives,
        "event_count": len(applied_events),
        "global_capacity_before": p.global_capacity,
        "global_capacity_after": sim.global_capacity,
        "total_consumed": sim.total_consumed,
    }


# ----------------------------------------------------------------------
# 发布事件构建（校验通过后，由服务层原子追加）
# ----------------------------------------------------------------------


def build_publish_events(
    p: Any, items: list[dict[str, Any]], draft_id: str
) -> list[dict[str, Any]]:
    """与 simulate 同序规划整批；任一条目不合法即抛错（整批不生效）。

    返回应追加的*业务事件*（CapacityAdjusted / LeaseGranted / LeaseAdjusted），
    不含 DraftPublished 包装事件（由服务层补）。
    """
    now = time.time()
    sim = copy.deepcopy(p)
    indexed = list(enumerate(items))

    def kind_rank(it: dict[str, Any]) -> int:
        return 0 if it["target_capacity"] >= _node_capacity(p, it["node_id"]) else 1

    ordered = sorted(
        indexed,
        key=lambda pair: (
            kind_rank(pair[1]),
            (1 if kind_rank(pair[1]) == 0 else -1) * _depth(p, pair[1]["node_id"]),
        ),
    )

    events: list[dict[str, Any]] = []
    for original_index, item in ordered:
        item_events, rejection = _plan_item(sim, item, draft_id, now)
        if rejection is not None:
            raise ConflictError(
                f"批量发布被拒绝：第 {original_index + 1} 项（{item['node_id']}）不合法 —— "
                f"{rejection['reason']}（原子批次：整批均未生效）",
                details={"index": original_index, **rejection},
            )
        for ev in item_events:
            ev.pop("_preview_affected", None)
            apply_event(sim, {"seq": sim.seq + 1, "type": ev["type"], **ev["payload"]})
            events.append(ev)
    negatives = [(nid, b) for nid, b in sim.balance.items() if b < 0]
    if negatives:
        raise ConflictError(f"批量发布会导致负余额: {negatives}")
    return events


def assert_editable(draft: dict[str, Any], action: str) -> None:
    if draft["status"] == "discarded":
        raise ConflictError(
            f"草案 {draft['id']} 已废弃，不能{action}",
            details={"draft_id": draft["id"], "status": "discarded"},
        )


def assert_fresh(draft: dict[str, Any], current_seq: int) -> None:
    """发布/预览的乐观前提：锚点之后账本不得有新事件。"""
    if draft["base_seq"] != current_seq:
        raise ConflictError(
            f"草案版本陈旧：创建/预览时账本位置为 #{draft['base_seq']}，"
            f"当前已到 #{current_seq}（期间发生了新的授约/消费/续约/结算/容量调整）。"
            "请重新预览后再发布。",
            details={
                "draft_id": draft["id"],
                "draft_base_seq": draft["base_seq"],
                "current_seq": current_seq,
                "stale": True,
            },
        )
