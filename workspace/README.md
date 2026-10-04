# 多租户 API 配额租约服务

平台把全局容量沿 **组织 → 项目 → 客户端** 三级树向下分配；子级只能在父级
*当前可用额度内*申请带有效期的租约。实际调用按**幂等用量回执**扣减，
归还 / 撤销 / 到期 / 离线只释放**尚未消费**的部分；已经确认的消费永不消失。
服务重启后从**只追加事件账本**重放重建当前余额。

零外部依赖：仅 Python 3.11 标准库（含 SQLite）。

---

## 快速开始

```bash
python3 -m quota.http_server --port 8765 --db ./quota.db
# 打开 http://localhost:8765/ 即为租约控制台
```

跑测试（功能 / 并发 / 到期 / 离线 / 续约 / 重启重建 / 账本篡改 / 批量容量草案，共 195 项）：

```bash
python3 test_service.py
```

---

## 架构

```
HTTP (http_server.py)
  └─ QuotaService (service.py)        串行命令、幂等去重、到期 sweep、审计
       ├─ Projection (domain.py)      内存余额/租约投影（事件折叠）
       └─ EventStore  (ledger.py)     SQLite 只追加事件 + 命令回执 + 哈希链
```

账本是唯一事实来源。进程启动时清空内存、从 `events` 表完整重放；
任何业务状态都不做外部写入。

### 事件（append-only）

| 事件 | 含义 |
|---|---|
| `Bootstrapped` | 设置全局容量（一次性） |
| `NodeRegistered` | 注册组织 / 项目 / 客户端 |
| `CapacityAdjusted` | 调整全局容量（缩容防超卖） |
| `LeaseGranted` | 父子节点间授予一条租约 |
| `UsageRecorded` | 幂等用量回执，按 `usage_event_id` 去重 |
| `LeaseExpired` | 租约到期，标记 `pending_expired`（不动余额） |
| `LeaseAmended` | 修订有效期：截止时间延后，租约修订号 +1（不动余额） |
| `LeaseClosed` | 结算租约，退回未消费余量（修订号同样 +1） |
| `ClientPresenceChanged` | 客户端上 / 下线 |
| `LeaseAdjusted` | 批量容量调整在租约边上的落点：收缩授予量（不动消费量） |
| `DraftCreated` / `DraftPreviewed` / `DraftPublished` / `DraftDiscarded` | 批量容量调整草案生命周期（含发布锚点与首次发布结果） |

每条事件带 `prev_hash/event_hash`（SHA-256 链）与所属 `command_id`，
篡改任意载荷都会让审计的哈希链检查断裂。

### 记账规则（关键不变量）

每个节点一个余额账户，租约是父子之间的一条边：

```
授予 A：   lessor 余额 −A，holder 余额 +A
消费 C：   holder 余额 −C；C 在每个祖先层的活动入边上贪心吸收
           （祖先边的可退余量同步收缩 C，中间节点余额不变）
关闭：     holder −(A−consumed)，lessor +(A−consumed)   # 只退未消费
```

由此恒成立：

- **非负余额**：任何节点 `avail ≥ 0`（无超卖）；
- **全局守恒**：`Σ 可用 + 总消费 = 全局容量`；
- **消费不复活**：关闭只退 `A−consumed`，已消费永久保留为消费记录。

### 为什么到期要“挂起 → 延迟结算”

中间层租约（如项目租约）到期时，它的余量可能已经被持有者合法地分配给
了更下层的活动租约。若到期就强抽，会把持有者余额压成负。因此：

1. 到期先置为 `pending_expired`：停止接受新消费 / 新授约，但**不动余额**；
2. 后台 sweep 在每条命令前自底向上做固定点结算：一旦下游释放使持有者
   余额足以让出该租约剩余，才真正 `LeaseClosed` 退款。

这样中间层到期永远不会引发跨层超卖；重启后待结算租约会在重放+sweep 时
继续走完结算。

### 有效期修订（续约）与乐观修订号

每条租约带一个单调递增的 **revision（修订号）**：授予即为 `0`，任何成功
的状态变更都让它 `+1`（`LeaseAmended` 与 `LeaseClosed` 都推进版本）。
持有人为**仍在生效**的租约提交它所看到的 `expected_revision` 与一个更晚
的截止时间，服务端在锁内校验：

- `expected_revision == 当前修订号`，且新截止时间**严格晚于**当前截止时间；
- 租约必须 `active`（已 `closed`、或到期 `pending_expired` 等待结算都拒绝）；
- 持有人是已下线客户端时拒绝；
- 永久租约（授予时无 TTL）不能修订。

满足则追加一条 `LeaseAmended`（只改时间，不动任何余额），修订号递增。
冲突返回 `409` 并在 `details` 中回传租约最新状态（`current_revision` /
`expires_at` / `status`），失败方据此重试即可。截止时间二选一：
`extend_seconds`（在**当前截止时间**之上顺延的增量）或绝对
`new_expires_at`（Unix 时间戳）。

修订与**归还 / 撤销 / 用量**共享同一个版本号：这些命令都可携带
`expected_revision` 做乐观并发。因此「续约」与「归还」同时发生时，在锁内
只有一个变更能追加进事件账本，另一方必然收到 `409` 并看到最新状态
（不带 `expected_revision` 的旧调用保持原有行为，向后兼容）。相同
`command_id` 的重试由幂等命令表重放**第一次**的结果，包括返回过的截止时间，
不会因相对延长量而重新计算。修订号、截止时间与修订历史都只存在于事件账本，
重启重放后完整恢复。

### 防超卖 / 一致性的四道防线

1. **进程内串行**：所有命令在同一把锁内“到期结算 → 去重 → 校验 → 原子提交”；
2. **授予守卫**：`amount ≤ avail(lessor)`，跨层并发申请由锁保证读改写原子；
3. **关闭守卫**：显式归还 / 撤销要求持有者让出剩余后余额非负（防止父级缩容
   抽走已分配给孙级的额度）；到期走挂起+延迟结算，离线只关客户端叶子边；
4. **幂等命令表 + 用量回执**：相同 `command_id` / `Idempotency-Key` 重放原
   结果；相同 `usage_event_id` 绝不二次扣减。

### 批量容量调整草案（原子批次 + 乐观版本）

管理员可把多条**节点容量变更**编辑成一个草案（draft）。节点容量定义：
平台为 `global_capacity`；其余节点为其 live 入边（`active`+`pending_expired`）
授予量之和。条目支持绝对目标 `capacity` 或相对 `delta`。

1. **创建**（`POST /api/drafts`）：做参数级校验（节点存在、每节点一条、
   容量非负），并锚定创建时账本位置 `base_seq`。
2. **预览**（`POST /api/drafts/preview`）：在投影的深拷贝上模拟整批，返回
   - `layers`：每个受影响节点容量/可用的「执行前→执行后」逐层余额；
   - `affected_leases`：被收缩/新增的租约边（前后授予量）；
   - `rejections`：逐项拒绝原因（`oversold` / `client_offline` /
     `unchanged` …），一次性列出所有不合法条目；
   - `valid=false` 时发布必然整批不生效。预览本身追加一笔系统事件
     `DraftPreviewed`，把锚点推进到预览事件自身位置（重启可查）。
3. **发布**（`POST /api/drafts/publish`）：
   - **乐观前提**：锚点之后账本若前进过（新的授约/消费/续约/到期结算/
     容量调整…），返回 `409` 且 `details.stale=true`，要求重新预览；
   - 整批在深拷贝上重新规划，**任一项不合法就整批不生效**（不追加任何
     业务事件）；全部合法才把业务事件 + `DraftPublished` 作为**一个
     原子批次**写入哈希链；
   - 首次发布结果（逐层余额、受影响租约、事件数）完整存于事件载荷，
     **重复发布只能得到首次发布结果**（`replayed=true`）。
4. **废弃**（`POST /api/drafts/discard`）：追加 `DraftDiscarded`，此后
   不能再预览或发布。

**缩容记账**：非平台节点缩容时，按授予时间*从新到旧*贪心从入边收回，
每条边最多缩到其已消费量（已消费永不消失，是硬下限）；被缩边产生
`LeaseAdjusted`（lessor 收回、holder 让出，`Σ余额`与总消费不变）。
扩容则向父级新租一条永久租约，受父级当前可用约束。一批中先自顶向下
执行全部扩容、再自底向上执行全部缩容，以最大化整批可行性。

草案状态（`draft/published/discarded`）、锚点、首次发布结果与审计
记录都只存在于事件账本，服务重启后完整恢复；审计新增 `draft_lifecycle`
检查，核对发布事件与原子批次的业务事件数量一致、草案不出现「既废弃
又发布」等非法状态。

---

## HTTP API

| 方法 / 路径 | 说明 |
|---|---|
| `POST /api/bootstrap` | `{capacity}` 初始化全局容量（一次性） |
| `POST /api/nodes` | `{type, parent_id, node_id?}` 注册节点 |
| `POST /api/capacity` | `{capacity}` 调整容量（低于“已承诺/消费下限”拒绝） |
| `POST /api/leases` | `{lessor_id, holder_id, amount, ttl_seconds?}` 申请租约 |
| `POST /api/leases/usage` | `{lease_id, amount, usage_event_id}` 幂等扣减 |
| `POST /api/leases/amend` | `{lease_id, expected_revision, extend_seconds}` 或 `{…, new_expires_at}` 修订有效期 |
| `POST /api/leases/release` | `{lease_id, expected_revision?}` 归还（释放未消费） |
| `POST /api/leases/revoke` | `{lease_id, expected_revision?}` 父级撤销 / 缩容 |
| `POST /api/clients/presence` | `{client_id, online}` 上 / 下线 |
| `POST /api/drafts` | `{draft_id?, items:[{node_id, capacity|delta}]}` 创建批量容量草案 |
| `POST /api/drafts/preview` | `{draft_id}` 预览逐层余额/受影响租约/拒绝原因（推进陈旧锚点） |
| `POST /api/drafts/publish` | `{draft_id}` 原子发布；陈旧或任一项非法则整批不生效 |
| `POST /api/drafts/discard` | `{draft_id}` 废弃草案 |
| `GET /api/drafts` / `?id=` | 草案列表 / 单个草案（状态、锚点、发布结果） |
| `GET /api/state` | 全量节点余额 + 租约视图 |
| `GET /api/audit` | 一致性审计（见下） |
| `GET /api/events` | 最近事件账本 |
| `GET /` | 租约控制台页面 |

所有写命令接受可选 `command_id`（或 `Idempotency-Key` 头）做幂等；
`usage` 必须提供 `usage_event_id`。错误返回结构化 JSON 与状态码：
`400` 参数错误 / `404` 不存在 / `409` 冲突（超卖、重复回执、状态不允许）。

### 审计 `GET /api/audit`

每次执行 8 项检查：哈希链完整、幂等命令表一致、非负余额、全局守恒、
层级边完整、租约记账合法、**从事件重放重建的状态与线上投影逐字段
一致**（`replay_equivalence`），以及**批量容量草案生命周期与原子
批次一致**（`draft_lifecycle`）。

---

## 代码结构

| 文件 | 职责 |
|---|---|
| `quota/ledger.py` | SQLite 事件存储、确定性哈希、批量原子提交 |
| `quota/domain.py` | 事件折叠、命令校验、余额模型、到期结算 |
| `quota/drafts.py` | 批量容量草案：条目校验、逐层模拟、原子发布规划、陈旧检测 |
| `quota/service.py` | 命令执行 / 幂等 / 草案生命周期 / 重建 / 后台 sweep / 审计 |
| `quota/http_server.py` | HTTP 路由与静态页面 |
| `static/index.html` | 租约控制台（节点树、租约操作、容量草案、审计、账本） |
| `test_service.py` | 195 项端到端测试（含真实重启、并发、续约竞争与批量草案） |
