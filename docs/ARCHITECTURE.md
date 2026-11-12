# 架构、数据库设计与一致性边界说明

本文说明 `syncworker` 的模块划分、SQLite 表结构、事务边界，以及题目要求的四个关键正确性论证：检查点为何不超前、beta 扫描为何不漏变更、epoch 隔离与代际发布如何成立、三个崩溃窗口恢复后允许出现什么状态。

## 1. 模块与并发架构

```text
python -m syncworker
  __main__.py        命令行参数、信号处理
  app.py             装配 Storage、BoundedHttpClient、每个源一个 SourceWorker
  config.py          配置读取与参数校验
  jsonlog.py         stdout UTF-8 JSON Lines
  httpclient.py      全局/分源信号量、HTTP 错误分类、JSON/NDJSON 完整解析
  protocol.py        alpha(JSON) 与 beta(NDJSON) 协议适配，归一化事件结构
  validation.py      信封合法性与业务 payload 规则
  storage.py         SQLite schema、公开四表、暂存/重建内部表、事务
  worker.py          单源状态机：初始化、增量尾读、安全重建、原子发布
  backoff.py         429 Retry-After 与有界指数退避（Event 等待，无忙轮询）
  crash.py           after_receive/before_commit/after_commit 崩溃注入
```

每个源一个工作线程，任一时刻每个工作线程最多一个在途 HTTP 请求。
`BoundedHttpClient` 在请求进入前同时获取全局信号量（配置 `max_concurrency`）
与该源信号量（源 `max_concurrency`），因此即使未来提高单源流水线并发，
也不会突破两级上限。源 A 退避时只等待自己的 `threading.Event`，源 B 的
请求与提交完全独立，不存在全局队列，也不会堆积任务。

## 2. SQLite 表设计

公开表（名称与必要列固定，允许加列）：

| 表 | 关键字段 | 说明 |
|---|---|---|
| `sync_state` | source, epoch, generation, checkpoint, status, degraded, mode, backoff_until, last_error | 每源一行，始终指向当前发布代 |
| `sync_items` | source, epoch, generation, item_id, revision, deleted, payload_json, event_id, updated_at | 仅当前发布代，主键 `(source, generation, item_id)`，删除保留墓碑 |
| `sync_applied` | source, epoch, event_id, item_id, revision, seq | 有效增量事件审计，`(source, epoch, event_id)` 唯一 |
| `sync_quarantine` | source, epoch, event_id, item_id, revision, seq, raw_json, reason, resolved | 坏 payload 隔离，`(source, epoch, event_id)` 唯一 |

内部表（下游不应依赖）：

| 表 | 作用 |
|---|---|
| `sync_staging` | 未发布新一代的物化行，主键 `(source, generation, item_id)`，按 revision 合并 |
| `sync_rebuild` | 每源最多一行的可恢复重建进度（epoch、fence、阶段、数字 catch-up 位置等） |
| `sync_meta` | 每源单调 generation 计数器，重启后不复用旧代编号 |

数据库使用 WAL、`synchronous=NORMAL`、`busy_timeout=30000`。采集进程与只读
读者可以并发打开；所有写连接只有一个，由进程内锁串行化，读者在单个读事务
中看到一致快照。

## 3. 统一事务边界：检查点为何不超前

增量页只有在 HTTP 响应被完整接收、JSON/NDJSON 完整解析、每条信封通过结构
校验后才进入事务。`Storage.begin()` 开启 `BEGIN IMMEDIATE`，在同一个事务内
依次完成：

1. 有效事件写入 `sync_applied`（`INSERT OR IGNORE` 去重）；
2. 新 event_id 才按源内 epoch + revision 合并 `sync_items`，旧 revision 不覆盖；
3. 坏 payload 写入 `sync_quarantine`，同商品更高有效版本可将旧记录置 resolved；
4. 最后才把 `sync_state.checkpoint` 更新为该页服务端给出的 `through`。

因此检查点只能与该页物化数据、去重记录、隔离记录一起可见。解析失败、坏信封
或任何异常都会回滚整事务，检查点保持在上一个已提交位置，重拉从数字检查点
重新开始。日志中的 `page_commit` 只是可观察事件，不是提交凭证。

协议保证 `through` 是“本页处理完成后可以提交的检查点”，最后一页 `through=until`。
本实现从不使用客户端推测位置，也不在读到 window 帧时提前保存检查点；beta
必须等 NDJSON 所有行完整且结尾换行后才解析提交。

网络层不声称 exactly-once：请求可能已被服务端处理而客户端未收到响应，允许
重拉。幂等边界在数据库：`sync_applied` 与 `sync_quarantine` 的唯一键保证重放
不产生重复审计行，revision 合并保证物化数据不重复、不回退。

## 4. beta 实时扫描为什么不漏变更

beta 的 ticket 不冻结数据，扫描期间列表可能变化。实现顺序是：

1. `bootstrap` 记录扫描前的日志高水位 `fence`；
2. 按 Unicode 码点键序完整扫描实时非删除列表，逐页写入 `sync_staging`，同键按
   revision 取高；
3. 扫描结束后才读取一次 `head`，选定有限目标 `target=head`，不能假设扫描行属于
   fence 时刻；
4. 在一个有限窗口 `(fence, target]` 上完整重放变更日志，写入同一暂存代；
5. 原子发布到 `target`。发布后源若继续变化，由普通增量轮询处理。

扫描期间的三类变化都被覆盖：

- 已扫描键范围之后的新增/更新：实时扫描直接读到；
- 已扫描键范围之前的新增键、删除、重建：键分页本身读不到，但变更发生在 bootstrap
  之后，日志 seq 大于 `fence`，由 catch-up 补回；
- 扫描中重复投递或乱序：日志按 seq 读取，业务按 event_id 去重、按 revision 决胜，
  不依赖到达顺序或 `updated_at`。

alpha 的导出是冻结快照，`fence` 与快照属于同一原子时点；快照分页完成后同样从
`fence` 追平一个有限 head，因此分页期间变化不会丢失。

## 5. epoch 隔离与代际发布

epoch 是不透明字符串，绝不排序或比较大小。所有公开行都带 `(source, epoch,
generation)`，版本判断只在同一源、同一 epoch、同一商品键内比较 revision。新 epoch
的 revision 从 1 开始也能生效，因为它写入全新 generation 的暂存表，不与旧 epoch 的
高 revision 比较。

重建触发：

- 增量 `head` 返回不同 epoch，或窗口/扫描返回 409 `epoch_changed`；
- 本地 checkpoint 小于 head 的 floor，或窗口返回 410 `history_gap`；
- 410 `cursor_expired` 不是永久丢历史：增量窗口丢弃不透明游标，从已提交数字检查点
  重开窗口；快照游标失效则重新导出并清空未发布暂存，旧发布代仍可读。

重建过程：

1. 从 `sync_meta` 分配新的单调 generation，立即持久化 `sync_rebuild` 行；
2. 新数据只写 `sync_staging`，公开 `sync_items`/`sync_state` 完全不动；
3. 重建中再次 epoch 改变/历史缺口：丢弃该未发布暂存与 rebuild 行，再分配新一代重来，
   过时代永远没有发布机会；
4. 进程崩溃：旧发布代继续可读；重启后发现未完成 rebuild 行，校验 epoch 后重新建立
   基线（alpha 重新导出、beta 重新 bootstrap+scan），再从数字位置追平；
5. 追平有限 target 后，在单事务内删除旧代 `sync_items`、插入新代暂存行、删除暂存与
   rebuild 行、同时切换 `sync_state` 的 epoch/generation/checkpoint。

读者在一个 SQLite 读事务中只会看到完整旧代或完整新代，不会看到空窗、部分新数据或
“新检查点 + 旧数据”。源持续更新不会阻塞发布：target 是扫描/导出后选定的有限 head，
发布后剩余变化进入正常增量。一个源重建不影响另一个源的工作线程与公开数据。

## 6. 三个崩溃窗口的允许状态

崩溃仅针对“已有发布代的第一个非空增量页”，初始化与重建追平不触发。

| 窗口 | 触发位置 | 崩溃后数据库允许状态 | 重启行为 |
|---|---|---|---|
| after_receive | 页面完整解析后、`BEGIN` 前 | 仍是上一个已提交页：数据、applied、quarantine、checkpoint 均无本页内容 | 从旧 checkpoint 重拉整页，幂等提交一次 |
| before_commit | 事务内所有更新完成、`COMMIT` 前 | 进程死亡导致 SQLite 回滚未提交事务，四项一起保持旧状态 | 同上，重放后只提交一次 |
| after_commit | `COMMIT` 成功返回后、下一次网络请求前 | 四项一起已提交到新 checkpoint | 从新 checkpoint 继续，唯一键阻止重复副作用 |

崩溃使用 `os._exit(86)`，不执行 atexit、连接清理或正常 shutdown；`before_commit` 钩子
确实位于打开的事务内。崩溃参数不跨启动记忆，去掉参数重启即恢复。

## 7. 状态、退避与隔离语义

`sync_state.status` 优先级为 error > backoff > rebuilding > initializing > degraded >
running；`degraded` 由未解决 `sync_quarantine` 派生，能与 rebuilding/backoff 组合并在
重启后恢复。429 使用服务端 `Retry-After`（本题为整数秒）且只作用于该源；503、连接
失败、超时、截断 JSON/NDJSON 使用有界指数退避（0.2s 起、上限 30s，带抖动），所有等待
都是 `Event.wait(timeout)`，没有忙循环。400/404/405 等永久错误进入 error 并以 15s 慢
重试，状态与 `last_error` 持久化，不会静默跳过。
