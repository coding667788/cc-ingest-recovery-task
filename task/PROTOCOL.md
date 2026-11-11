# 接口与一致性契约 v1

本文与 PROMPT.txt 共同构成需求。所有接口都在本机模拟器上；业务程序只能访问 `/v1/`。`/admin/` 是开发测试和人工验收的控制面，不是采集数据源。

## 1. 数据、顺序和保证

- 两个源名固定为 `alpha` 和 `beta`，各自拥有独立 `epoch`、日志 `seq` 和商品键空间。相同 `item_id` 在不同源中是不同商品。
- epoch 是不透明字符串，不能排序；改变代表上游历史已重新开始。新 epoch 的 revision 可以从 1 开始。
- 同一 epoch 中，商品 `revision` 是正整数。最高有效 revision 决定该商品状态；不同商品 revision 不能互相比较。删除事件保留版本，重建商品使用更高 revision。
- 日志 `seq` 从 1 单调递增，是投递位置，不是业务版本。旧版本可能在新版本后投递；同一逻辑事件可出现在多个 seq。相同 `event_id` 的内容保持相同，不存在同 revision 的冲突内容。
- `updated_at` 仅为显示信息，多条记录可以相同，不可用作唯一检查点。
- 合法事件信封含 `seq`、`event_id`、`item_id`、`revision`、`deleted`、`payload`、`updated_at`。seq/revision 是整数，item_id/event_id 非空，deleted 是布尔。缺字段或非法信封不得跳过。
- 未删除记录的 payload 为对象，包含非空字符串 `title`、非负整数 `stock`（布尔不是整数）和固定字符串 `currency="CNY"`；删除事件 payload 必须为 null。其他字段可保留。合法信封中的坏 payload 可以隔离，不能覆盖上一条有效版本。
- 快照记录与事件相同，但不含 seq。快照仅包含当前最高有效版本的非删除商品。模拟源的日志可夹带坏 payload，而其当前库存视图仍保留上一条有效业务状态。
- `floor` 表示最小可用的 **after** 检查点：`after >= floor` 可读取全部后续历史；`after < floor` 返回 HTTP 410 `history_gap`。不是“最早事件的 seq”。
- 分页期间源可能变化；变更日志会记录每一次变更。beta 的商品键是不可变字符串，排序使用 Unicode 码点序；键不能重命名，只能删旧键/增新键。
- 测试会让源最终停止变化，以验证收敛。高水位是有限边界，持续有更新时也不得一直等待“全世界静止”才发布。重建期间再遇到 epoch 改变或历史缺口，应废弃未发布代并重新建立基线。

公共接口：`GET /v1/{source}/head` → `{"epoch":"…","head":24,"floor":0}`。不能跨 epoch 比较 head/floor。

## 2. alpha：固定快照 + 不透明游标

### 固定快照

`POST /v1/alpha/exports`，body `{}` → `{"epoch":"…","export_id":"…","fence":24}`。

该导出冻结创建时的有效库存，`fence` 为同一原子时点的日志高水位。随后：

`GET /v1/alpha/snapshot?export_id=…&limit=7[&cursor=…]`

```json
{"epoch":"…","fence":24,"items":[{"event_id":"…","item_id":"…","revision":1,"deleted":false,"payload":{"title":"…","stock":10,"currency":"CNY"},"updated_at":"…"}],"next_cursor":"opaque-or-null"}
```

没有 cursor 表示第一页，next_cursor=null 表示结束。不可从 token 推导页号。快照期间的新变化从 fence 之后的日志补齐。

### 增量

首次：`GET /v1/alpha/changes?epoch=…&after=24&until=40&limit=7`。

续页：`GET /v1/alpha/changes?cursor=…&limit=7`，不要同时提供 epoch/after/until。

```json
{"epoch":"…","until":40,"through":31,"events":[{"seq":25,"event_id":"…","item_id":"…","revision":2,"deleted":false,"payload":{"title":"…","stock":20,"currency":"CNY"},"updated_at":"…"}],"next_cursor":"opaque-or-null"}
```

事件在 `[after+1, until]` 中按 seq 排序。through 是本页处理完成后可以提交的检查点；最后一页 through=until，即使本页为空。until 必须来自本 epoch 的 head，且 `floor <= after <= until <= 当前head`。续页保持 until 不变。

cursor / export 可能失效，返回 410 `cursor_expired`。增量 cursor 失效可用**已经提交的数字检查点**重新开始一个窗口；未提交页不可计入。快照失效需重新导出并丢弃未发布快照，旧发布代仍可读。不能把 cursor_expired 误报成永久丢历史；也不能把 history_gap 当成普通游标失效无限重试。

## 3. beta：实时键分页 + NDJSON 变更

`POST /v1/beta/bootstrap`，body `{}` → `{"epoch":"…","ticket":"…","fence":24}`。

fence 是建立扫描之前捕获的日志高水位，ticket 只绑定 epoch，**不冻结数据**。

`GET /v1/beta/scan?ticket=…&limit=7[&after_key=…]`

```json
{"epoch":"…","rows":[{"event_id":"…","item_id":"…","revision":1,"deleted":false,"payload":{"title":"…","stock":10,"currency":"CNY"},"updated_at":"…"}],"next_key":"last-key-or-null"}
```

只返回当前非删除、`item_id > after_key` 的记录，按键排序。next_key=null 表示本次扫描结束。与 fence 后日志结合可以补回扫描期间在已扫描键范围新增的记录、处理更新和删除。扫描结束后才能选取用于追平的有限 head；不能假定扫描读到的记录均属于 fence 时刻。

`GET /v1/beta/events?epoch=…&after=24&until=40&limit=7`

响应 `application/x-ndjson`，**每行包含结尾换行，最后一行也必须完整**：

```text
{"kind":"window","epoch":"…","until":40,"through":31,"more":true}
{"kind":"change","offset":25,"change":{"event_key":"…","key":"…","rev":2,"op":"upsert","body":{"title":"…","stock":20,"currency":"CNY"},"updated_at":"…"}}
```

change 的 op 只允许 upsert/delete，删除 body=null。more=false 表示窗口完成；续页以已提交的 through 作为 after，until 不变。window 语义与 alpha 相同。解析完整页后才能提交 through；不能读到 window 就先保存检查点。

## 4. 错误和调度

- 错误体为 JSON `{"error":"code"}`。409 `epoch_changed`、410 `history_gap` 触发重建。
- 410 `cursor_expired` 按上述协议恢复；404、400 等永久接口/配置错误应呈现明确状态，不能无限快速重试。
- 429 的 Retry-After 是十进制秒数，本题使用整数 2。对**该源**后续请求生效；已在途请求可以完成。503、连接失败和超时应有有界退避。坏 JSON/截断 NDJSON 属于整页读取失败。
- 正常情况下，全局最大在途请求数=配置 max_concurrency，各源上限=配置值；失败时也不能突破。源 A 在退避时，源 B 仍应在 8 秒内处理一批变更（本机正常负载、24~300 条初始记录）。不是每个商品都要求并发处理。
- page_size 支持 1~100，size 支持 8~20000。性能扩展场景 size=5000、page_size=100，在本机正常负载下 120 秒内完成初始化，允许分页暂存，不允许无限任务队列。不是用微小时间阈值判断语义正确性。
- 服务端可能已接受请求但客户端尚未收到响应。重新请求不能假定“上次一定没有执行”；本题导出/扫描 ticket 的重复创建不修改业务库存，可安全重试。

## 5. 对外读取与持久化约定

内部表结构、并发架构和事务策略自行设计，但须提供以下 SQLite 表或视图，列名固定，可加列：

| 名称 | 必要列 |
|---|---|
| sync_items | source, epoch, generation, item_id, revision, deleted, payload_json |
| sync_state | source, epoch, generation, checkpoint, status, last_error |
| sync_applied | source, epoch, event_id, item_id, revision |
| sync_quarantine | source, epoch, event_id, item_id, revision, raw_json, reason, resolved |

sync_items 只暴露每个源**当前发布代**的数据，deleted 用 0/1，删除记录允许保留为墓碑；验收非删除数据时过滤 deleted=0。payload_json 为业务 payload 的 JSON。

sync_state 每个源一行，epoch/generation/checkpoint 指向**当前发布代**；未首次发布时允许 epoch/generation=null、checkpoint=0。status 至少区分 initializing/running/rebuilding/backoff/degraded/error。重建中不得把新检查点与旧数据配对；内部扫描进度可另建表。存在未解决隔离记录时必须可识别 degraded（重建/退避中的细分可通过额外列或结构化日志说明）。

sync_applied 对已接受的有效**增量事件**按 (source,epoch,event_id) 唯一，重投/重拉不能多行；旧版本虽不覆盖当前状态，仍可记录为已处理。初始化快照不要求写入 applied。历史缺口之前未曾观察到的事件不要求补造审计。原有已提交审计可以跨代保留。

本题的“可观察副作用不得重复”指持久化 applied/quarantine 记录及物化数据，不要求 stdout 重试日志只打印一次，不要求对外部消息系统实现分布式 exactly-once。

sync_quarantine 对坏 payload 按 (source,epoch,event_id) 唯一，必须包含原始事件、定位字段和非空原因。只有后续同商品更高有效 revision 到达才可以标 resolved=1，或一直保留 unresolved 并保持 degraded；不能在重启时静默清空。

**业务数据更新、有效事件去重、坏事件隔离、through 前进**必须具有共同事务边界，不能检查点先走、数据后写。重建最终发布涉及的公开数据、epoch、generation、checkpoint 也必须原子切换。读者通过一个 SQLite 读事务同时读取上述视图；不要求跨不同读事务看到同一版本。

日志为 stdout UTF-8 JSON Lines，至少包含 event/source/epoch/checkpoint（不适用可 null），并能定位 bootstrap、publish、page_commit、retry、quarantine、epoch_changed、history_gap、shutdown。日志不是数据库提交凭证，不可只根据打印 success 判定成功。

## 6. 可重复崩溃验证

除基础入口外，支持 `--crash-at after_receive|before_commit|after_commit --crash-source alpha|beta`。仅针对**已有发布代的第一个非空增量页**触发，初始化和重建追平不触发：

- after_receive：完整接收并解析该页后，开始持久化事务前。
- before_commit：该页数据/去重/隔离/检查点都已在事务内更新，commit 调用之前。
- after_commit：commit 已成功返回，下一次网络请求之前。

触发时输出并刷新 `event=crash_injected`，随后使用立即进程退出（退出码 86），不运行正常清理。再次启动时去掉 crash 参数即可恢复，不要求跨启动记住注入开关。事务内钩子必须真的位于事务内，不能用正常退出假装断电。

自动化测试应使用明确的事件/进度条件和有限超时等待，覆盖读事务看不到未发布代。测试可自己启动/终止服务、驱动 admin；生产采集路径不得接触 admin。
