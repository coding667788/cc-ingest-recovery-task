# 公开验收场景

本文件定义开发目标与人工复现方法，不预填正式作答的分数或 Feedback。`fixtures/tests` 只证明输入夹具能产生所述现象；通过它们不代表采集服务完成。

## 通用操作与观测

服务窗口：`python -m fixtures.server --port 8765 --seed 20261111 --size 24`。

业务窗口（由作答者实现）：`python -m syncworker --config task/task-config.json --db runtime/case01.sqlite`。

控制窗口：`python -m fixtures.control status` 查看上游 epoch/head 与请求审计；`python -m fixtures.control expected` 查看当前非删除有效库存。control 输出的是上游事实，不分析采集实现、不生成评分。

每个独立场景使用新的 SQLite 路径并重启模拟服务；场景内部测试恢复时，**保持模拟服务运行**，保留同一个 SQLite 文件。结束业务进程时使用 Ctrl+C 或按明确 PID 执行 `Stop-Process -Id <PID> -Force`，不要使用全局杀死所有 Python 的命令。

待业务实现后，可执行 `python -m tools.read_state --db runtime/case01.sqlite`，以单个只读事务打印公开视图。比较时按 source/item_id 排序，将 payload_json 解码为 JSON 后比较，不比较 JSON 文本字段顺序。逐条检查商品键、revision、payload，不仅比较总数。expected 的 event_id/updated_at 不在 sync_items 必需列中，不强制比较。

等待“追平”指：上游已停止变化，sync_state 的 epoch 与上游相同、checkpoint=head，存在已发布 generation；有未解决隔离记录则状态为 degraded，否则 running。不要用固定 sleep 代替状态条件；正常小数据场景给予 30 秒收敛预算，性能场景另述。

## 场景与判据

### A01：全新启动与源隔离

1. 全新模拟器、空数据库，启动业务服务。
2. 等待两个源发布并追平，读取 expected 与公开视图。
3. 各源初始 24 个商品；相同 item_id 的标题包含不同源名，应分别保留。公开视图中的 epoch/checkpoint/数据须互相对应。日志能定位初始化和发布。

### A02：全量扫描期间发生更新

1. 全新服务，在启动业务程序前执行 `python -m fixtures.control mutate-on-scan --source all`。
2. 启动业务程序，页大小保持 7；每个源返回第一页后，夹具立即产生一次确定的变化。
3. 最终状态逐条等于 expected，包括排在已扫键之前的 A00000001、P00000003 的删除、P00000002 的重建。
4. alpha 的导出仍是旧快照，beta 的列表是实时变化的；两者都必须通过各自正确的衔接逻辑收敛。只看列表第一页或只从扫描结束时的 head 开始增量会漏数据。

### A03：乱序、重复、删除重建与同时间戳

1. 在 A01 追平后执行 `python -m fixtures.control advance --source all`。
2. 每源产生 10 个投递位置，包含 3 次逻辑重复；显示时间全部相同。
3. P00000001 保留更高 revision；P00000002 保留重新创建的记录；P00000003 不再出现在 live 集合；A/Z 新键存在。
4. 每源本批 7 个唯一逻辑 event_id，sync_applied 对本批不得重复；重启业务后仍不新增重复行。不得把来自不同源的相同商品键合并。

### A04：游标过期与历史缺口不能混为一谈

1. A01 后正常停止业务，模拟器继续运行。先执行 `advance --source alpha`，再执行 `expire-on-page --source alpha --path changes`。
2. 启动业务，alpha 第一页增量返回后即失效所有游标。后续页返回 cursor_expired。
3. 从已提交的数字检查点恢复后追平，业务数据不回滚、不漏事件；没有无休止重试过期 token。
4. 另用全新场景，在业务启动前执行 `expire-on-page --source alpha --path snapshot`，验证未发布的快照可以废弃重建，不能发布不完整列表。

所有控制命令前缀均为 `python -m fixtures.control`；下文省略重复前缀。

### A05：429 与源间公平性

1. A01 后执行 `fault --source alpha --mode throttle --count 3`，然后 `advance --source all`。
2. alpha 收到 429 后的 2 秒冷却只作用于 alpha。beta 在 8 秒内处理本批；alpha 冷却结束后最终追平。
3. 查看 status 的 requests：429 之后的新 alpha 请求须遵守 Retry-After，排除早已在途的请求并允许约 100ms 的记录/调度误差。不能通过每毫秒请求探测是否解封。
4. 在**没有超时遗留服务端请求**的场景，max_active 不得超过配置全局/分源上限。超时后的服务端处理残留不等同于客户端新增在途请求，该场景同时核查客户端日志/测试。

### A06：503、超时和截断整页

独立子场景均从 A01 开始，保持另一源有待处理增量：

- `fault --source alpha --mode unavailable --count 3`；随后 advance all。检查有限退避与恢复，beta 正常推进。
- `fault --source alpha --mode delay --seconds 3 --count 2`；随后 advance all。配置超时为 1500ms，超时后不得形成不断扩大的任务队列。
- 停止业务，advance beta，设置 `fault --source beta --mode truncate --path /events --count 1`，重启。即使 NDJSON 第一行 window 已到达，也不得提交半页检查点；完整重拉后每个有效事件只落一次。
- 对 alpha 的 `/changes` 设置同样 truncate，验证完整 JSON 页的解析失败处理。

### A07：三个崩溃窗口（分别执行）

每个窗口采用独立的新服务/新数据库：完成 A01，停止业务，执行 advance all，然后运行：

```powershell
python -m syncworker --config task/task-config.json --db runtime/crash01.sqlite --crash-at before_commit --crash-source alpha
```

分别以 after_receive、before_commit、after_commit 替换 crash-at；三个子场景都要执行。须出现 crash_injected 并以 86 退出，不能只正常 close 数据库。

崩溃后立即用只读事务查看 SQLite，再去掉 crash 参数重启。

- after_receive / before_commit：本页的业务状态、去重、隔离和检查点均未提交。
- after_commit：四者均已提交，重启不能重复副作用。
- 恢复后两源 live 集合逐条等于 expected，无重复 applied。数据库可正常读取，不需要人工修改状态或删除文件。

另外对 beta 重复 before_commit，以覆盖 NDJSON 连接器。不要只在初始化结束点模拟崩溃。

### A08：历史截断后重建，旧数据持续可读

1. A01 后停止业务，advance beta，然后 trim beta。此时旧 checkpoint < floor，且上游已有删除/重建变化。
2. 设置 `fault --source beta --mode delay --path /scan --seconds 3 --count 1`。启动业务。
3. 在重建期间以只读事务重复观察。公开 beta 数据在发布前仍完整等于旧代；发布后完整等于新代，epoch/generation/checkpoint 同时切换。不得出现空窗、部分新数据或“新检查点+旧数据”。初始为空时允许尚未发布。
4. 在 beta 重建时 advance alpha，alpha 仍能推进。任意时点强制结束业务并重启仍须收敛，不允许人工清库。
5. 作答者需提供有明确同步事件的自动化测试，观测原子发布前后；3 秒延迟仅方便人工观察，不代替事务隔离测试。

### A09：epoch 改变与版本数字回退

1. A01 后 advance all 并追平，再 rotate alpha。
2. 新 alpha epoch 版本从 1 开始，初始集合不含 P00000001，也没有旧的 A/Z 键。
3. beta 不被重置。alpha 发布新代后完全符合新 expected；旧 epoch 的较高 revision 不得阻止新 epoch 的版本 1 生效，也不得留下旧代幽灵数据。
4. 在重建尚未发布时再次 rotate，程序应废弃过时未发布代并自动重建，不能永久卡住。作答者的自动化测试负责精确控制这个时点。

### A10：坏业务字段隔离与继续推进

1. A01 后停止业务，poison beta，再 advance beta，然后重启。
2. P00000004 的坏 stock="broken" 只影响该事件；它的上一条有效记录仍可读，同页其余变更成功处理，检查点能追平。
3. sync_quarantine 保留一个可定位的原始事件及原因；状态明确 degraded。重启不重复隔离，不静默丢记录。
4. repair beta 后，P00000004 的更高有效版本可采集。允许旧隔离记录 resolved=1，也允许保留 unresolved 并继续 degraded；不能伪称隔离记录从未存在。
5. 作答者另写坏信封测试：缺少定位字段与截断响应不等同于坏 payload，不能跳过后直接推进。

### A11：规模和参数变化

使用 seed=42,size=31,page_size=1；再使用 seed=9001,size=300,page_size=19，重复 A02/A03。不得依赖固定商品数、具体 seed、端口或 page_size=7。

扩展场景：模拟器 size=5000，配置 page_size=100。Windows 本机正常负载下，120 秒内双源首次发布；记录实际运行时间与峰值任务数量。此时重点仍是有界分页和正确性；环境异常须如实记录，不能改动正确性条件来压时间。

### A12：交付与操作

从干净目录按文档能够启动；正常退出后可重启；自动化测试有明确的失败退出码；只通过 `/v1/` 采集业务数据。核查 fixtures/ 与 task/ 未被修改，没有实现通往 expected/admin 的业务捷径。

提供架构/事务边界说明、恢复步骤、每个场景的复现方法与已验证/未验证范围。测试不能仅复述实现，也不能靠截图或 success 日志替代实际读取 SQLite 的验证。

### A13：组合故障与同一事务中的坏记录

1. 从 A01 追平状态开始，正常停止业务，保持模拟器运行。执行 poison beta，再 advance all；beta 下一个窗口的第一页包含一个坏 payload 以及有效增量。
2. 执行 `fault --source alpha --mode throttle --count 3`。启动业务，附加 `--crash-at before_commit --crash-source beta`，在 beta 这一页的事务提交前立即退出。
3. 崩溃后读取 SQLite：beta 本页的隔离记录、有效变更、applied 和检查点必须一起回滚，不能只回滚业务表。alpha 允许尚未请求或已进入退避；不能把退出前 alpha 是否恰好收到 429 当成确定性条件。
4. 去掉 crash 参数，以同一数据库重启。beta 在 8 秒内处理这批可用增量，alpha 遵守它实际收到的 Retry-After；beta checkpoint 追平且 degraded，隔离只出现一次，有效商品逐条符合 expected。alpha 最终也追平。
5. 作答者另提供一个使用明确同步屏障的组合测试：某源重建期间持续有新写入，读者持续读取，在发布前改变 epoch；证明旧代可读、过时代不会发布、最终按有限边界发布新代。测试在内部定义和控制事件顺序，不以固定 sleep 或恰好碰到某时点代替条件。该测试可以使用自建测试替身，不得修改固定 fixtures/ 和 task/。

## 录屏建议（由人工实际执行后录制）

90 秒以内的视频可展示一条代表性链路：从干净 SQLite 启动→双源首次发布→一次 advance→读结果→停止业务并设置崩溃参数→崩溃后原库重启→读结果。完整验收在录屏前另行执行，短视频不需要塞入全部场景。若启动或恢复失败，也保留失败过程，不剪掉错误。
