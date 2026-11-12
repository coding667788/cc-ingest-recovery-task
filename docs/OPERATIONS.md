# Windows 操作手册与验收场景复现
所有命令使用 Windows 原生 Python 3.12，仅标准库 + SQLite；不需要 Docker、WSL 或外部服务。
以下命令均在仓库根目录的 PowerShell 中执行。

## 1. 环境与目录准备

```powershell
python --version          # 需要 3.12.x
New-Item -ItemType Directory -Force runtime | Out-Null
python -m unittest discover -s fixtures/tests -v   # 夹具自测，应全部 OK
```

## 2. 启动模拟供应商（窗口一，保持运行）

```powershell
python -m fixtures.server --port 8765 --seed 20261111 --size 24
```

控制面（另开窗口，仅供测试/人工验收，采集进程不访问 `/admin/`）：

```powershell
python -m fixtures.control status
python -m fixtures.control expected
```

## 3. 启动、停止、恢复采集服务（窗口二）

```powershell
# 启动（持续运行，stdout 为 JSON Lines）
python -m syncworker --config task/task-config.json --db runtime/state.sqlite

# 正常停止：在该窗口按 Ctrl+C；或按明确 PID 停止（不要全局杀 Python）
Stop-Process -Id <PID> -Force

# 恢复：保留模拟器运行与同一个 SQLite 文件，去掉崩溃参数再次启动
python -m syncworker --config task/task-config.json --db runtime/state.sqlite

# 只读查看公开视图（单读事务）
python -m tools.read_state --db runtime/state.sqlite
```

后台运行与日志收集：

```powershell
$p = Start-Process python -ArgumentList '-m','syncworker',
  '--config','task/task-config.json','--db','runtime/state.sqlite' `
  -PassThru -RedirectStandardOutput runtime/worker.log `
  -RedirectStandardError runtime/worker.err
Stop-Process -Id $p.Id -Force
```

## 4. 崩溃注入

```powershell
python -m syncworker --config task/task-config.json `
  --db runtime/crash.sqlite --crash-at before_commit --crash-source alpha
# 期望 stdout 出现 event=crash_injected，进程退出码 86
$LASTEXITCODE
# 去掉崩溃参数，用同一数据库重启即可恢复
python -m syncworker --config task/task-config.json --db runtime/crash.sqlite
```

`--crash-at` 可选 `after_receive|before_commit|after_commit`，`--crash-source` 可选
`alpha|beta`。注入仅对已有发布代的第一个非空增量页触发一次。

## 5. 自动化测试入口

```powershell
# 全部测试（约 1 分钟，退出码非 0 即失败）
python -m unittest discover -s tests -v

# 单模块
python -m unittest tests.test_basic_flows -v
python -m unittest tests.test_faults -v
python -m unittest tests.test_crash_recovery -v
python -m unittest tests.test_rebuild -v
python -m unittest tests.test_cursor_and_quarantine -v
python -m unittest tests.test_scale -v
python -m unittest tests.test_units -v
```

测试在进程内启动真实夹具 HTTP 服务，并以子进程运行真实 `syncworker`；生产代码只
调用 `/v1/`，只有测试辅助代码调用 `/admin/`。测试临时库位于 `runtime/tw_*`，可直接
删除清理。

## 6. 逐场景复现步骤

每个独立场景使用新的 SQLite 路径；场景内恢复时保持模拟器运行并复用同一数据库。
“等待追平”使用状态条件：`sync_state.epoch` 等于上游 epoch、`checkpoint=head`、已有
发布 generation；有未解决隔离时 status 为 degraded，否则 running。

### A01 全新启动与源隔离
1. 新模拟器 + 新库启动采集。
2. 等待两源发布追平：`python -m tools.read_state --db runtime/a01.sqlite`。
3. 与 `python -m fixtures.control expected` 按 source/item_id 排序、解码 payload_json 比较；同 item_id 的 alpha/beta 标题不同且分别保留。日志含 bootstrap、publish。

### A02 全量扫描期间更新
1. 启动采集前执行 `python -m fixtures.control mutate-on-scan --source all`。
2. 以 page_size=7 启动，等待追平。
3. 校验 P00000003 删除、P00000002 重建、A00000001 存在，且与 expected 逐条一致。

### A03 乱序、重复、删除重建、同显示时间
1. A01 追平后停止采集，执行 `advance --source all`。
2. 重启并追平；每源本批 7 个唯一 event_id，`sync_applied` 无重复。
3. 再次重启，确认 applied 行数不增加；不同源同键不合并。

### A04 游标过期 vs 历史缺口
1. A01 后停止；`advance --source alpha`，再 `expire-on-page --source alpha --path changes`。
2. 启动采集，日志出现 cursor_expired 但无 history_gap；从数字 checkpoint 恢复后追平，无无限重试。
3. 新场景：启动前 `expire-on-page --source alpha --path snapshot`，验证未发布快照废弃重建，发布数据完整。

### A05 429 与源间公平
1. A01 后 `fault --source alpha --mode throttle --count 3`，再 `advance --source all`。
2. beta 在 8 秒内处理本批；alpha 遵守 2 秒 Retry-After 后追平。
3. `control status` 审计请求时间与 max_active，确认全局≤4、分源≤2，无毫秒级探测。

### A06 503、超时、截断
- `fault --source alpha --mode unavailable --count 3` 后 advance all：有限退避，beta 正常。
- `fault --source alpha --mode delay --seconds 3 --count 2`，配置超时 1500ms：超时重试，任务不膨胀。
- beta `/events` 或 alpha `/changes` 设置 truncate count=1：半页不提交，完整重拉后事件只落一次。

### A07 三个崩溃窗口
对 after_receive、before_commit、after_commit 分别用新库执行；再对 beta 重复 before_commit。
验证退出码 86 与 crash_injected；前两者本页四项全部回滚，after_commit 四项全部提交；重启后与 expected 一致且无重复 applied。

### A08 历史截断重建
1. A01 后停止；advance beta、trim beta、再 advance beta；可加 `fault --source beta --mode delay --path /scan --seconds 3 --count 1` 便于观察。
2. 启动重建，用只读事务持续读取：发布前完整旧代，发布后完整新代，epoch/generation/checkpoint 同时切换。
3. 重建中 advance alpha，alpha 继续推进；重建中强制 kill 再重启，同一数据库自动收敛。

### A09 epoch 改变与版本回退
1. A01 后 advance all 并追平，再 `rotate --source alpha`。
2. 重启后 alpha 发布新代，无 P00000001/旧 A/Z 幽灵数据，revision 1 正常生效；beta 不受影响。
3. 未发布时再次 rotate（测试用 delay 屏障控制时点）：过时代被丢弃，最终按新 epoch 收敛。

### A10 坏字段隔离
1. A01 后停止；`poison --source beta`、`advance --source beta`，重启。
2. P00000004 上一条有效版本仍可读，同页其他事件成功，checkpoint 追平，status=degraded。
3. `sync_quarantine` 含 raw_json、reason、定位字段；重启不重复。`repair --source beta` 后更高有效版本到达，隔离记录 resolved=1。
4. 坏信封/截断响应测试：无法定位或无法完整解析时整页失败，不推进检查点。

### A11 参数与规模
- seed=42,size=31,page_size=1 与 seed=9001,size=300,page_size=19 重复 A02/A03。
- size=5000、page_size=100：记录双源首次发布耗时与峰值任务/在途数，预算 120 秒。

### A12 交付核查
- 从干净目录按本文启动；Ctrl+C 后可重启；测试退出码明确；`fixtures/`、`task/` 保持原样。
- 代码检索可确认采集路径只出现 `/v1/` URL，没有导入 fixtures 或访问 `/admin/`。

### A13 组合故障
1. A01 追平后停止；poison beta、advance all、`fault --source alpha --mode throttle --count 3`。
2. 以 `--crash-at before_commit --crash-source beta` 启动，崩溃后确认 beta 本页隔离、有效变更、applied、checkpoint 一起回滚。
3. 去掉崩溃参数同库重启：beta 8 秒内追平且 degraded、隔离仅一条；alpha 遵守实际 Retry-After 后追平。
4. 屏障测试：重建期间持续写入并在发布前 rotate，读者持续读取，证明只可见完整旧代或完整新代。
