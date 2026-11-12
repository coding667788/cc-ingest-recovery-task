# 验证结果与覆盖矩阵

本文记录在本机（Windows 原生 Python 3.12.8，SQLite 标准库）执行的自动化验证。
测试不使用 sleep 猜测语义：进程内夹具 + 子进程采集器通过明确的数据库状态、日志事件、
同步屏障和有限超时等待条件。

## 1. 自动化测试入口

```powershell
python -m unittest discover -s tests -v
```

## 2. 测试到验收场景的覆盖矩阵

| 测试文件/用例 | 覆盖验收点 | 关键断言 |
|---|---|---|
| `test_basic_flows.test_a01_fresh_bootstrap_and_source_identity` | A01 | 双源首次发布、checkpoint=head、同键跨源不合并 |
| `test_basic_flows.test_a02_mutation_during_full_scan` | A02 | 扫描中新增/删除/重建经快照+日志/扫描+日志衔接收敛 |
| `test_basic_flows.test_a03_ordering_duplicates_recreate_shared_timestamp_and_restart` | A03 | 乱序、重复投递、删除重建、同时间戳、重启无重复 applied |
| `test_cursor_and_quarantine.test_a04_changes_cursor_expiry...` | A04 | cursor_expired 从数字 checkpoint 恢复，不误判 history_gap |
| `test_cursor_and_quarantine.test_a04_snapshot_expiry...` | A04 | 未发布快照废弃重建，不发布不完整列表 |
| `test_faults.test_a05_throttle...` | A05 | Retry-After 源级生效、beta 8 秒内推进、并发上限 |
| `test_faults.test_a06_unavailable...` | A06 | 503 有界退避、另一源继续 |
| `test_faults.test_a06_timeout...` | A06 | 超时不扩大任务队列、在途上限 |
| `test_faults.test_a06_truncated_beta_ndjson...` | A06 | 半页不提交、重拉后事件只落一次 |
| `test_faults.test_a06_truncated_alpha_json...` | A06 | JSON 截断整页失败与幂等重放 |
| `test_crash_recovery.test_a07_three_crash_windows...` | A07 | 三窗口 + beta before_commit，退出码 86，回滚/提交语义 |
| `test_rebuild.test_a08_history_gap_rebuild...` | A08 | 读者持续读，只见完整旧代或完整新代 |
| `test_rebuild.test_a08_other_source_keeps_advancing...` | A08 | 一源重建不阻塞另一源 |
| `test_rebuild.test_a08_force_kill_during_rebuild...` | A08 | 重建中强杀，旧代保持可读，同库重启收敛 |
| `test_rebuild.test_a09_epoch_rotation...` | A09 | 新 epoch revision 1 生效、无旧代幽灵、beta 不受影响 |
| `test_rebuild.test_a09_second_rotation...` | A09 | 未发布代遇再次 rotate 被丢弃并重建 |
| `test_cursor_and_quarantine.test_a10_poison...` | A10 | 坏 payload 隔离、degraded、重启不重复、repair 后 resolved |
| `test_cursor_and_quarantine.BadEnvelopeTests` | A10 | 缺定位字段坏信封整页失败，检查点不前进 |
| `test_scale.test_a11_*` | A11 | seed/size/page_size 变化；5000 行 120 秒预算与并发上限 |
| `test_crash_recovery.test_a13_combined...` | A13 | 限流 + 坏记录 + before_commit 同事务回滚，恢复后隔离仅一次 |
| `test_rebuild.test_a13_continuous_writes...` | A13 | 重建持续写入并在发布前 rotate，有限边界发布新代 |
| `test_units.*` | 支撑逻辑 | 退避边界、信封/payload 规则、JSON/NDJSON 完整性 |

## 3. 实际运行结果

最终完整运行（Windows 原生 Python 3.12.8）：

```text
python -m unittest discover -s tests -v
Ran 30 tests in 62.421s
OK
```

退出码为 0。夹具线程在连接被采集进程主动关闭时打印的
`ConnectionResetError [WinError 10054]` 来自模拟服务器的请求审计线程，属于测试
夹具的连接回收噪声，不影响断言；所有用例结果均为 ok。

规模场景实测（`test_a11_large_initial_publish_under_120_seconds`，seed=777,
size=5000, page_size=100）：双源首次发布在 120 秒预算内完成；夹具审计
`max_active` 全局不超过配置 4、分源不超过 2。由于每源工作线程任一时刻只有一个
在途请求，客户端峰值任务数恒为每源 1、全局最多 2，低于配置上限，不存在无界队列。
小数据场景（24~300 条）端到端收敛均在数秒内；A05 断言 beta 在 8 秒内、A13 组合
故障断言 beta 在 8 秒内处理可用增量，均通过。

崩溃注入实测：三种 `--crash-at` 与 beta `before_commit` 均输出 `crash_injected`
并以退出码 86 结束；`os._exit(86)` 不执行正常清理。

## 4. 已知边界与未使用假设

- 网络投递不保证 exactly-once；系统保证重拉后的数据库副作用幂等。
- 不透明游标不跨进程持久化，崩溃/重启后从数字 checkpoint 或重新导出/扫描恢复。
- 永久接口错误（400/404/405 等）进入 error 并慢重试，不会静默成功。
- 测试夹具的 `/admin/` 只被 `tests/support.py` 调用；`syncworker/` 生产代码只构造 `/v1/` 请求。
