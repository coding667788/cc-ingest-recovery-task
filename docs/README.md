# syncworker 交付说明

多源商品库存采集服务，仅使用 Python 3.12 标准库与 SQLite。

- 架构、数据库设计与一致性论证：`docs/ARCHITECTURE.md`
- Windows 启动/停止/恢复与 A01-A13 复现步骤：`docs/OPERATIONS.md`
- 自动化测试覆盖矩阵与实际结果：`docs/VERIFICATION.md`

快速开始：

```powershell
python -m fixtures.server --port 8765 --seed 20261111 --size 24
python -m syncworker --config task/task-config.json --db runtime/state.sqlite
python -m tools.read_state --db runtime/state.sqlite
python -m unittest discover -s tests -v
```
