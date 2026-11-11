# 多源增量采集与故障恢复：0—1 开发任务

本仓库是任务的**初始材料**，只有模拟供应商接口、协议、题面及夹具自身测试；尚未实现待开发的采集系统。数据全部为本项目生成的虚构商品库存。

目标领域：数据工程 / 数据采集、爬虫与 RPA。任务类型：功能开发。难度目标：地狱，需按完整任务进行正式判定。

## 开始阅读

1. `task/PROMPT.txt`：完整的单轮开发请求。
2. `task/PROTOCOL.md`：上游保证、接口与持久化输出约定。
3. `task/ACCEPTANCE.md`：公开验收场景、触发步骤与判断依据。
4. `task/task-config.json`：本地示例配置。

## 环境

Windows 原生 Python 3.12，Git；模拟接口与自身测试只用标准库，无需安装依赖、Docker、WSL、云服务或真实账号。题目实现也使用 Python 3.12 标准库和 SQLite。

```powershell
python -m unittest discover -s fixtures/tests -v
python -m fixtures.server --port 8765 --seed 20261111 --size 24
```

第二条命令持续运行，在另一个 PowerShell 窗口操作：

```powershell
python -m fixtures.control status
python -m fixtures.control advance --source all
python -m fixtures.control expected
```

终止模拟服务可按 Ctrl+C。每次重启服务恢复到指定种子的初始状态。恢复采集进程时应保留服务运行与 SQLite 文件；不得同时重启服务而误把新的上游当成旧实例。

`fixtures.control` 是供开发测试及人工验收使用的故障控制器。待开发的 `syncworker` 只能访问 `/v1/` 数据接口，不能访问 `/admin/`、导入夹具代码或直接读取夹具内存来获取答案。

## 尚待开发

仓库目前没有 `syncworker`。其最小运行入口约定为：

```powershell
python -m syncworker --config task/task-config.json --db runtime/state.sqlite
```

采集服务持续运行、定期检查增量，直到正常退出或被结束进程。SQLite 的公开读取视图以及结构化运行日志用于验收，不要求网页管理后台。

本仓库公开提供题目输入与后续作业快照，评审无需登录即可读取。正式作答开始前记录初始提交的 40 位完整 SHA 链接；产物提交保留模型历史，初始提交必须是产物的祖先。不得 force-push、rebase 或 squash 已记录的快照。当前仅有题目输入，尚未执行正式模型作答。
