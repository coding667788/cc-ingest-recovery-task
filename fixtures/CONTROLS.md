# 模拟数据源故障控制

所有命令均从仓库根目录运行。默认地址 `http://127.0.0.1:8765`，可加 `--base` 更换。控制面只供测试驱动访问。

| 命令（前缀 `python -m fixtures.control`） | 效果 |
|---|---|
| status | 上游 epoch/head/floor、最近最多 4000 个请求的单调时钟起止时间/状态、最大并发 |
| expected | 当前两源有效的非删除库存事实；不是采集实现的测试报告 |
| advance --source all | 每源产生 10 个投递位置：7 个唯一事件及 3 次重投；更新/乱序/删除重建/删除/前后键新增 |
| poison --source beta | 对 P00000004 产生合法信封、非法 stock 的新版本；当前有效库存保持不变 |
| repair --source beta | 为 P00000004 产生更高的有效版本 |
| trim --source beta | floor=head，并截断所有已有事件；当前库存保留，后续事件仍正常追加 |
| rotate --source alpha | 创建新 epoch，revision 重置、移除旧动态键及 P00000001；清除该源旧故障配置 |
| expire --source alpha | 立刻令已有导出及不透明游标失效，历史日志仍保留 |
| mutate-on-scan --source all | 各源下一张 snapshot/scan 页取数后立即执行一次 advance；只触发一次 |
| expire-on-page --source alpha --path changes | 下一张增量页取数后失效游标；也可用 --path snapshot 测试导出失效 |
| fault --source alpha --mode throttle --count 3 | 连续 3 轮源级 429，每轮 Retry-After=2 秒；冷却期间请求仍返回 429 |
| fault --source alpha --mode unavailable --count 3 | 接下来 3 个匹配请求返回 503 |
| fault --source beta --mode truncate --path /events --count 1 | 下一次匹配响应去掉末尾字节，Content-Length 为实际截断长度；测试解析完整性 |
| fault --source alpha --mode delay --seconds 3 --count 2 | 接下来两个匹配请求各延迟 3 秒；等待时不持有全局锁 |
| clear-faults --source all | 清除临时故障、冷却以及尚未触发的变更/过期钩子；不改变业务数据 |

每个源同一时刻最多有一个 fault 配置，新 fault 覆盖旧配置。`--path` 是 URL 路径的子串过滤，默认匹配全部数据接口；expire-on-page 的 path 特别取 changes/snapshot 两个动作名。fault 不影响 admin 接口。

重启模拟器会生成不同 epoch，不能把重启模拟器当成“只重启采集程序”。size/seed 相同可复现业务样本，但 epoch 和不透明 token 每次不同。所有 stock、title、商品键均为虚构，无供应商凭据。
