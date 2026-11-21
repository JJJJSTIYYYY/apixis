# 2026-11-20 显式生命周期验证

对照 `a2a288a` 与 validation.json 源码指纹对应的工作区快照，验证显式 create/start/stop/close、纯同步 getter 和终止取消。生命周期契约见[五阶段说明](../core/runtime-lifecycle.md)。

EventWaiter 使用普通 asyncio Future：close 直接 cancel，on_cancelled 调用 _resolve，不改变 asyncio 的任务取消传播。graph 固定引用创建时的组件，不能跨已关闭 core 迁移。

后续将 waiter 的取消通知分离为 `_cancel()`、取消等待统一返回 `None`，恢复原有 `_resolve()`，改为直接完成活跃 waiter，把 waiter 注册、注销收回 registry 统一管理，并将未派发事件改为直接丢弃，删除额外取消通知任务。下列性能、安装和验证记录对应这些调整前的快照；原始数据与源码指纹保留，不代表后续修改已重新计时。

## 正确性与安装验证

| 检查 | 结果 |
| --- | --- |
| 全量 `--asyncio-debug` | 1461 passed，90.27 s |
| eager task factory + debug 的生命周期、关闭、graph 和 waiter 回归 | 83 passed，0.29 s |
| 小规模 graph workload，32 次调用、8 节点、1 轮 | 13 passed |
| 完整 README / 快速开始示例 | 4 段执行通过 |
| 文档 Python 示例与本地链接 | 53 段解析通过，链接存在 |
| wheel / sdist | 离线构建、Twine 严格元数据检查通过 |
| 独立 wheel 安装 | 元数据、显式生命周期、事件和 graph 冒烟通过 |
| wheel 运行时内容 | 47 个文件与工作区逐字节相同 |
| 缓存审计 | 13 个正常场景通过；3 个已知原地修改注册字段的问题继续复现 |
| 静态检查 | Ruff、git diff --check 通过 |

关闭回归覆盖：没有产生过的事件、同名请求 event_id 关联、部分收集、业务 close、任务自然取消、清理代码等待事件、回调异常、取消被吞掉或改抛异常、关闭调用者被取消、handler 内关闭、eager 调度、满队列、待派发事件和旧 graph 引用。stop 回归确认已安排的链继续执行，getter 不恢复消费，显式 start 不丢失队列或捕获的引用。

缓存审计的非零退出来自此前已经记录的三个 findings，未将这些历史缺陷算作本轮修复。原始输出独立归档。

## 配对性能

使用同一 Python 3.12.12 和驱动，按 before → after → after → before 串行运行四个独立进程。每个场景预热后测 5 轮，每版 10 个样本；GC 开启，阈值 [700,10,10]，每轮前 gc.collect()；性能计时关闭 asyncio debug。

| 场景 | before ms | after ms | 耗时变化 |
| --- | ---: | ---: | ---: |
| registry 冷查询，100,000 次，包含 LRU 淘汰 | 45.286 | 45.102 | -0.41% |
| registry 热查询，1,000,000 次 | 113.034 | 114.475 | +1.27% |
| 缓存 pipe 发布，32,768 个事件 | 222.581 | 225.139 | +1.15% |
| 全局 post_event，32,768 个事件 | 229.945 | 225.155 | -2.08% |
| 请求响应，2,048 次 | 178.851 | 187.124 | +4.63% |
| 混合监听，waiter 0% | 35.852 | 36.682 | +2.31% |
| 混合监听，waiter 25% | 115.740 | 116.031 | +0.25% |
| 混合监听，waiter 50% | 175.462 | 176.315 | +0.49% |
| 混合监听，waiter 75% | 236.486 | 233.959 | -1.07% |
| 混合监听，waiter 100% | 315.572 | 312.704 | -0.91% |

混合场景每轮 512 个事件、64 个总监听，覆盖 exact/glob 与 received/processed。各版 workload、容量、回调/结果数量和缓存构建审计一致。

请求响应另外使用同一源码和驱动做独立 ABBA，每 worker 预热后 15 轮，每版 30 个样本：**179.199 → 179.671 ms（+0.26%）**。主对照的 +4.63% 未在这次独立复测中保持，因此不将其判定为固定的请求响应回退；两组原值均保留，不用复测替换主样本。其余已测场景在约 -2.1% 至 +2.3% 之间，也不将小幅波动外推为稳定收益或全负载保证。

两轮配对的源码和驱动指纹一致；运行时代码在计时期间保持不变。

## 证据与复现

- [十项主对照及全部进程样本](./data/2026-11-20-lifecycle/comparison.json)
- [请求响应独立复测](./data/2026-11-20-lifecycle/request-reply-recheck.json)
- [缓存审计及历史 findings](./data/2026-11-20-lifecycle/cache-audit.json)
- [源码指纹和验证记录](./data/2026-11-20-lifecycle/validation.json)
- [相同基准驱动](./data/2026-11-20-lifecycle/driver.py)、[eager 测试插件](./data/2026-11-20-lifecycle/eager_plugin.py)和[文件校验](./data/2026-11-20-lifecycle/manifest.json)

基线可从 `git archive a2a288a src` 提取。仓库根目录运行比较时使用 `scripts/benchmark_runtime.py --baseline-source /tmp/before/src`；上述十项通过重复的 --scenario 参数选择，repeats=5，events=32768，registry-hot-queries=1000000，registry-cold-queries=100000，mixed-listeners=64，mixed-events=512。请求响应复测只选择 event/request-reply，repeats=15。

eager 插件通过 PYTHONPATH 加入它所在目录，再用 `pytest -p eager_plugin --asyncio-debug` 运行 validation.json 中列出的五个测试文件。归档基准驱动支持 --worker-source；配对入口使用仓库根目录的脚本。

[生命周期](../core/runtime-lifecycle.md) · [性能说明](./README.md)
