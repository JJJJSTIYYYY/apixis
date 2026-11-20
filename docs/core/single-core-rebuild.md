# 共享 core 重建记录

本轮以 `5049c7d` 的父提交 `f8e885d` 还原工作树，再重建后续独立的功能、优化和修复。原提交历史保留，用于逐项核对。

运行时恢复为一个共享 event core、一个 asyncio 事件循环。graph 保留，使用同一组 registry、pipe、handler registry 和 event loop。版本保持 `3.0.0`。

## 逐项落实

| 原提交 | 重建内容 | 移除或不恢复的内容 |
| --- | --- | --- |
| `5049c7d` | 全局 `post_event()`，沿用本地 pipe 的上下文捕获和许可借还 | 无 |
| `51f7bea` | `AGENTS.md`、非线程缓存审计和本地发布基准 | 桥接、跨线程发布和等待、线程压力参数 |
| `9227e95` | 独立等待缓存、received/processed 位置、普通优先级范围、监听清理及 graph 终端 hook 顺序 | 已被单 Future 约束替代的延迟超时实现 |
| `35cbd85` | 无界信号量使用独立计数字段，保留有界 asyncio 计数 | 桥接队列、锁、唤醒与取消优化 |
| `b18263b` | base、handler registry、subscription 按职责拆包，共享匹配工具，测试按职责归属 | 线程 waiter 模块 |
| `16741c3` | `EventType.DEFAULT`、`create_event()`、`post_and_wait()`、`expect_event()` / `wait_event()` 和原本地别名，pipe 直接使用 asyncio.Queue | 自动跨线程转发、桥接入队、远程等待；BuiltinChannel 包装层保持移除 |
| `66a9227` | 同步 predicate、不可变本地状态快照、许可统计、流耗尽后重复读取继续结束 | 桥接状态类型与字段、远程 predicate 转发 |
| `572a1b8` | 版本变更已由当前 `3.0.0` 覆盖 | 中间版本不恢复 |
| `33022f6` | 本地注册/注销及 subscriber 的职责归属 | 远程注册、异步注册外观和按线程改变返回类型 |
| `464e52b` | 本地同步 API、已完成结果在清理后可读 | 线程局部 core、命名空间、路由、多目标聚合、专属聚合工具 |
| `78684a2` | 本地事件名注册表去锁，元数据读取使用必要的浅拷贝 | 桥接重入和远程操作调度 |
| `9d0dd66` | 无有效 handler chain 时的空上下文快路径，本地基准与历史证据 | 线程 waiter 快路径及跨循环许可归属检查 |
| `612d5d0` | 应用注入 `Logger` 回调及全部调用点，删除 LOG_LEVEL | 多线程共享日志的说明 |
| `058dc4b` | 普通版本 CI 和单事件循环压力验证保留 | 多核/无 GIL 专属 CI、开关和测试 |
| `c838c16` | 版本 `3.0.0` | graph 删除不再落实；graph 实现、导出、异常、配置、文档、测试和基准均恢复 |
| `7b05584` | 本地 waiter 绑定单 Future，固定注册期限，完成/超时/取消清理，清理先于许可恢复；post_and_wait 沿用相同约束 | 远程 waiter、远程结果 Future 和跨线程清理确认 |

graph 的 16 个源码文件逐一核对。除 `node_graph.py` 的等待缓存终端 hook 改进和 `stream_writer.py` 的流结束修复外，其余文件与 `f8e885d` 相同。线程局部图命名空间没有恢复。

## 生命周期与调用约束

- core 继续按需构造，同步 getter 在 asyncio 中安排启动，异步 getter 等待启动。订阅、等待器和 graph 共用组件。
- `stop_core()` 继续暂停消费，保留组件、注册、队列和待派发事件，支持原事件循环内重启。已启动的派发和后台 handler 继续运行。
- 本地 handler 的注册和注销保持同步。已捕获的 handler chain 不因后续注销而改变。
- waiter 构造时注册，一个 Future 对应一次收集，固定期限，终态清理。不恢复 keep_listening、期限重设或延迟超时续等。
- 已完成 Future 的结果、异常和取消状态可重复读取。上下文只能进入一次。
- 单事件循环内的并发、嵌套图、ParallelNode、背压、取消和许可借还继续保留。

## 验证入口

```bash
python -m pytest
python -m pytest --asyncio-debug
python -m pytest tests/performance --run-performance \
  --graph-count 32 --graph-nodes 8 --graph-repeats 1
ruff check .
git diff --check
```

另检查源码无桥接或多线程依赖、公开导出移除、graph 源码来源、wheel/sdist 内容、隔离安装后的事件与 graph 冒烟、文档本地链接及性能历史 manifest。

本轮验证结果：

| 检查 | 结果 |
| --- | --- |
| 还原后的 `f8e885d` 基线 | 1475 项测试通过 |
| 重建源码，显式 --asyncio-debug 并断言已开启 | 1440 项测试通过 |
| eager task factory 与显式 debug 下的背压边界、图压力、发布等待和 waiter 生命周期 | 238 项测试通过 |
| 64 张父图，每张 64 次并发调用，派发/队列容量 {1,2,3} 九种组合 | 18 项压力测试通过，含取消、超时、迟到回包与重启恢复 |
| 默认/eager 调度下的启动、满队列重启、嵌套请求响应 | 18 组场景通过 |
| 隔离安装 wheel，全部测试使用安装包中的运行时 | 1440 项测试通过 |
| Graph 性能 workload 小规模验证 | 13 组通过 |
| 本地事件、graph、getter、混合等待基准 | 8 个场景通过 |
| 缓存审计 | 13 个正常场景通过，3 个历史问题复现，见下文 |
| wheel/sdist | 构建、Twine 严格元数据检查、两种隔离安装和冒烟通过 |
| 发布包内容 | 46 个运行时文件与工作区逐字节一致 |
| 文档 | 52 个 Python 示例语法解析通过，本地链接及两个性能 manifest 校验通过 |
| 静态检查 | Ruff 和 git diff --check 通过 |

## 已有缓存审计问题

审计工具仍能复现直接修改已注册 handler 的 subscribe/filter 字段，再调用底层 registry 注册时的旧缓存不一致。对照 `f8e885d`，缓存链与重新构建链的差异相同；该问题不是本轮重建引入，也不属于这些提交中已完成的修复。

本轮保留正常注册路径的行为。应用更新注册信息使用 `subscribe()` 或 handler 的 `register()`，历史审计项仍单独记录在工具输出的 findings 中。

[事件 API](./event/README.md) · [图 API](./graph/README.md) · [测试说明](../../tests/README.md)

重建后的性能对照与查询入口后续优化见[性能核对](../performance/2026-11-20-single-core-recheck.md)和[查询入口优化](../performance/2026-11-20-handler-query-optimization.md)。
