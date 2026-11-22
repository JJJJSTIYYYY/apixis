# Apixis 文档与代码一致性核对

核对日期为 2026-11-22，基线为本地 `master` 的 `5220a2c`（Apixis `3.0.0`）。基线共确认 **17 处不一致或缺少必要前提**，另列 5 项接口细节。后续已修复 L1、R1；本次文档整理对齐 Markdown 指南，保留决定不修复的实现边界。

范围包括根 README、快速开始、接口与生命周期说明、配置、源码与测试指南、性能记录及既有审计报告，共 22 份 Markdown。历史记录按各自注明的源码快照解释。依据为当前实现、既有测试、公开接口探针和文档命令实跑。

【疑似缺陷】表示实现或工具可能违反已有行为契约。【文档未对齐】表示核对时的说明、示例或使用前提需要同步。以下问题描述保留发现时的现象，后续状态以本表和各项修复说明为准。

## 文档对齐状态

本次仅调整 Markdown 文档及其组织，不改运行时代码、源码 docstring、测试实现或历史原始数据。指南分为[使用](../usage/README.md)、[开发](../development/README.md)、[测试](../testing/README.md)、[审查](README.md)四部分；编写规则见 [文档编写要求](../README.md#文档编写要求)。

| 项目 | 处理状态与当前说明 |
| --- | --- |
| L1 | 已有代码修复保留；[处理器取消清理](../usage/event/handlers.md#取消清理)与当前阶段取消检查一致 |
| L2 | 已对齐；[关闭说明](../usage/lifecycle.md#关闭)明确 waiter 返回与取消清理可交错 |
| E1 | 已对齐；[等待结果](../usage/event/README.md#关闭时的等待结果)区分期限核对与关闭直接完成，先终态优先 |
| E2 | 已对齐；[队列发布](../usage/event/channels.md#发布与等待)说明创建后可先入队，无业务订阅也可等待本次前台处理 |
| E3 | 已对齐；[取消清理](../usage/event/handlers.md#取消清理)单列 5 秒协作超时及仅日志处理 |
| E4 | 已对齐；[通知回调](../usage/event/handlers.md#通知回调)明确 awaitable 要求及注册与执行校验边界 |
| E5 | [等待使用示例](../usage/event/README.md#等待事件)只用公开接口；源码 docstring 的私有 pipe 示例保留，遵守本轮不修改代码的范围 |
| G1 | [构图说明](../usage/graph/README.md#构建图)改用字符串 `"<global>"`；两处源码 docstring 的内部常量引用保留 |
| G2 | 已对齐；[快照示例](../usage/graph/README.md#快照)在活跃节点内保存，[KeepRef](../usage/graph/state.md#keepref)明确快照完整深拷贝 |
| G3 | 已对齐；[Command](../usage/graph/state.md#command)说明跨节点冲突、同节点覆盖和已提交更新不回滚 |
| G4 | 已对齐；[图调用](../usage/graph/README.md)区分所属 core、新调用拒绝与已接纳调用取消；内部归属放入[开发设计](../development/graph-runtime.md) |
| G5 | 已对齐；[中断说明](../usage/graph/README.md#interrupt--block)按 `event.seen` 解释异常，明确普通订阅与后台执行边界 |
| C1 | 已对齐；[队列与并发](../usage/config/README.md#队列与并发)描述共享本地队列与独立前台许可 |
| C2 | 不予修复；[日志配置](../usage/config/README.md#事件日志输出)说明 YAML 布尔用法及字符串 `"false"` 的真值行为 |
| R1 | 已有文档修复保留；命令集中到[当前源码跨版本比较](../testing/performance/README.md#当前源码的跨版本比较)，基线为 `4253f73` |
| R2 | 已对齐；[历史诊断复现](../testing/reproduction.md#2026-年-11-月-22-日诊断基线)选择 `a93d34e`，单列配置子探针使用当前源码的边界 |
| R3 | 已对齐；[历史性能报告](../testing/performance/README.md#历史测量报告)固定源码、驱动和环境，11 月 21 日报告给出归档重建命令 |
| 取消回调子 Task 关闭 | 不予修复；[使用边界](../usage/lifecycle.md#关闭)及[最小复现](../testing/reproduction.md#取消清理中的关闭重入)明确父子互等和协作超时 |
| 默认组注销 waiter 监听 | 已对齐；[注销说明](../usage/event/handlers.md#替换查询与注销)说明移除监听不会主动完成等待 |
| 晚注册 waiter | 已对齐；[监听时点](../usage/event/README.md#等待事件)明确必须早于出队捕获处理链 |
| 空事件名 | 已对齐；[发布事件](../usage/event/README.md#发布事件)区分两个发布接口现有行为 |
| 查询接口遗漏 | 已补全；[查询说明](../usage/event/handlers.md#替换查询与注销)包含 `is_registered()` 和 `get_unmatched_subscriptions()` |

## 生命周期

1. **【疑似缺陷】L1 关闭取消后，同一 handler 仍可进入业务函数。** 生命周期说明承诺，即使吞掉取消，也不会再执行后续业务回调。实际在 `on_has_error` 等待时调用 `close_core()`，该回调捕获并吞掉 `CancelledError` 后，设置了 `stop_when_error=False` 的同一 handler 仍会执行 `core_func`。前台和后台均可复现。取消检查发生在整个 handler 执行之后，`on_has_error` 与后续回调之间存在空档。若后续业务继续挂起，关闭也要等待它结束。这是实现与现行承诺的差异，复现见文末。 [生命周期说明](../development/runtime-lifecycle.md#close-的取消顺序) · [handler 阶段执行](../../src/apixis/core/event/base/handler.py#L117) · [前台派发](../../src/apixis/core/event/event_loop.py#L290) · [后台执行](../../src/apixis/core/event/event_loop.py#L401)

   **已修复。** `_execute_func()` 在回调返回后检查当前 Task 的取消状态，并结合前台事件取消标记或后台执行身份重新抛出 `CancelledError`，随后沿既有路径执行取消清理。新增 4 项回归覆盖前台、后台及后续 core/on_accepted 两个分支。修复后默认测试 `1629 passed`。 [阶段取消检查](../../src/apixis/core/event/base/handler.py#L150) · [新增回归](../../tests/core/event/test_close_core.py#L610)

2. **【文档未对齐】L2 在途取消通知和 waiter 直接完成可以交错。** 关闭说明将二者列为先后的第 4、5 步。实测阻塞前台 `on_cancelled` 时，后续 processed waiter 已返回 `None`，其 predicate 调用次数为零，关闭任务仍在等待清理。实现先请求任务取消，随后直接完成仍活跃的 waiter，再汇合任务。文档应明确这两条路径可交错，避免调用方依赖通知先于 waiter 返回。 [关闭顺序](../development/runtime-lifecycle.md#close-的取消顺序) · [关闭实现](../../src/apixis/core/event/event_loop.py#L140) · [关闭回归](../../tests/core/event/test_close_core.py#L460)

## 事件与订阅

1. **【文档未对齐】E1 等待期限说明省略了两条终态路径的优先规则。** 事件页将非正数写为立即超时，并概括期限到达就抛 `TimeoutError`。构造时实际安排定时器，正常解析和真实事件取消会先核对期限，过期时跳过 predicate 并置为 `TimeoutError`。关闭直接完成尚处于 pending 的 Future 时可先得到 `None`，即使期限已到但定时器尚未运行。已有回归分别固定两种行为。 [等待参数](../usage/event/README.md#等待事件) · [关闭时的等待结果](../usage/event/README.md#关闭时的等待结果) · [期限与真实事件取消](../../src/apixis/core/event/subscription/waiter.py#L149) · [关闭直接完成](../../src/apixis/core/event/handler_registry/registry.py#L124) · [期限回归](../../tests/core/event/test_waiter_lifecycle.py#L14)
2. **【文档未对齐】E2 发布前置条件写得过严。** 通道页要求发布前创建 core、注册业务 handler 并启动消费。公开接口实测只创建 core 即可 `post_event()`，从状态快照能看到事件已入队，启动后才开始消费。`post_and_wait()` 自带 processed 监听，在运行中的 core 上没有业务订阅也能返回本次事件。 [通道说明](../usage/event/channels.md#发布与等待) · [发布与安装监听](../../src/apixis/core/event/subscription/publication.py#L17) · [无业务订阅回归](../../tests/core/event/test_post_and_wait.py#L71)
3. **【文档未对齐】E3 `on_cancelled` 的超时和错误处理规则需要单列。** 处理器页写 `time_out` 适用于每个回调，并概括自身回调的普通异常会触发 `on_error`。前台和后台的框架取消都显式传入 5 秒，使用协作式 `asyncio.timeout()`。实测将 handler 的 `time_out` 设为 1 ms，`on_cancelled` 仍完成了 20 ms 的等待，随后抛出的异常只写日志。清理异常既不触发 `on_error`，也不追加业务 `error_stack`。 [处理器回调](../usage/event/handlers.md#回调语义) · [处理器超时](../usage/event/handlers.md#time_out) · [取消清理](../../src/apixis/core/event/base/handler.py#L232) · [前台通知](../../src/apixis/core/event/event_loop.py#L308) · [后台通知](../../src/apixis/core/event/event_loop.py#L413)
4. **【文档未对齐】E4 回调配置错误可能延后到派发时才报出。** 处理器页称配置错误会在注册时报错并保留旧订阅。这适用于注册时实际验证的条件。实现对通知回调检查 `callable`，调用后才 `await` 返回值。实测返回 `None` 的同步回调能成功替换同名订阅，到派发时才产生 `TypeError`。文档需要写明回调必须返回 awaitable，以及签名和返回值的错误可能在执行时暴露。 [回调配置](../usage/event/handlers.md#通知回调) · [回调校验](../../src/apixis/core/event/base/handler.py#L68) · [回调执行](../../src/apixis/core/event/base/handler.py#L143) · [替换顺序](../../src/apixis/core/event/handler_registry/registry.py#L408)
5. **【文档未对齐】E5 `expect_event()` 的 docstring 示例使用了私有管道。** 示例在等待回包前调用 `pipe.post_event()`。应用入口可直接调用公开的 `post_event()`。等待外部事件的示例可以只展示监听。 [docstring 示例](../../src/apixis/core/event/subscription/waiter.py#L302) · [通道公开边界](../usage/event/channels.md#容量与状态)

## 图执行与状态

1. **【文档未对齐】G1 全局 namespace 指向已撤出公开门面的常量。** 图页把 `GLOBALNS` 列为显式用法。`apixis`、`apixis.core` 和 `apixis.core.graph` 均已撤出该导出，公开接口测试对此有保护。当前参数接受 `using_namespace="<global>"`。`compile_graph()` 和 `NodeGraph` 的 docstring 也引用了这个内部常量。 [图构建参数](../usage/graph/README.md#构建图) · [GraphManager docstring](../../src/apixis/core/graph/graph_manager.py#L102) · [NodeGraph docstring](../../src/apixis/core/graph/node_graph.py#L87) · [namespace 解析](../../src/apixis/core/graph/utils/namespace.py#L66) · [公开接口回归](../../tests/test_public_api.py#L21)
2. **【文档未对齐】G2 手动快照示例缺少活跃 context 前提，`KeepRef` 缺少快照边界。** 文档展示 `create_context()` 后的 `context`，接着示范调用 `take_a_snapshot()`。新建 context 处于 pending，直接调用会抛 `RuntimeError`，终态同样不能保存。手动快照应在活跃执行中创建，读取历史可以在执行后进行。`KeepRef` 保留运行时字段引用，快照仍对整个状态做深拷贝，恢复也是独立副本。实测含不可深拷贝资源时，默认快照会报错，关闭自动快照后该资源可保留原引用。 [GraphContext 和快照](../usage/graph/README.md#快照) · [KeepRef](../usage/graph/state.md#keepref) · [快照实现](../../src/apixis/core/graph/context/graph_context.py#L179) · [恢复实现](../../src/apixis/core/graph/node_graph.py#L288) · [活跃状态回归](../../tests/core/graph/test_graph_context.py#L99) · [快照引用隔离回归](../../tests/core/graph/test_keep_ref.py#L199)
3. **【文档未对齐】G3 并发更新说明缺少冲突和部分写入规则。** 状态页只说明按声明顺序应用更新。不同图节点在同一并发步写入同一个非 `AutoMerge` 字段会抛 `ValueError`，先前已经写入 `context.state` 的更新不会回滚。一个专用节点产出的多条 `Command` 仍可按顺序覆盖该节点自己的字段。 [Command](../usage/graph/state.md#command) · [并发下一跳](../usage/graph/state.md#并发下一跳) · [更新与冲突判断](../../src/apixis/core/graph/node_graph.py#L582) · [同一节点多条命令回归](../../tests/core/graph/test_graph_concurrency.py#L91) · [并发冲突及部分写入回归](../../tests/core/graph/test_graph_concurrency.py#L176)
4. **【文档未对齐】G4 图绑定的是所属 core，关闭时还需区分调用阶段。** 图页称图直接绑定 pipe 和 handler registry。`5220a2c` 的图保存所属 core，使用公共订阅、发布和注销入口，并以 core 状态和身份防止旧图进入新 core。新调用命中关闭状态时会得到 `CoreClosedError`。如果已接纳的调用在发布下一步时被拒，内部发布路径抛出 `CoreClosedError` 并取消 invocation。此时 `invoke()` 或 `stream()` 的调用方收到 `CancelledError`。 [图 API 开头](../usage/graph/README.md) · [所属 core](../../src/apixis/core/graph/node_graph.py#L114) · [准入检查](../../src/apixis/core/graph/node_graph.py#L237) · [发布下一步](../../src/apixis/core/graph/node_graph.py#L713) · [调用方异常回归](../../tests/integration/graph/test_core_close.py#L64)
5. **【文档未对齐】G5 中断缺失异常依据 `event.seen`，普通订阅也会影响结果。** 图页以是否存在可执行的 interruption hook 区分 `BlockHookNotRegisteredError` 与 `BlockNotResolvedError`。实际在事件无错误、未被 accept 且 Block 未完成时，检查的是 `event.seen`。只注册一个正常返回且不操作 Block 的后台普通订阅，也实测收到 `BlockNotResolvedError`。`post_and_wait()` 仍只等待前台处理。文档需要把异常条件与支持通过事件名称注册的普通处理函数对齐。 [中断接口](../usage/graph/README.md#interrupt--block) · [中断结果判断](../../src/apixis/core/graph/interrupter/graph_interrupter.py#L105) · [记录执行 handler](../../src/apixis/core/event/base/handler.py#L125) · [普通订阅接管回归](../../tests/integration/graph/test_default_interrupted_hook.py#L283)

## 配置

1. **【文档未对齐】C1 配置页仍描述跨线程队列和每线程 core。** 当前只有共享 core 的本地有界事件队列。`event_pipe_max_len` 控制该队列，`event_loop_backpressure` 独立限制前台派发许可。配置页第 31、37 行仍描述独立的跨线程接收队列、各线程独立许可和跨线程容量通知。这是既有审计的 D3，在当前提交仍存在。 [队列与并发](../usage/config/README.md#队列与并发) · [共享实例](../../src/apixis/core/event/factory.py#L11) · [本地队列配置](../../src/apixis/core/config/core_config.py#L7) · [既有审计](./2026-11-22-full-code-review.md#可以明确清理的内容)
2. **【疑似缺陷 - 不予修复】C2 日志配置页只给出布尔语义，实际按 Python 真值判断。** YAML 中引号包裹的 `show_event_dispatch: "false"` 被读取为非空 `str` 并实际开启逐事件打印。实现未在配置边界校验类型。这与既有审计 D2 一致。布尔契约需要配置加载器执行校验。 [事件日志输出](../usage/config/README.md#事件日志输出) · [读取开关](../../src/apixis/core/config/core_config.py#L30) · [分发打印](../../src/apixis/core/event/event_loop.py#L213) · [既有审计](./2026-11-22-full-code-review.md#可以明确清理的内容)

## 开发命令与历史记录

1. **【疑似缺陷】R1 测试指南中的跨版本命令在第一个 worker 退出。** 文档取 `9743814` 的 `source/` 作为 baseline。当前驱动只判断旧版是否存在 `Logger`，随后调用旧版没有的 `Logger.set_error()`。原命令与缩小后的单场景均复现 `AttributeError`。用 `4253f73` 的 `src/` 作为 baseline，当前驱动的小规模全场景对照已跑完四个 worker。 [测试指南](../testing/performance/README.md#当前源码的跨版本比较) · [Logger 历史分支](../../scripts/benchmark_runtime.py#L78)

   **已修复。** 指南的示例基线更新为 `4253f73`，提取目录和 `--baseline-source` 同步改为 `src/`，并说明历史版本接口需要与比较脚本匹配。旧基线还缺少当前驱动使用的 `registry.clear()`，本次通过更新示例基线恢复命令可执行性。小规模对照已完成四个 worker，共 25 个配对场景，结果断言通过。

2. **【文档未对齐】R2 既有代码诊断的两条复跑命令缺少基线源码选择。** 报告正文已说明探针面向 `a93d34e`，命令块却直接运行当前安装。当前源码下，两个探针分别因无法从 `apixis` 导入 `ApixEventHandler` 和 `ApixisCore` 退出。将 `a93d34e` 的 `src/` 提取到临时目录并设为 `PYTHONPATH` 后，两条命令均可运行，生命周期矩阵通过 972 组。 [诊断复跑命令](./2026-11-22-full-code-review.md#证据与复跑) · [缺陷探针导入](./data/2026-11-22/reproduce_findings.py#L17) · [矩阵导入](./data/2026-11-22/lifecycle_matrix.py#L5)
3. **【文档未对齐】R3 性能索引把历史 after 称为当前工作区。** 11 月 21 日报告中的 25 项对照属于 `f09f638` 与当时冻结的 after。归档记录的 after 源码 SHA-256 和 `a93d34e` 一致（均排除 `_version.py`），与 `5220a2c` 不同。性能索引首段和复现段若继续使用「当前工作区」，会把当前运行时测得的结果误认为历史对照。重跑原测量配置需要固定归档驱动和对应的源码快照。 [性能索引](../testing/performance/README.md) · [历史复现说明](../testing/performance/2026-11-21-lifecycle-recheck.md#复现) · [归档源码与驱动指纹](../testing/performance/data/2026-11-21-lifecycle-recheck/validation.json#L2)

## 其他接口细节

- 【疑似缺陷 - 不予修复】`on_cancelled` 中直接 `await close_core()` 时，框架能识别受管理的执行任务并避免等待自身。将关闭调用包装为新 Task 再等待时，子任务不在受管理集合里，实测会与父回调互相等待，通常由每个取消回调的 5 秒协作超时解除；吞掉超时取消并持续不返回仍会延长关闭。 [关闭重入说明](../development/runtime-lifecycle.md#close-的取消顺序) · [关闭重入判断](../../src/apixis/core/event/core.py#L233) · [直接调用回归](../../tests/core/event/test_close_core.py#L378)
- 【文档未对齐】`unsubscribe(group="global")` 也会移除默认组中的 waiter 临时监听。Future 仍可保持 pending，以后新出队的事件不会再匹配被移除的监听。已捕获的链保持原状。生命周期页已有内部说明，面向应用的注销章节缺少这一使用边界。 [注销接口](../usage/event/handlers.md#替换查询与注销) · [生命周期说明](../development/runtime-lifecycle.md#eventwaiter-的两类取消) · [分组移除回归](../../tests/core/event/test_waiter_handler_cache.py#L165)
- 【文档未对齐】waiter 必须在目标事件出队并捕获处理链前完成注册。事件因背压处于待派发状态时，晚注册的 processed waiter 也不会加入该次链。发布页的「已经消费过的事件不会重放」可以明确这一时点。 [等待事件](../usage/event/README.md#等待事件) · [出队时捕获处理链](../../src/apixis/core/event/event_loop.py#L171)
- 【文档未对齐】`post_event(event_name="")` 可把事件放入队列，消费逻辑会跳过空名称。`post_and_wait(event_name="")` 在安装监听时抛出 `ValueError`。 [发布并等待](../usage/event/README.md#发布并等待前台处理) · [空名过滤](../../src/apixis/core/event/event_loop.py#L284) · [等待模式验证](../../src/apixis/core/event/subscription/waiter.py#L54)
- 【文档未对齐】处理器页的查询章节未收录公开的 `is_registered()` 和 `get_unmatched_subscriptions()`。后者以已观察的非 `TRANSIENT` 事件名为依据，查询尚无匹配的订阅模式。 [处理器查询说明](../usage/event/handlers.md#替换查询与注销) · [公开接口](../../src/apixis/core/event/__init__.py#L64) · [未匹配查询实现](../../src/apixis/core/event/handler_registry/registry.py#L532)

## 验证结果

以下为初始核对与既有修复的验证结果；本次文档重构的检查见[测试汇总](../testing/README.md#文档重构验证)。

| 检查 | 基线核对结果 |
| --- | --- |
| 当前源码默认测试 | Python 3.12.12，`1625 passed` |
| CI 同参数的小规模性能用例 | 在临时源码副本执行，`13 passed`，用来验证执行和清理 |
| Ruff | `All checks passed` |
| 完整 Python 示例 | 中英文根 README、快速开始两段、生命周期应用入口，共 5 段，独立进程均输出预期结果 |
| 其余 Python 代码块 | 共核对 54 段（含上述 5 段），语法解析及显式 `apixis` 导入通过。含上下文变量或函数内 `return` 的片段按片段核对 |
| 文档引用和性能归档 | 仓库内链接的目标文件均存在。旧审计 D3 的两处 `#L31` / `#L37` 指向 Markdown 行号，渲染后无法按文档标题跳转。5 份 manifest 的 74 个文件大小和 SHA-256 均匹配 |
| 当前缓存审计 | 13 个场景、2 项受控重注册检查，未复现缓存缺陷 |

实际发布的 PyPI 包、外部链接在线状态、Python 3.13/3.14 和历史性能数值未在本轮复测。`single-core-rebuild.md` 中的隐式 getter、旧注册接口和历史 findings 均写明为第一次重建快照，适用该历史基线。 [重建记录](2026-11-20-single-core-rebuild.md) · [CI 配置](../../.github/workflows/test.yml)

## L1 修复复现

最小脚本及运行说明见[测试复现指导](../testing/reproduction.md#l1-回调吞掉关闭取消)。发现时输出包含 `core ran`；已有 L1 修复后不再进入该业务阶段。修复实现、四项回归及默认全量结果见上文 L1。
