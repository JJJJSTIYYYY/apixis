# 全量代码诊断报告

初始检查完成于 2026-11-22，基线为 `a93d34e`（3.0.0），当时仅新增报告和诊断材料。后续修复状态在对应问题中注明；原始验证数据保留为基线快照。

## 结论

当前架构的主要职责已经分开：工厂管理共享实例，`ApixisCore` 协调生命周期，event loop 消费和派发，registry 管理路由与等待器登记，waiter 管理自己的 Future，graph 管理 invocation 的完成。**没有发现 event core 为 graph 增加类型识别、补充事件、专用 listener 或反向导入的兼容路径。**

初始检查确认 4 项 P2 运行时问题，其中 **F2–F5 均已修复**；3 项 P3 清理问题中 **D1 已修复，D2、D3 待处理**。风险集中在异常发生后的资源释放，以及可变对象与索引/句柄之间的一致性，不宜靠继续增加标记位解决。

| 编号 | 优先级 | 确认的问题 | 可见后果 |
| --- | --- | --- | --- |
| F2 | P2，已修复 | 单阶段生命周期流程可重入；应用保证日志器正常工作 | 流程中断后可再次调用，完成清理 |
| F3 | P2，已修复 | handler 所有配置成员私有，收紧公开导出 | 只通过订阅入口修改配置、通过独立快照查询 |
| F4 | P2，已修复 | waiter 的取消分支没有执行固定期限检查 | 相同的零超时等待在默认/eager 调度下结果不同 |
| F5 | P2，已修复 | 已获取的 stream writer 没有发送时的终态约束 | abort 后仍输出；稍晚发送则抛关闭异常 |
| D1 | P3，已修复 | 基准脚本连续创建相同 core 两次 | 无意义的创建、关闭、重建 |
| D2 | P3 | 日志开关没有类型校验 | YAML 字符串 `"false"` 实际启用逐事件输出 |
| D3 | P3 | 当前配置文档残留多线程契约 | 用户可能按已删除的队列/线程模型设计应用 |

P2 表示功能、资源收尾或公开语义不一致；P3 表示低风险冗余、边界或文档问题。以下均基于当前源码和实际复现，不将维护性候选计入缺陷数量。问题编号保留原编号，便于对照讨论。

## 范围与验证

逐文件检查了 `src/apixis` 全部 **46 个 Python 模块**：事件系统 19 个、graph 16 个、配置和工具 8 个、根包/门面与版本 3 个。同时检查了 3 个 scripts、测试装配与关键单元/集成测试、CI、打包配置及当前文档。测试目录执行了完整默认收集；没有把历史报告当成当前验证结果。

| 验证 | 本轮结果 | 说明 |
| --- | --- | --- |
| 完整测试，开启 asyncio debug 和分支覆盖采集 | **1,552 passed，137.59 秒** | 未收集需显式开启的性能负载 |
| eager 生命周期/关闭/中断专项 | **182 passed，0.81 秒** | 使用仓库已有 eager plugin |
| 生命周期交错矩阵 | **972 组通过** | 默认/eager、四次 start/stop/close 的全部组合、三档让出间隔、取消调用者与否；操作入口插入调度点并检查 start/stop 不重叠 |
| 路由缓存审计 | **13 个场景与 2 项既有注册检查通过** | 包含 LRU 淘汰、32,768 个真实事件、重启、请求响应和图调用；未涵盖 F3 的改名序列 |
| 覆盖率 | **语句 97.11%，分支 93.21%，综合 96.15%** | 2,563 条语句、840 条分支；覆盖率不证明任务依赖和时序正确 |
| 静态与接口检查 | 基线 Ruff 通过；当时 84 个顶层导出均存在 | 当前 Markdown 本地文件链接无失效；事件层无 graph 导入 |

上表为初始检查的基线结果。后续修复验证分别记录在 F2、F3 中。完整测试的 18 条 warning 全部来自初始检查启用 JUnit XML 后，`record_property` 与默认 xunit2 格式的冲突，不是运行时资源或协程警告。原始日志见 [pytest-output.txt](./data/2026-11-22/pytest-output.txt)、[eager-output.txt](./data/2026-11-22/eager-output.txt)。本轮使用本机 Python 3.12.12，没有重新验证 CI 的 3.13/3.14、安装包或性能回退。

全部缺陷探针通过公开入口触发，读取私有属性仅用于观察残留状态；没有修改生产方法。探针退出码 0 代表诊断执行完成，**不代表实现健康**。

## 已确认运行时问题

### F2：已修复——单阶段生命周期流程可重入

位置：[registry.py:110](../../src/apixis/core/event/handler_registry/registry.py#L110)、[registry.py:509](../../src/apixis/core/event/handler_registry/registry.py#L509)、[registry.py:521](../../src/apixis/core/event/handler_registry/registry.py#L521)、[waiter.py:223](../../src/apixis/core/event/subscription/waiter.py#L223)、[node_graph.py:253](../../src/apixis/core/graph/node_graph.py#L253)。

初始问题：应用通过 `Logger.set_debug()` 设置的日志打印函数抛出异常时，会中断正在执行的注销操作。在基线代码中，部分对象提前清除了后续清理所需的记录，导致再次调用无法完成清理。

基线快照中的两条复现记录：

- graph 先设置 `_decomposed=True`，再批量注销，最后才释放 namespace。第一次异常后恢复 logger，再次 `decompose()` 直接返回；实际仍占用 namespace，组内还有 1 个 handler。
- waiter 先将 `_registry=None`，再整体注销。注销中断后再次 `close()` 不再进入 registry；随后 core 成功 CLOSED，但 `_waiter_cache.waiters` 仍保留 1 个 inactive waiter owner。`clear()` 有意保留 Future 所有权，而 `cancel_waiters()` 跳过已终态 Future，因此没有后续清理者。

采用的修复方案与职责划分：

- `_start`、`_stop`、`_close` 单阶段流程可重入。失败或取消后不回滚，保留已有进度；再次调用创建新的操作 Task，重新执行流程。
- 正在执行的同阶段调用仍共享 Task。等待其他生命周期 Task、消费者及业务清理时隔离取消，保留未完成工作的引用。
- waiter 完成注销后才释放 registry 引用，关闭重试也会清理 Future 已结束但注销未完成的 waiter。
- 应用负责保证设置的日志器没有抛异常的 bug；异常仍正常传播，框架不增加日志异常兜底。原先对 graph 分解在故障日志器下仍须完整执行的要求不再作为修复范围，也不据此要求额外改造 graph。

新增 [36 项中断重试测试](../../tests/core/event/test_lifecycle_reentry.py)，涵盖默认/eager、操作尚未开始、组件步骤已完成、等待其他生命周期操作、消费者退出和 handler 清理等中断位置。后续验证：完整测试 **1,588 passed**，eager 专项 **218 passed**，生命周期矩阵 **972 组通过**。按上述修复方案和日志器职责边界，F2 标记为已修复。

### F3：已修复——handler 成员私有并收紧公开导出

位置：[handler.py:30](../../src/apixis/core/event/base/handler.py#L30)、[subscriber.py:252](../../src/apixis/core/event/subscription/subscriber.py#L252)、[registry.py:424](../../src/apixis/core/event/handler_registry/registry.py#L424)、[registry.py:488](../../src/apixis/core/event/handler_registry/registry.py#L488)。

基线版本的最小公开操作序列：

```python
h = ApixEventHandler(noop, name="original").register("alpha")
core.handler_registry.get_handlers_for_event("alpha")
h.name = "renamed"
h.register("beta")
h.unregister()
unsubscribe("original")
core.handler_registry.get_handlers_for_event("alpha")  # KeyError: 'original'
```

重注册按新名称查找旧记录，随后把新元数据写回仍被旧键引用的对象。两键一度指向同一实例；注销时又按对象当前的 `beta` 匹配字段失效缓存，遗漏旧 `alpha` 缓存。最终 registry 已空，缓存仍含 `original`。消费者遇到这种缓存会记录匹配解析失败，并跳过该事件。

采用的修复方案：

- handler 的名称、ID、分组、匹配模式、排序、回调和执行选项全部改为内部私有成员；使用 slots，原来的 name 等属性不能再赋值。
- core 的组件成员改为私有；保留 get_core() 的状态、生命周期和关闭回调接口，组件构造入口不再公开。
- 取消 handler、core 实现、event loop、pipe、registry 类及组件 getter、get_handler 的公开导出；namespace 管理和图内部复制、解析、校验工具也不再从应用门面导出。顶层导出从 84 项收敛为 62 项，不增加旧名称兼容别名。
- 应用统一通过 subscribe() 配置 name、group、排序、执行选项以及四类通知回调，通过 get_handler_meta() 查询独立快照，通过 unsubscribe() 注销。图插件需要的事件名称查询仍保留。
- 内部实现与组件测试改从所属模块导入，应用示例和安装包冒烟脚本只使用公开入口。

新增 [29 项公开接口回归](../../tests/test_public_api.py)，验证旧可写成员被拒绝、各门面不再暴露内部实现、元数据修改不影响路由，以及订阅配置和四类通知回调仍可正常使用。完整测试 **1,617 passed**，eager 专项 **247 passed**，13 个缓存审计场景及 2 项既有注册检查通过。wheel 与源码包离线构建、严格元数据检查，以及隔离环境中的 wheel 冒烟检查均通过。基准驱动在基线/当前源码上各跑通 7 个小规模场景，用于确认工具适配，不据此作性能结论。F3 标记为已修复。

### F4：真实事件取消绕过固定等待期限（已修复）

位置：[waiter.py:152](../../src/apixis/core/event/subscription/waiter.py#L152)、[waiter.py:177](../../src/apixis/core/event/subscription/waiter.py#L177)、[waiter.py:278](../../src/apixis/core/event/subscription/waiter.py#L278)。

在已运行的 core 注册立即抛出 `CancelledError` 的普通 handler，再创建 `point="processed", time_out=0` 的 waiter 并发布事件：默认调度返回 `TimeoutError`，eager 调度返回 `None`。`_resolve()` 核对实际期限；`_cancel()` 只检查 Future/predicate，能在过期定时器获得调度之前完成 Future 并删除定时器。

公开契约是固定注册期限、非正数立即过期，因此结果不应取决于定时器是否已经执行。建议在真实事件取消入口应用同一期限规则，保留 `_cancel()`、predicate/event_id 关联与 `None` 结果。**这不要求改变 core 关闭直接 `finish()` 的契约**：关闭无需补充通知、无需调用 predicate，也无需借真实事件解析结束等待器。

修复：`_cancel()` 在 predicate 执行前检查固定期限，过期走已有 `_expire()`；保留 predicate/event_id 关联及部分收集规则，core 直接 `finish()` 仍返回 `None`。新增回归覆盖非正超时且定时器尚未执行、真实 handler 取消和 core 直接结束。

### F5：writer 的终态检查发生在获取句柄时（已修复）

位置：[manager.py:73](../../src/apixis/core/graph/context/manager.py#L73)、[stream_writer.py:18](../../src/apixis/core/graph/context/stream_writer.py#L18)、[stream_writer.py:36](../../src/apixis/core/graph/context/stream_writer.py#L36)、[node_graph.py:403](../../src/apixis/core/graph/node_graph.py#L403)。

节点缓存 `writer = get_stream_writer()`，输出 `before` 后等待。调用方读到首条数据，执行 `await graph.abort(context)`，再允许节点输出。若 stream 执行任务的完成回调尚未封闭 channel，消费者还能读到 `after-abort`；若稍后输出，则得到 `Cannot write to a closed graph stream.`。重新获取 writer 才会得到 no-op。

这让相同终态下的发送行为依赖句柄获取时间及回调调度，和当前文档的终态不再发送语义不一致。已有排队 chunk 在终态后被读出是正常行为；本复现发送的是 **abort 之后新产生的 chunk**。

建议在 graph 持有的发送句柄边界判断 invocation 是否活跃，避免只在 getter 判断。通用 `StreamChannel` 仍保留关闭后写入报错的独立契约，不应反向认识 `GraphContext`。

修复：`GraphContext._bind()` 为调用绑定带状态检查的 writer，每次发送仅在调用活跃时转发；已获取句柄在 abort、失败或完成后也会静默丢弃新 chunk。`StreamChannel` 关闭后写入仍报错，不依赖图上下文。

新增回归覆盖缓存 writer 在 abort 后、通道关闭后、正常完成后和失败后的发送。修复后完整测试 **1,625 passed（93.18 秒）**，eager 专项 **151 passed**；Ruff 与差异空白检查通过。原始基线快照保持不变。

## 冗余、残留与维护性

### 可以明确清理的内容

| 编号 | 位置 | 证据与处理方向 |
| --- | --- | --- |
| D1 | [benchmark_runtime.py:579](../../scripts/benchmark_runtime.py#L579) | 连续两次 `core = await fresh_core(128)`；第二次立即关闭刚创建的实例，已删除一次重复创建。发生在计时区间外，不能据此否定已有性能数据。 |
| D2 | [core_config.py:30](../../src/apixis/core/config/core_config.py#L30) | 临时 YAML 的 `show_event_dispatch: "false"` 导入后得到 `str` 且 truthy。应在配置加载边界要求 bool，不在派发热路径加验证。 |
| D3 | [配置文档:31](../core/config/README.md#L31)、[配置文档:37](../core/config/README.md#L37) | 仍描述跨线程接收队列、每线程独立 core、容量通知；当前实现已删除。只更新现行说明，历史迁移文档不必抹除。 |

### 维护性候选，不计作行为缺陷

- [BaseNode.__init__:23](../../src/apixis/core/graph/node.py#L23) 是接受任意参数但仅 `pass` 的“compatibility hook”，仓库没有调用这个基类构造器。应核实外部扩展契约后考虑删除，避免无意义吞参数；不能仅凭内部无引用认定所有公共 API 都是死代码。
- [NodeGraph._is_active_block:218](../../src/apixis/core/graph/node_graph.py#L218) 遍历已由该图维护的活跃 `_contexts`，又逐项重复检查图归属、集合包含和活跃状态。可根据接纳/终态不变量减少判断，无需额外缓存或标记。
- [namespace.py:83](../../src/apixis/core/graph/utils/namespace.py#L83)、[namespace.py:113](../../src/apixis/core/graph/utils/namespace.py#L113) 有相同的 namespace 解析与存在性验证，可提取一个确有复用的私有函数。
- [registry.py:26](../../src/apixis/core/event/handler_registry/registry.py#L26)、[registry.py:41](../../src/apixis/core/event/handler_registry/registry.py#L41) 仍允许缓存值为 `None` 的注释/类型，而内部实际存列表或删除键。可收紧表述；查询中 `get()` 返回 None 表示缺失的分支仍需保留。

未确认可整体删除的生产运行时子系统或大块死逻辑。最值得优先处理的是跨组件收尾中的半完成状态，而不是为减少行数合并不同职责。

### 已核实必要，应保留的复杂流程

| 流程 | 必要原因 |
| --- | --- |
| start/stop/close 共享 Task、shield、八状态枚举 | 协调并发操作、允许停止后重启、隔离调用者取消；STARTED 是明确要求的调度确认状态。 |
| 操作入口 `sleep(0)` | eager 执行需要先发布操作 Task 句柄，避免日志/消费者重入时拿到未赋值或旧句柄。 |
| close 中两次 `cancel_in_flight()` | 停止前后的所有权移交不同；standalone eager 调度可能先开始关闭，再登记当前 dispatch。 |
| 汇合后的 `sleep(0)` | 已完成 Task 的所有权删除回调尚未运行，直接循环可能反复同步等待同一组 done Task。 |
| `_pending_dispatch` | 已出队但还没拿到派发许可；暂停需保留捕获的 handler 引用，关闭需单独确认一次 task_done。 |
| waiter 的计算型 active、`_registry=None` 注销保护 | Future 是唯一终态；成功注销后释放引用，避免重复注销；失败时保留引用供重试。 |
| handler 汇合前结束 waiter，最终 registry 再关闭 | 前者解除清理代码等待事件的依赖，后者负责组件终态收尾。 |
| suspend_process 的 owner/child 与恢复协调 | 子任务取消时需要恢复仍存活父链的许可；父链结束时又不能等待子任务重新拿许可。 |
| graph 捕获所属 core 的 pipe/registry，使用 close 完成回调 | 防止旧图清理新 core；上层自行结束 completion，保持依赖方向。 |
| graph interrupt 的 post_and_wait 与独立 Block 等待 | 前者等待 hook 处理并借出许可，后者等待被业务接管的 Block；职责不同。 |
| 状态复制与 checkpoint 复制 | 当前执行隔离和历史恢复隔离是不同需求；KeepRef 已表达显式共享，不宜一并去掉深拷贝。 |

## 鲁棒性边界与验证机制

当前普通路径的暂停/恢复、满队列发布、capacity 1/2/3、嵌套等待、取消/超时后的许可恢复均有实际测试；图排队关闭测试已移除 xfail，并用非取消式观察确认调用先结束，再断言 CancelledError，没有重用此前的超时取消假阳性。

仍需明确以下边界，不把它们混报成新缺陷：

- 原生 asyncio 取消是协作式的。无限吞取消、永久不返回的业务协程会阻止汇合；`asyncio.timeout()` 也不能强杀它。关闭是否承诺硬期限应单独决定，当前不能宣称存在强制终止保证。
- stop 只暂停消费。已经安排的链若继续等待后续队列事件，仍依赖显式 start；框架没有承诺暂停期间业务请求能继续完成。
- 原始 pipe `get()` 不是 close-aware 接收协议；core 负责取消自己的消费者。外部直接调用原始队列等待的应用需自行管理其任务。
- StreamChannel 是同步写入的无界队列，慢消费者下没有内存上限；后台 handler 没有单独并发上限。这些是现有资源契约，不能未经设计偷偷增加缓存、丢弃或阻塞语义。
- namespace 注册表和 handler/callback 对 graph 持有强引用，不能依赖 `__del__` 正常释放；仍需显式 decompose。应用须保证配置的日志器正常执行。

验证机制还有两项改进空间：CI 测试步骤未设明确的短周期进程截止时间，fixture 的 close 也可能卡住；`faulthandler_timeout` 只打印堆栈，不会结束进程。建议加进程级 watchdog 并保存超时堆栈。注册基准每轮仅测 5 个单操作，容易放大计时噪声；最新性能报告已用独立重复 probe 纠正，不应再把该小样本当稳定回退依据。

## 建议顺序

F4、F5 和 D1 已完成。后续仅剩 D2 配置边界校验与 D3 配置文档清理。

不建议重写整个 core、再增加一组生命周期布尔标记，或为了结束 graph 而向 event core 填入 graph 专用兼容逻辑。

## 证据与复跑

初始基线源码指纹（沿用仓库缓存审计的算法，排除 `_version.py`）：`15e276d137e09fbe593ad85893980b4ba814f35ace44fdbbcd7513650a51363c`。完整命令、46 个模块清单和基线验证结果见 [validation.json](./data/2026-11-22/validation.json)；基线复现结果见 [findings.json](./data/2026-11-22/findings.json)，覆盖明细见 [coverage-summary.json](./data/2026-11-22/coverage-summary.json)。这些快照不代表后续修复后的运行结果。原始探针使用了当时公开的内部接口，应配合基线源码运行；当前公开边界验证使用 tests/test_public_api.py。

```bash
.venv/bin/python docs/audits/data/2026-11-22/reproduce_findings.py
.venv/bin/python docs/audits/data/2026-11-22/lifecycle_matrix.py
.venv/bin/python scripts/audit_handler_chain_cache.py --output /tmp/apixis-cache-audit.json
.venv/bin/python -m pytest -q --asyncio-debug --maxfail=1 -o faulthandler_timeout=45
.venv/bin/python -m ruff check .
```

完整测试与生命周期矩阵通过，只证明已覆盖的路径；F2–F5、D1 按上述方案完成修复，D2、D3 仍待处理。
