# 事件派发与等待设计

事件层将入队、匹配、前台派发和临时等待分开管理。队列限制尚未消费的事件数量，许可限制前台执行量，注册表决定以后出队事件的匹配链，waiter 的 Future 决定一次等待的终态。

## 出队时捕获处理链

消费者先出队，再同步取得 handler 引用及顺序，最后等待前台许可。先获取许可再等待空队列，会让空闲消费者占住其他链恢复所需的许可。

已出队但尚未获得许可的事件保存在 `_pending_dispatch` 中，包含事件与捕获的链。stop 保留它，start 继续派发；close 丢弃它并确认一次 `task_done()`。后续注册、替换和注销只影响以后捕获的链，不修改在途引用。临时监听也必须在这个捕获时点之前注册。

拿到许可后，消费者按发布时捕获的 context 创建前台 Task。该 Task 顺序执行前台 handler，并为后台 handler 创建独立 Task。处理链顺序保证的是后台任务的创建位置，实际开始执行仍服从事件循环的 task factory。框架使用 `asyncio.create_task()`；默认调度与应用设置的 eager factory 可能产生不同的前后台交错。

## 普通注册与匹配缓存

普通 handler 通过 `register_handler()` 进入名称、分组、精确名称及通配符索引，并按优先级和相对插入顺序组织。匹配字段保存为私有元组，注册表保留旧索引信息，使受控替换可以移除旧归属再安装新项。

校验在替换前完成，已验证的配置错误保留原项。通知回调只校验 callable；参数签名和 awaitable 返回约束在实际调用时体现，注册成功并不证明回调能够正确执行。

普通链按精确事件名缓存，使用有容量上限的 LRU。空链也是有效缓存；注册变化只使相关匹配失效，已交给派发的列表保持原样。元数据查询返回独立列表，避免查询结果被修改后反向改变注册内容。

## 等待监听的归属

EventWaiter 构造本次收集的 Future、predicate 与临时 handler，再调用一次 `register_waiter()` 安装整组监听。registry 同时记录 waiter、监听名称、分组和匹配索引；安装中断时移除本次已经安装的条目。

等待监听位于单独的精确/通配符缓存，分别组成 received、processed 两端，不占普通优先级桶。最终派发顺序为 received 监听、普通处理链、processed 监听。临时等待的增删不会使普通链缓存反复重建。监听只收集结果，不调用普通 handler 的业务执行记录逻辑，不修改 `event.seen`、accept 或业务错误栈。

registry 通过 `WaiterLifecycle` 协议持有 waiter，只需要 `active`、`finish()` 和 `close()`，不依赖上层 subscription 模块。匹配与注销归 registry，期限、predicate 和收集结果归 waiter。

Future 到达终态后，waiter 请求 `unregister_waiter()` 删除自己仍拥有的监听，并释放 registry 引用、取消计时器。`wait()` 的 finally 保证注销先于前台许可恢复；不再有人读取时，Future 完成回调仍负责清理。注销中断时保留引用，允许稍后重试。

单独按名称或组移除临时 handler 只改变匹配入口，不完成 Future，也不删除 registry 对该 waiter 的关闭追踪。替换临时 handler 后，旧 waiter 的清理不会删除新注册项。这区分了“停止监听”和“结束等待”。

## 许可借还与子任务

前台链使用一个 `HandlerChainToken` 记录许可归属。`suspend_process()` 在等待期间归还许可，退出时重新获取；发布接口和等待接口已经在对应位置使用它，嵌套调用不再额外占用许可。

子任务可以共享同一 token。多个独立挂起分支由最后恢复的分支重新获取许可，恢复锁避免重复获取。普通子任务取消时，若所属派发仍存活，需要先恢复父链的许可；派发本身正在取消或已经结束时，不能让子任务永远等待重新获取许可。派发完成回调关闭 token，只在实际持有时释放许可，并中断仍在恢复的子任务。

后台 handler 创建前清除 token 上下文。它不拥有前台许可，发布或等待不能归还别的前台链的许可。状态快照中的派发任务数因此不等于正在占用的许可数。

## 取消与完成的职责

前台取消将原事件标为 cancelled，并按原链顺序执行前台取消通知；后台取消只通知对应 handler，不改变共享事件的取消标记。普通回调返回后还会检查 Task 的取消状态，避免吞掉取消后继续执行后续业务阶段。

每个框架取消通知使用独立的 5 秒协作式超时；通知异常只记录，不进入业务 `on_error` 或错误栈。关闭时还有一条直接完成活跃 waiter 的路径，解除取消清理再次等待事件的依赖。这两条路径可以交错，完整终态规则见[生命周期设计](runtime-lifecycle.md#eventwaiter-的两类取消)。

当前重入判断只识别受管理的前台、后台 Task。取消回调直接等待 `close_core()` 可以避开等待自身；另建子 Task 再等待关闭会形成相互等待，通常直到通知超时才被打断。当前不扩展子任务归属追踪，边界记录在[一致性核对](../audits/2026-11-22-doc-code-consistency.md#其他接口细节)。

实现入口：[消费者和任务归属](../../src/apixis/core/event/event_loop.py)、[普通注册与缓存](../../src/apixis/core/event/handler_registry/registry.py)、[等待缓存与协议](../../src/apixis/core/event/handler_registry/waiter_cache.py)、[等待器](../../src/apixis/core/event/subscription/waiter.py)、[许可协调](../../src/apixis/core/event/base/process.py)。

[开发文档](README.md) · [事件使用指南](../usage/event/README.md)
