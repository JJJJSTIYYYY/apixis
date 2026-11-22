# 图执行与状态设计

图运行时把下一步执行表示为事件，把一次调用的完成状态保存在 GraphContext 中。节点、context 和事件系统各自拥有不同的生命周期，图层负责协调这些边界。

## 所属 core 与图资源

编译时 NodeGraph 捕获节点绑定、解析状态 schema、申请 namespace、注册分发 handler，并保存所属 core。每张图使用包含 graph ID 的分组，避免同一 namespace 被复用时混淆旧图和新图的订阅。

图通过公共订阅、发布、注销接口操作事件系统，通过保存的 core 查询状态和注册关闭完成回调。发布下一步前核对 core 身份；分解旧图时只在当前 core 仍是所属 core 的情况下注销，避免触及新 core 的同名注册。

namespace 管理拥有图的强引用，handler 和关闭回调也持有图引用，因此释放资源依赖显式 `decompose()`。`exist_ok=True` 替换 namespace 时先同步退役旧图；若新图后续注册失败，释放新资源，不恢复已退役的图。

## 执行完成独立于事件消费

`create_context()` 产生 pending context，开始调用时绑定新的 run ID、completion Future 和流写入器，进入 running。图只持有活跃 context，终态转换立即将其移出活跃集合。

一次事件负责一个节点或并发批次。批次结束后按结果应用 Command，推进步数并发布下一次事件；到达结束目标时完成 context。并发节点的执行可以交错，结果应用按声明的节点顺序进行。

close 可能丢弃已经排队、尚未派发的图事件，这些事件不会再执行图的业务回调。因此图同时监听所属 core 的关闭 Task 完成，取消仍活跃的 context。event core 只提供通用完成通知，不导入图类型或直接修改图 completion。

所属 core 进入 CLOSING/CLOSED 后，新调用由状态检查抛 `CoreClosedError`；已接纳调用的 completion 被取消时，invoke/stream 调用方收到 `CancelledError`。内部发布下一步失败时也先取消本次 context，再将 `CoreClosedError` 沿事件处理路径传播。

## 状态隔离与快照

状态 schema 在编译时解析一次。输入状态、节点视图、Command 更新和正常完成的调用结果在各自的执行边界使用相同复制策略：普通字段深拷贝，`KeepRef` 字段保留对象引用。标记显式表达业务希望共享的资源，不需要为全部状态增加全局缓存。

`AutoMerge` 字段按已有值的 `__add__` 合并，`Reset` 跳过合并直接替换。一个并发批次内，不同节点写同一非 AutoMerge 字段会失败；同一节点返回的多条 Command 可按顺序覆盖它自己的字段。

Command 直接应用到活跃状态，后续更新失败不回滚已经写入的值。恢复边界是执行前的 checkpoint，避免每次应用更新都再复制整个状态。自动快照可关闭；关闭后就没有相应的自动恢复记录。

手动和自动快照都要求 context 活跃，对完整状态深拷贝并保留有限条历史。读取和恢复快照也返回独立副本，不采用 KeepRef 的运行时共享策略。中止已有 checkpoint 的调用时，返回的 checkpoint 状态同样完整深拷贝；没有 checkpoint 的中止结果沿用运行时复制策略。不可深拷贝的资源要使用 KeepRef 并关闭自动快照，同时避免手动快照。

## 中断与外部输入

图通过 `post_and_wait()` 发布 interruption 事件并等待前台处理，借出当前派发许可。Block 单独表示业务输入是否已经提交；前台事件处理完成和 Block 得到结果并非同一个条件。

业务可以通过 interruption hook 或普通订阅接管 Block。事件未报错、未被 accept 且 Block 尚未完成时，interrupter 依据 `event.seen` 区分“没有执行过处理器”和“有处理器但没有完成 Block”。普通后台订阅一旦执行也会记录 seen，不过前台等待不负责汇合它；不能用该异常分类推断是否注册了专用 hook。

流写入器绑定同一次活跃 context。context 终态后，旧写入器和后来读取的写入器都不会继续输出，避免节点的迟到结果污染已结束的调用。

实现入口：[图执行与归属](../../src/apixis/core/graph/node_graph.py)、[context 和快照](../../src/apixis/core/graph/context/graph_context.py)、[namespace](../../src/apixis/core/graph/utils/namespace.py)、[中断](../../src/apixis/core/graph/interrupter/graph_interrupter.py)。

[开发文档](README.md) · [图使用指南](../usage/graph/README.md)
