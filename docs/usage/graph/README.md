# 图 API

构图前调用 `create_core()`，执行前调用 `await start_core()`；graph 不会自动创建或启动 core。应用结束时调用 `await close_core()`。详见[生命周期](../lifecycle.md)。

编译后的图属于创建它的 core。该 core 关闭后，旧图不能迁移到新 core，应用应重新构图。`decompose()` 仍可释放旧图资源。

关闭时，新的 `invoke()` 调用或开始迭代的 `stream()` 会抛出 `CoreClosedError`；已经接纳但尚未完成的调用会被取消，调用方收到 `asyncio.CancelledError`。这也包括还在排队、尚未执行节点的调用，以及安排下一步时遇到关闭的调用。普通 `stop_core()` 只暂停消费，已安排的节点继续执行，后续节点等待显式 `start_core()` 恢复。

Graph API 的主要入口是 `GraphManager`。它负责注册节点与 state 策略，`compile_graph()` 返回可执行的 `NodeGraph`。

图领域异常 `GraphNodeError`、`InvalidNodeReturnsError` 和 `InvalidContextError` 从 `apixis.core.graph` 导出，顶层 `apixis` 也提供同名类型。

## 构建图

```python
from apixis import GraphManager, create_core

create_core()


def step(state: dict) -> dict:
    return {"count": state.get("count", 0) + 1}


graph = (
    GraphManager()
    .add_node(step)
    .compile_graph(entry_point="step")
)
```

### `GraphManager(state_schema=None)`

可选 `state_schema` 用于声明 `AutoMerge` / `KeepRef` 等 state 策略。

### `add_node(node_func, node_name=None, *, timeout=None)`

注册同步/异步函数或 `BaseNode`。普通函数默认使用 `__name__` 作为节点名。

注册时保存图内名称和超时配置，不修改传入的节点对象；编译后的图固定这两项配置。复用同一节点不会改变已有图的配置，执行对象及其资源仍共享。

### `add_nodes(node_list)`

批量注册节点。

### `compile_graph(entry_point, *, max_steps=1024, max_snapshots=None, using_namespace=None, no_snapshot=False, exist_ok=False)`

显式指定入口并编译为 `NodeGraph`：

- `entry_point="first"`：从单个节点开始；
- `entry_point=["a", "b"]`：从并发步开始；
- `entry_point=None` 或 `[]`：不执行节点，直接结束。

入口节点必须已经注册。节点之后的每一步完全由返回的 `Command.goto` 决定。
返回普通 mapping 等价于 `Command(update=mapping)`，应用更新后结束当前分支。
条件判断直接写在节点内，例如：

```python
from apixis import Command


def choose(state):
    return Command(goto="approved" if state["approved"] else None)
```

- `using_namespace=None` 或 `""`：自动生成进程内唯一的 namespace，不承诺跨进程唯一；
- `using_namespace="<global>"`：显式使用全局命名域；
- `max_steps=1024`：非负整数，限制节点执行批次数；完成图不占额外步数，0 允许空图完成；
- `max_snapshots=None`：使用 `APIXIS.GRAPH.max_snapshots`（默认 16）；传入正整数可覆盖；
- `no_snapshot=False`：默认在每批节点执行前保存完整快照；`True` 关闭自动快照，减少大 state 的复制与历史保留开销；
- `exist_ok=False`：namespace 已占用时抛异常；
- `exist_ok=True`：释放旧图并由新图接管。

### `NodeGraph(nodes, entry_point, *, max_steps=1024, max_snapshots=None, state_schema=None, using_namespace=None, no_snapshot=False, exist_ok=False)`

也可直接使用节点名到 `BaseNode` 的映射构建图。`entry_point` 与上面语义一致。
`max_steps` 按节点执行批次计数，并发步也只计一步；完成图不占额外步数。
`no_snapshot=True` 关闭执行前快照。

新建 context 从图的入口开始；恢复 context 从快照记录的目标与步数继续。节点返回 `goto=None` / `[]` 时结束当前分支。

## 执行

### `await graph.invoke(state=None, graph_context=None)`

两种调用方式二选一：

```python
result = await graph.invoke({"value": 1})
```

或：

```python
context = graph.create_context({"value": 1})
result = await graph.invoke(graph_context=context)
```

不能同时传 `state` 和 `graph_context`。

在前台事件 handler 或图节点中等待另一个图的 `invoke()` 时，等待完成期间会挂起当前 handler chain、释放其调度容量，并在继续执行前恢复容量。

每次调用在执行入口绑定一次图上下文。节点、注册在图 dispatch handler 前后的插件，以及由节点发布的中断 hook，均可通过 `get_graph_context()`、`get_current_run_id()` 和 `get_stream_writer()` 访问该次调用的上下文。不同调用相互隔离，嵌套图结束后恢复外层绑定。

`get_current_namespace()` 返回当前调用所属图的 namespace，可用于日志和事件关联；这些查询在没有图调用上下文时抛出 `RuntimeError`。`get_graph_context()` 用于读取上下文或在活跃期保存快照，节点状态更新仍应通过返回值提交。

调用方的 `ContextVar` 绑定随事件传播；子 task 对绑定的修改不会回写父 task，也不会自动传给后续节点。跨节点数据通过 state / `Command` 传递；变量值中的可变对象仍按引用共享。

## Stream

```python
async for chunk in graph.stream({"value": 1}):
    ...
```

节点内部可通过：

```python
from apixis import get_stream_writer

writer = get_stream_writer()
writer.write({"progress": 0.5})
```

向图外实时发送数据。

返回的 `StreamWriter` 是同步可调用对象，`writer(chunk)` 与 `writer.write(chunk)` 等价，无需 `await`。

`stream()` 在开始迭代时继承调用方上下文，图上下文绑定在独立的执行 task 内，消费 chunk 的代码保留自己的上下文。调用进入终态后，writer 不再发送新 chunk，包括终态前已获取的句柄；此前已排队的 chunk 仍可正常消费。

## `GraphContext`

推荐通过图创建，而不是直接实例化：

```python
context = graph.create_context(initial_state)
```

新建或恢复的 context 处于 `pending`，开始执行后为 `running`，结束后为 `finished`、`failed` 或 `aborted`。context 属于创建它的图，不能跨图使用，也不能重复或并发执行同一个已接纳的 context；需要重新执行时，创建或恢复新的 context。

`GraphContextStatus` 是上述状态字符串的类型别名；`GraphContextSnapshot` 是快照字典的类型，包含 `timestamp`、`state`、`target_node_name`、`steps` 和 `graph_id`。两者可直接从 `apixis` 导入用于类型标注。

常用属性：

- `graph_id`
- `status`: `pending | running | failed | aborted | finished`
- `state`
- `target_node_name`
- `steps`
- `is_consumed`
- `is_bound`
- `is_active`

### 快照

只有正在执行且 `is_active=True` 的 context 可以保存快照；新建的 `pending` context 和已经结束的 context 调用 `take_a_snapshot()` 都会抛出 `RuntimeError`。例如，在节点内保存手动 checkpoint：

```python
from apixis import GraphManager, get_graph_context


def step(state):
    get_graph_context().take_a_snapshot()
    return {"count": state["count"] + 1}


graph = GraphManager().add_node(step).compile_graph(
    entry_point="step", no_snapshot=True,
)
context = graph.create_context({"count": 0})
await graph.invoke(graph_context=context)
latest = context.get_snapshot()
all_snapshots = context.get_all_snapshots()
```

快照记录保存时已经提交的 context 状态、目标节点和步数；节点收到的局部 state 在返回 update 前尚未提交。上例保存的是 `step` 更新提交前的状态，恢复后会重新执行 `step`。执行结束后仍可读取已有历史。

恢复由图负责，返回新的 `pending` context；传给 `invoke(graph_context=...)` 后继续执行：

```python
restored = graph.restore_context(context)
restored = graph.restore_context(snapshot)
restored = graph.restore_context(snapshot_list, version=-1)
```

恢复结果属于同一个 `NodeGraph`，不能跨图使用。

保存、读取和恢复快照均会对状态做深拷贝，包括 `KeepRef` 字段。快照不会保留资源对象的原引用；恢复后修改状态也不会改写原快照。

`context.max_snapshots` 是该图的快照条数上限，自动和手动保存均只保留最近记录；恢复历史也按此上限裁剪。`version` 索引当前保留的历史，已淘汰的快照无法从 context 恢复。

通过 `compile_graph(..., no_snapshot=True)` 或 `NodeGraph(..., no_snapshot=True)` 关闭自动快照后，节点输入、命令更新和正常返回结果仍按原来的 state 策略复制。新建 context 不会自动积累历史；没有手动保存的快照时，`restore_context(context)` 会抛异常。

## Abort

```python
await graph.abort(context)
# or
context.abort()
```

abort 使用最近一次 checkpoint 作为返回边界，并对返回的快照状态完整深拷贝，包括 `KeepRef` 字段。已经开始执行的节点可能继续完成自身协程，但其结果不会再提交到已中止 invocation。
关闭自动快照且没有手动保存 checkpoint 时，abort 返回已提交的当前 state；正在执行的节点尚未返回的更新不会提交。

## Interrupt / Block

节点内：

```python
from apixis import interrupt

answer = await interrupt(data={"question": "continue?"})
```

为图注册处理器：

```python
@graph.add_interrupted_hook
async def on_interrupted(block):
    block.resolve("yes")
```

也可以按 namespace 注册：

```python
from apixis import interrupted_hook

@interrupted_hook(graph.namespace)
async def on_interrupted(block):
    block.resolve("yes")
```

`interrupt()` 等待本次中断事件的全部前台处理结束，再检查事件和 Block。中断事件没有错误、未被 `event.accept()`，且 Block 仍未完成时，异常条件如下：

| 条件 | 结果 |
| --- | --- |
| `event.seen` 为空，即没有业务处理器执行 | `BlockHookNotRegisteredError` |
| `event.seen` 非空，但 Block 未完成且未被 `block.accept()` 接管 | `BlockNotResolvedError` |

`event.seen` 包括实际执行过的普通订阅，不只包括上述 hook。应用也可通过 `subscribe(get_graph_interrupted_name(graph.namespace))` 处理 `event.context` 中的 Block；普通后台订阅如果已经执行，也会影响异常判断，但中断接口不等待后台处理结束。负责应答时，使用前台 hook，并在返回前完成或接管 Block。

需要暂存 Block、稍后由外部响应时，在 hook 返回前调用 `block.accept()`。它只声明接管，不会恢复图执行，仍需稍后调用 `resolve()`、`fail()` 或 `cancel()`。hook 自身抛出的异常会传播到等待中断的节点。

`timeout` 使用一个期限，覆盖事件入队、前台 hook 处理和最终 Block 响应；期限到达返回 `None` 并关闭 Block。hook 自己抛出的 `TimeoutError` 仍按原异常传播。

`post_and_wait()` 自行管理事件处理阶段的派发许可。其返回后，`interrupt()` 仅在等待尚未完成的 Block 时再次让出许可，使负责回复的事件 handler 即使在背压为 1 时也能执行。

`Block` 常用接口：

```python
block.resolve(value)  # Return a value to interrupt().
block.fail(error)  # Fail the interrupted operation.
block.cancel()  # Cancel the interrupted operation.
block.accept()  # Take ownership for a later response.
```

## 生命周期

编译后的图持有 namespace 和 event handler 注册。使用结束后调用：

```python
graph.decompose()
```

`decompose(force=True)` 会中止 `running` context 并释放 handler / namespace；`force=False` 仅在仍有 `running` context 时抛异常。`pending` context 保持原状态，但之后通过已分解的图调用 `invoke()` / `stream()` 会抛异常。

图自身的处理器和 `graph.add_interrupted_hook()` 注册的 hook 统一归入 `graph.handler_group`，随图分解一起注销；每个图实例使用独立的组。

需要随图一起清理的自定义事件处理器，可显式加入此组：

```python
from apixis import subscribe

@subscribe(graph.dispatch_name, group=graph.handler_group)
async def observe_graph(event):
    ...
```

普通 `subscribe()` 和独立 `interrupted_hook(namespace)` 注册的 handler 默认仍使用 `"global"` 组，由调用方管理其生命周期。移除图的处理器组会影响后续事件分发；正常释放图时使用 `graph.decompose()`，一并处理运行中的 context 和 namespace。

### 使用上下文管理器自动管理生命周期

```python
with graph:
    await graph.invoke({...})
```

上下文管理器退出时自动分解图。

[状态与 Command](./state.md) · [运行时设计](../../development/README.md) · [文档首页](../../README.md)
