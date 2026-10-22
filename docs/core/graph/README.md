# 图 API

Graph API 的主要入口是 `GraphManager`。它负责注册节点与 state 策略，`compile_graph()` 返回可执行的 `NodeGraph`。

## 构建图

```python
from apixis import GraphManager


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

### `add_nodes(node_list)`

批量注册节点。

### `compile_graph(entry_point, *, using_namespace=None, no_snapshot=False, exist_ok=False)`

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
- `GLOBALNS`：显式使用全局命名域；
- `no_snapshot=False`：默认在每批节点执行前保存完整快照；`True` 关闭自动快照，减少大 state 的复制与历史保留开销；
- `exist_ok=False`：namespace 已占用时抛异常；
- `exist_ok=True`：释放旧图并由新图接管。

### `NodeGraph(nodes, entry_point, *, max_steps=1024, state_schema=None, using_namespace=None, no_snapshot=False, exist_ok=False)`

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

## `GraphContext`

推荐通过图创建，而不是直接实例化：

```python
context = graph.create_context(initial_state)
```

新建或恢复的 context 处于 `pending`，开始执行后为 `running`，结束后为 `finished`、`failed` 或 `aborted`。context 属于创建它的图，不能跨图使用，也不能重复或并发执行同一个已接纳的 context；需要重新执行时，创建或恢复新的 context。

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

```python
context.take_a_snapshot()
latest = context.get_snapshot()
all_snapshots = context.get_all_snapshots()
```

恢复由图负责：

```python
restored = graph.restore_context(context)
restored = graph.restore_context(snapshot)
restored = graph.restore_context(snapshot_list, version=-1)
```

恢复结果属于同一个 `NodeGraph`，不能跨图使用。

通过 `compile_graph(..., no_snapshot=True)` 或 `NodeGraph(..., no_snapshot=True)` 关闭自动快照后，节点输入、命令更新和正常返回结果仍按原来的 state 策略复制。新建 context 不会自动积累历史；没有手动保存的快照时，`restore_context(context)` 会抛异常。

## Abort

```python
await graph.abort(context)
# or
context.abort()
```

abort 使用最近一次 checkpoint 作为返回边界。已经开始执行的节点可能继续完成自身协程，但其结果不会再提交到已中止 invocation。
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

如果图发送了 `Block` 但没有可执行的 interruption hook，调用会因 `BlockHookNotRegisteredError` 失败。
如果 hook 正常返回后，`Block` 仍未完成且没有调用 `block.accept()`，调用会因 `BlockNotResolvedError` 失败。需要暂存 Block、稍后由外部响应时，必须在 hook 返回前调用 `block.accept()`；它只声明接管，不会恢复图执行，仍需稍后调用 `resolve()`、`fail()` 或 `cancel()`。hook 自身抛出的异常会传播到等待中断的节点。

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

### 使用上下文管理器自动管理生命周期

```python
with graph:
    await graph.invoke({...})
```

上下文管理器退出时自动分解图。

[状态与 Command](./state.md) · [文档首页](../../README.md)
