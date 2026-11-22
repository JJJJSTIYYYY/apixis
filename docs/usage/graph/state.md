# 图状态与 Command

## 节点返回值

普通 `Node` 函数必须返回：

- `dict` / mapping：作为 state update；或
- `Command`：同时描述 update 与下一跳。

```python
from apixis import Command


def step(state):
    return Command(
        update={"count": state["count"] + 1},
        goto="next",
    )
```

普通 `Node` 不接受 `list[Command]`。`ParallelNode` 等专用 `BaseNode` 可返回多个 command，由图按声明顺序应用。

公开类型别名 `NodeResult` 为 `dict[str, Any] | Command`，`NodeFunction` 描述接收 state 字典并同步或异步返回 `NodeResult` 的函数。运行时也接受普通 mapping 返回值，并将其转换为字典更新。

## `Command`

```text
Command(
    update: dict = {},
    goto: str | list[str] | None = None,
)
```

`goto` 是节点执行后唯一的下一跳来源：

| 值 | 行为 |
| --- | --- |
| `None` / `[]` | 当前分支结束，不贡献下一跳 |
| `"node"` | 执行单个节点 |
| `["a", "b"]` | 按列表顺序声明一个并发步 |

单元素列表 `["a"]` 仍保留并发步形式。结束分支使用 `None` 或 `[]`。

一个执行步中的全部节点完成后，图按节点声明顺序、各节点的 command 顺序应用更新，
再收集下一跳并按首次出现顺序去重。多个分支指向同一节点时，下一步只执行它一次。
终止分支不会取消其他分支选定的后续节点；所有 command 都没有下一跳时整张图结束。
专用节点返回空 `list[Command]` 时，不更新 state，也不贡献下一跳。

同一并发步中，不同节点更新同一个未标记 `AutoMerge` 的字段会抛出 `ValueError`。同一个专用节点返回的多条 command 可以按顺序覆盖自己的同一字段。

应用更新时发生冲突或其他错误，先前已经写入 `context.state` 的更新会保留，不会自动回滚。需要从批次开始前重试时，使用该批次的快照恢复一个新 context；这要求启用自动快照或事先保存手动 checkpoint。

目标节点必须已注册；未知节点抛出 `ValueError`。

## `AutoMerge`

在 schema 中使用 `Annotated`：

```python
from typing import Annotated, TypedDict
from apixis import AutoMerge

class State(TypedDict):
    messages: Annotated[list[str], AutoMerge()]
```

当字段已有值时，更新通过当前值的 `__add__` 合并；字段不存在时直接写入。

## `Reset`

用于单次绕过 `AutoMerge`：

```python
from apixis import Reset

return {"messages": Reset([])}
```

最终 state 中保存的是 `Reset.value`，而不是 wrapper 本身。

## `KeepRef`

```python
from typing import Annotated
from apixis import KeepRef

resource: Annotated[object, KeepRef()]
```

被标记字段在节点执行前的 state copy 中保留原对象引用，其余字段正常复制。

`KeepRef` 不表示整个 invocation 生命周期永久使用同一个 state 对象；它只改变图执行边界上的字段复制策略。并行执行时共享可变引用可能产生竞争，应由调用方自行同步。

快照独立执行完整深拷贝，`KeepRef` 不会使快照保留原引用；恢复快照也会生成独立副本。若字段包含无法深拷贝的连接、锁等资源，应同时设置 `no_snapshot=True`，并避免手动保存包含该资源的快照。此时仍可通过 `KeepRef` 在普通执行中传递原资源，但没有 checkpoint 时无法从历史恢复。

## `ParallelNode`

```python
from apixis import ParallelNode

parallel = ParallelNode([branch_a, branch_b], name="parallel")
manager.add_node(parallel)
```

每个 branch 接收同一个节点局部 state snapshot，branch 并发执行，结果按 branch 声明顺序合并，而不是按完成顺序合并。

## 并发下一跳

任意节点可返回：

```python
Command(goto=["a", "b"])
```

这表示下一批次并发执行 `a`、`b`。各节点收到独立的 state 副本，`KeepRef` 字段除外。结果按列表顺序应用；多个节点需要累计同一字段时，使用 `AutoMerge`，否则同字段更新会产生上述冲突。

[图 API](./README.md) · [文档首页](../../README.md)
