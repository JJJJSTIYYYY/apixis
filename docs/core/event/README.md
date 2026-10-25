# 事件 API

事件 API 负责创建、发布、路由和处理 `ApixEvent`。常用接口可直接从 `apixis` 导入。

## 共享事件系统

### `await start_core()`

唤起共享 event core。已运行时重复调用不会重复启动；返回不表示后台消费者已开始处理事件，也不表示已发布事件处理完成。启动连接时发生的异常会传播给调用方。

共享 core 只构建一次，后续调用复用同一组 registry、pipe、handler registry 和 event loop。

```python
from apixis import start_core

await start_core()
```

### 同步 getters

- `get_event_registry()` → `ApixEventRegistry`
- `get_event_pipe()` → `ApixEventPipe`
- `get_handler_registry()` → `ApixHandlerRegistry`
- `get_event_loop()` → `ApixEventLoop`

同步 getter 会立即返回共享对象；处于运行中的 asyncio loop 时会触发启动，但**不保证启动完成**。

### 异步 getters

- `await aget_event_registry()`
- `await aget_event_pipe()`
- `await aget_handler_registry()`
- `await aget_event_loop()`

异步 getter 以 `await` 方式唤起并返回同一组共享组件，不等待已发布事件处理完成。

## `EventType`

```python
from apixis import EventType
```

可选值：

- `EventType.TRANSIENT`
- `EventType.WORKFLOW`
- `EventType.LIFECYCLE`
- `EventType.INFO`
- `EventType.WARNING`
- `EventType.ERROR`

`TRANSIENT`（值为 `"transient"`）表示临时事件：事件正常分发给订阅的处理器，但其名称不会被 `ApixEventRegistry` 记录。

## `ApixEvent`

```text
ApixEvent(
    event_id: str,
    event_type: EventType,
    event_name: str,
    context: Any,
    timestamp: float,
    accepted: bool,
    seen: list[str],
    error_stack: list[ApixEventError]
)
```

`accepted` 默认为 `False`，`seen` 和 `error_stack` 各自默认为独立的空列表。

主要字段：

| 字段 | 说明 |
| --- | --- |
| `event_id` | 事件 ID |
| `event_type` | `EventType` |
| `event_name` | 用于 handler 匹配的事件名 |
| `context` | 用户上下文 |
| `timestamp` | Unix timestamp |
| `accepted` | 是否已被 accept |
| `seen` | 已进入 core callback 的 handler 名称 |
| `error_stack` | 前台 handler 产生的 `ApixEventError` 列表 |

### `event.accept()`

标记事件已被接受。后续 handler 不再执行自己的 core callback，但适用的错误通知和 accepted 通知仍可能执行。

### `event.has_error`

是否有已记录的前台 handler 错误。

### `event.datetime`

时间戳信息，会将 `timestamp` 转为本地 `datetime` 返回。

## 发布事件

推荐通过共享 `ApixEventPipe`：

```python
from apixis import EventType, get_event_pipe, start_core

await start_core()
pipe = get_event_pipe()

await pipe.post_event(
    event_type=EventType.INFO,
    event_name="job.created",
    context={"job_id": 42},
)
await pipe.join()
```

`post_event()` 会创建 `ApixEvent` 并放入本地队列；本地队列满时等待空位。

`pipe.join()` 等待队列项目完成交接，不等待 handler 执行结束。需要等待结果事件时，可使用下文的 `await_for()` 或 `wait_for_event()`；也可以由处理器通过应用自己的结果对象或信号通知调用方，完整示例见[快速开始](../../quickstart.md)。

## `ApixEventPipe` 常用接口

```python
await pipe.put(event)
await pipe.post_event(...)
pipe.put_nowait(event)

event = await pipe.get()
pipe.task_done()
await pipe.join()
await pipe.clear()
```

所有事件均通过 `BuiltinChannel` 的本地队列处理，详见[事件通道](./channels.md)。

## 订阅

```python
from apixis import subscribe


@subscribe("job.*")
async def observe_job(event):
    ...
```

支持 glob 风格模式。详细排序、错误语义与取消语义见 [Event handlers](./handlers.md)。

## 等待事件：`await_for` 与 `wait_for_event`

先看等待之前，当前代码是否还需要发布请求：

| 你要做的事 | 应该使用 | 执行顺序 |
| --- | --- | --- |
| 立即等待其他任务接下来产生的事件 | `await await_for(...)` | 注册 → 等待 → 返回事件 |
| 先订阅回包，再由当前代码发布请求 | `async with wait_for_event(...) as waiter` | 注册 → 发布请求 → `await waiter.wait()` |

**两个接口都能等待一个或多个事件。区别在于是否需要在“注册”和“等待”之间执行自己的代码。**

### 正反例：发布请求并等待回包

假设 `job.start` 的处理器会向 `context["reply"]` 指定的事件名发布结果。以下代码在 async 函数中执行，`reply` 是本次请求独有的回包名。

**正例：使用 `wait_for_event`，先订阅，再发布。**

```python
async with wait_for_event(reply, time_out=40) as waiter:
    await pipe.post_event(
        event_type=EventType.WORKFLOW,
        event_name="job.start",
        context={"reply": reply},
    )
    event = await waiter.wait()
```

进入上下文时订阅已注册；即使回包在 `.wait()` 前到达，也会保留结果。

**反例：先发布，再使用 `await_for` 订阅。**

```python
await pipe.post_event(
    event_type=EventType.WORKFLOW,
    event_name="job.start",
    context={"reply": reply},
)
event = await await_for(reply, time_out=40)
```

回包可能在 `await_for` 注册前就已出队，导致错过结果并超时。两个接口都不回放注册前已出队的事件；需要自己发起请求时，应使用上面的正例。

### 同时等待多个事件

接口的选择仍取决于是否需要先注册再发布，与事件数量无关：

```python
# 事件由其他任务独立产生：直接等到两个事件都收到。
events = await await_for(["job.validated", "job.saved"], time_out=40)

# 当前代码需要发起任务：先注册两个订阅，再发布请求。
async with wait_for_event(
    ["job.validated", "job.saved"], point="processed", time_out=40,
) as waiter:
    await get_event_pipe().post_event(
        event_type=EventType.WORKFLOW,
        event_name="job.start",
    )
    events = await waiter.wait()
    validated = events["job.validated"]
    saved = events["job.saved"]
```

传入字符串时，`await_for()` 和 `.wait()` 返回单个 `ApixEvent`；传入列表或集合时，收齐所有模式的事件后返回以**订阅模式**为键的字典，即使集合中只有一个模式也是如此。模式不能为空，重复模式合并，字典不保证键顺序。每个模式只保留首个匹配，后续事件不覆盖；重叠模式可以由同一个事件满足。在上下文内重复调用 `.wait()` 返回同一个事件或字典。

### 共同参数与超时

```text
event_name: str | list[str] | set[str]
point: Literal["received", "processed"] = "received"
filter: list[str] | str | None = None
time_out: float | None = None
```

`event_name` 支持大小写敏感的 glob 模式；`filter` 排除匹配的事件名。`point="received"` 在普通前台 handler 之前取得事件；`point="processed"` 在普通前台 handler 完成后取得事件，不等待后台 handler。事件被 accept、发生前台错误或事件分发被取消时，适用的通知回调同样可以返回该事件；调用方可检查 `accepted`、`has_error` 和 `error_stack`。返回的是原始、可变的事件对象，不是内容快照，后续 handler 仍可能修改它。

`time_out=None` 表示没有截止时间，非正值表示立即超时。两个接口都从所有订阅注册完成开始计时；对于 `wait_for_event`，这也包括上下文内调用 `.wait()` 前的发布和其他工作。所有等待项都需要在同一截止时间前到达所选 `point`，只收到部分事件也会超时，不返回部分结果。

**截止时间不会中断 `wait_for_event` 的上下文主体**：超时使等待结果失效，随后 `.wait()` 抛出 `TimeoutError`；若所有结果已在截止时间前收齐，之后调用 `.wait()` 仍能取到。如需限制整个上下文主体，可在外层使用 `asyncio.timeout()`。

`await_for` 在返回、超时或取消时注销订阅；`wait_for_event` 在上下文退出时注销订阅并释放计时器，包括主体异常、超时和取消。未等待就退出时，尚未完成的结果会被取消；取消正在执行的 `.wait()` 也会取消尚未完成的结果。

在 handler 内，两种接口都在实际等待结果时通过 `suspend_process()` 借出当前处理链的调度许可，结束等待时恢复。`wait_for_event` 的上下文进入本身不借出许可；上下文主体中的 `pipe.post_event()` 继续使用其自身的许可借出逻辑。

## 取消订阅与检查

```python
from apixis import (
    get_handler,
    get_handler_meta,
    get_unmatched_subscriptions,
    is_registered,
    unsubscribe,
)

unsubscribe("observe_job")
```

- `unsubscribe(name, missing_ok=True)`：移除 handler；
- `get_handler(name)`：返回 handler 或 `None`；
- `get_handler_meta(name)`：读取注册元数据；
- `is_registered(name)`：检查是否注册；
- `get_unmatched_subscriptions(name)`：返回当前尚未观察到匹配事件名的订阅模式。

`ApixHandlerRegistry(event_registry, *, cache_size=8192)` 将精确订阅与 glob 订阅分别索引，并按原有优先级和相对插入顺序组合处理器链。匹配链使用有界 LRU；共享 core 的容量由 `PIPELINE.handler_chain_cache_size` 设置。淘汰只影响后续查询是否需要重建，不改变已取出事件的处理器引用。

独立 registry 可使用 `clear()` 同时清理订阅、索引和链缓存；该操作不清空 `ApixEventRegistry` 的观察历史。更新订阅和排序时使用 `handler.register()`、`subscribe()` 或 registry 注册接口，不单独修改内部索引、bucket 或缓存。

## 已观察到的事件名

`ApixEventRegistry` 可查询本地已经观察到的精确事件名；它不保存事件内容，也不能用于查询某个事件是否处理完成。

```python
from apixis import get_event_registry

registry = get_event_registry()
seen = registry.get_registered_events()  # frozenset[str]
registry.clear()
```

## 在 handler 内发布和等待

在 handler 内发布后续事件，优先使用 `pipe.post_event()`。它会在等待发布期间让出当前事件的调度容量。

handler 等待另一个事件时，可以使用 `await_for()` 或 `wait_for_event()`，它们会在等待结果期间借出当前处理链的调度许可。请求／回包流程使用 `wait_for_event()`，在发布请求前确保结果订阅已经注册。自行等待应用的 Future 或信号时，需要自行安排容量释放或将后续逻辑放到结果事件的订阅处理器中。图中的嵌套 `invoke()` 和 `interrupt()` 已提供相应的等待行为，见[图 API](../graph/README.md)。

并行子任务共享前台处理链的许可。仅取消子任务时，最后恢复的子任务会先归还许可，再向父任务传播取消；取消整个前台分发时，子任务不会为了重获许可而阻止分发退出。父 handler 自行组织可恢复的超时或并行等待时，应在父级使用 `suspend_process()` 包住等待过程，让父级 scope 负责恢复许可后继续执行。分发结束会取消仍停在许可重获阶段的子任务，避免遗留等待者。

## 暂停消费与停止管道

应用入口可以显式管理共享组件：

```python
from apixis import get_event_loop, get_event_pipe

event_loop = get_event_loop()
pipe = get_event_pipe()
await event_loop.stop()
await pipe.stop()
```

`event_loop.stop()` 暂停取出新事件，保留队列和待分发事件；已经启动的前台处理和后台任务继续运行，停止接口不会等待它们完成。关闭应用前，应先等待应用自身需要完成的业务工作。`pipe.stop()` 重置启动状态并保留队列内容，详见[事件通道](./channels.md)。

[处理器](./handlers.md) · [事件通道](./channels.md) · [文档首页](../../README.md)
