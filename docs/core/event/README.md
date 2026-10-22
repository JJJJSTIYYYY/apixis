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

两个接口都通过临时 handler 接收第一个匹配事件，返回 `ApixEvent`，并支持相同的参数：

```python
from apixis import await_for, wait_for_event
```

```text
event_name: str
point: Literal["received", "processed"] = "received"
filter: list[str] | str | None = None
time_out: float | None = None
```

`event_name` 支持大小写敏感的 glob 模式；`filter` 排除匹配的事件名。`point="received"` 在普通前台 handler 之前取得事件；`point="processed"` 在普通前台 handler 完成后取得事件，不等待后台 handler。事件被 accept、发生前台错误或事件分发被取消时，适用的通知回调同样可以返回该事件；调用方可检查 `accepted`、`has_error` 和 `error_stack`。

返回的是原始、可变的事件对象，不是内容快照。特别是 `received` 返回后，后续 handler 仍可修改同一个对象。

### 关键差异：注册就绪与等待结果

| 行为 | `await_for(...)` | `wait_for_event(...)` |
| --- | --- | --- |
| 使用方式 | `event = await await_for(...)` | `async with wait_for_event(...) as waiter`，然后 `event = await waiter.wait()` |
| 注册时机 | 协程实际开始执行时注册，然后立即等待 | 进入上下文时注册，执行上下文主体前已经就绪 |
| 调用后是否已经注册 | 创建协程或调用 `create_task()` 都不保证已注册 | 仅创建上下文管理器不注册；进入主体时保证已注册 |
| 注册与等待之间能否发布请求 | 单个调用同时完成注册和等待；另建 task 时需要自行协调就绪 | 可以直接在上下文主体中发布请求，再调用 `.wait()` |
| 结果如何获取 | `await` 的返回值 | `.wait()` 的返回值；调用前收到的事件保存在内部 Future 中 |
| `time_out` 从何时计时 | 协程执行并注册后开始等待时 | 上下文进入、注册完成时，包括调用 `.wait()` 前的发布和其他工作 |
| 订阅何时移除 | 返回、超时或调用方取消时 | 离开上下文时，包括主体异常、超时和取消 |
| 适用场景 | 立即等待其他任务产生的事件 | 先订阅结果、再发起请求、最后获取结果 |

**两个接口都不回放注册之前已经出队的事件。** 事件出队时即固定 handler 列表，随后才注册的 waiter 无法接收该事件。这是正常的订阅顺序语义。

### `await_for`：注册后立即等待

```python
from apixis import await_for

event = await await_for(
    "job.finished",
    point="processed",
    time_out=40,
)
result = event.context["result"]
```

`await_for` 从开始执行到注册完成之间不会让出 asyncio 事件循环。但 `await_for(...)` 本身只创建协程，`asyncio.create_task(await_for(...))` 只安排其执行；`create_task()` 返回不表示订阅已注册。因此，创建 task 后立刻发布事件，不具备“先注册、再发布”的接口保证。调用方在注册之前主动让出执行权时，也可能让其他任务先消费目标事件。

若需要请求／回包流程，使用 `wait_for_event` 显式建立就绪顺序，无需通过 `sleep(0)` 安排 waiter task 的执行。

### `wait_for_event`：先注册，再发布和取结果

```python
from uuid import uuid4

from apixis import EventType, get_event_pipe, wait_for_event


async def execute(tool_call: dict) -> str:
    # 每个请求使用独立回包名，避免并发调用取得其他请求的结果。
    reply = f"tool.result.{uuid4().hex}"
    async with wait_for_event(
        reply, point="processed", time_out=40,
    ) as waiter:
        # 此处订阅已注册。请求处理器应向 context["reply"] 发布结果事件。
        await get_event_pipe().post_event(
            event_type=EventType.WORKFLOW,
            event_name="tool.request",
            context={"tool_call": tool_call, "reply": reply},
        )
        event = await waiter.wait()
        return event.context["result"]
```

上下文返回 `EventWaiter`，而非已经到达的事件。即使顺序是“注册 → 发布 → 收到回包 → 调用 `.wait()`”，Future 也会保留第一个结果，`.wait()` 直接返回它。后续匹配事件不覆盖结果；在上下文内顺序重复调用 `.wait()` 会返回同一个事件。

`time_out=None` 表示没有截止时间，非正值表示立即超时。超时从注册完成开始计时，事件需要在截止时间之前到达所选的 `point`。**截止时间限制结果等待，不会中断上下文主体**：即使发布仍在等待队列容量，超时也只使结果失效，随后 `.wait()` 抛出 `TimeoutError`。若事件已在截止时间前到达，之后再调用 `.wait()` 仍可取到结果。如需限制整个上下文主体的执行时间，可在外层使用 `asyncio.timeout()`。

上下文在所有退出路径上注销订阅并释放计时器。未调用 `.wait()` 就退出时，尚未完成的 Future 会被取消；取消正在执行的 `.wait()` 也会取消尚未完成的结果。退出后再调用 `.wait()` 会抛出 `RuntimeError`。

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
