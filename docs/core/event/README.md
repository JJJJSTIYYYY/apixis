# 事件 API

事件 API 负责创建、发布、路由和处理 `ApixEvent`。常用接口可直接从 `apixis` 导入。

## 共享事件系统

### `await start_core()`

唤起共享 event core。已运行时重复调用不会重复启动；返回不表示后台消费者已开始处理事件，也不表示已发布事件处理完成。启动连接时发生的异常会传播给调用方。

共享 core 只构建一次，后续调用复用同一组 registry、pipe、handler registry 和 event loop。
`start_core()`、同步和异步 getters 均不接受 `core` 参数，订阅、等待器和图都使用这组共享组件。

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

发布时捕获当前 `ContextVar` 绑定，整条前台 handler chain 在该快照中执行，后台 handler 继承启动时的绑定。发布者之后的 `set/reset` 不影响已发布事件，handler 的绑定修改也不回写发布者；变量值中的可变对象仍按引用共享。直接使用 `put()` / `put_nowait()` 注入手工构造的事件不捕获发布者上下文。

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
async with wait_for_event(reply) as waiter:
    await pipe.post_event(
        event_type=EventType.WORKFLOW,
        event_name="job.start",
        context={"reply": reply},
    )
    event = await waiter.wait(time_out=40)
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
    ["job.validated", "job.saved"], point="processed",
) as waiter:
    await get_event_pipe().post_event(
        event_type=EventType.WORKFLOW,
        event_name="job.start",
    )
    events = await waiter.wait(time_out=40)
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
keep_listening: bool = False
```

`event_name` 支持大小写敏感的 glob 模式；`filter` 排除匹配的事件名。`point="received"` 在普通前台 handler 之前取得事件；`point="processed"` 在普通前台 handler 完成后取得事件，不等待后台 handler。事件被 accept、发生前台错误或事件分发被取消时，适用的通知回调同样可以返回该事件；调用方可检查 `accepted`、`has_error` 和 `error_stack`。返回的是原始、可变的事件对象，不是内容快照，后续 handler 仍可能修改它。

`time_out=None` 表示没有截止时间，非正值表示立即超时。`await_for(time_out=...)` 限制这次等待；`wait_for_event(time_out=...)` 从注册完成开始计时，包括调用 `.wait()` 前的工作。每次 `.wait(time_out=...)` 都会替换当前截止时间，从本次调用计时；省略或传入 `None` 则取消尚未到期的截止时间，无限等待。已经超时关闭的监听不会恢复。所有等待项都需要在同一截止时间前到达所选 `point`，只收到部分事件也会超时，不返回部分结果。

**截止时间不会中断 `wait_for_event` 的上下文主体**：默认超时使等待结果失效，随后 `.wait()` 抛出 `TimeoutError`；若所有结果已在截止时间前收齐，之后调用 `.wait()` 仍能取到。如需限制整个上下文主体，可在外层使用 `asyncio.timeout()`。

默认行为保持不变：超时抛出 `TimeoutError`，取消或退出上下文会清理尚未完成的等待；普通上下文退出后再调用 `.wait()` 会报错。监听由 `EventWaiter` 管理，收齐结果或普通超时后立即注销，不必等到上下文退出。收齐的结果在上下文内仍可重复读取。

多模式等待在收齐所有结果后统一注销全部监听；部分结果到达时保留监听与路由缓存。每个模式只保留首次结果，同一事件可以同时满足重叠模式。无效的 `point`、模式和超时参数在注册前拒绝；超时必须是有限数字，布尔值不属于数字。

在 handler 内，两种接口都在实际等待结果时通过 `suspend_process()` 借出当前处理链的调度许可，结束等待时恢复。`wait_for_event` 的上下文进入本身不借出许可；上下文主体中的 `pipe.post_event()` 继续使用其自身的许可借出逻辑。

### 超时后继续收集结果

默认超时会结束监听；设置 `keep_listening=True` 后，超时返回 `EventWaiter`，后续通过 `await waiter.get_result()` 等待完整结果：

```python
from apixis import EventWaiter, await_for

result = await await_for("job.finished", time_out=1, keep_listening=True)
if isinstance(result, EventWaiter):
    # 首次等待已超时，监听仍在；这里可以先处理其他工作。
    result = await result.get_result()
```

请求／回包场景仍用原来的 `async with wait_for_event(...)`；将等待改为 `await waiter.wait(time_out=1, keep_listening=True)` 即可。每次 `.wait()` 都只使用本次传入的等待时长：数字表示从本次调用计时，省略或传入 `None` 均无限等待，首次调用也相同。`keep_listening` 省略时沿用等待器配置。同一等待器的并发等待共享这个预算与策略。已有结果会直接返回，只有尚未收齐且超时才返回等待器；已经按默认策略超时关闭的等待器不能恢复，应在超时前启用该选项。

超时拿到等待器后，也可以再次 `await waiter.wait()` 无限等待直到收齐，或通过 `await waiter.wait(time_out=5)` 指定新的等待时长；期间已收到的事件会保留，监听不会重复注册。若再次超时且继续监听，仍返回同一个等待器。

实际发生延迟返回后，正常退出上下文也会保留监听。`get_result()` 不重启已用完的等待预算，会继续等到收齐后返回原有的单事件／字典结构，并自动注销监听。若决定放弃，调用幂等的 `waiter.close()`；取消正在执行的 `.wait()`、`get_result()` 或上下文主体抛出异常，也会清理监听。仅设置 `keep_listening=True` 而未发生延迟返回，不会让监听在退出上下文后残留。

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
- `get_handler_meta(name)`：读取独立的注册元数据副本，修改返回值不影响订阅、过滤或排序；
- `is_registered(name)`：检查是否注册；
- `get_unmatched_subscriptions(name)`：返回当前尚未观察到匹配事件名的订阅模式。

`ApixHandlerRegistry(event_registry, *, cache_size=8192)` 将精确订阅与 glob 订阅分别索引，并按原有优先级和相对插入顺序组合处理器链。匹配链使用有界 LRU；共享 core 的容量由 `APIXIS.EVENT.handler_chain_cache_size` 设置。淘汰只影响后续查询是否需要重建，不改变已取出事件的处理器引用。

独立 registry 可使用 `clear()` 同时清理订阅、索引和链缓存；该操作不清空 `ApixEventRegistry` 的观察历史。更新订阅和排序时使用 `handler.register()`、`subscribe()` 或 registry 注册接口，不单独修改内部索引、bucket 或缓存。

## 已观察到的事件名

`ApixEventRegistry` 可查询本地已经观察到的精确事件名；它不保存事件内容，也不能用于查询某个事件是否处理完成。

```python
from apixis import get_event_registry

registry = get_event_registry()
seen = registry.get_registered_events()  # frozenset[str]
registry.clear()
```

## 背压与挂起

### 背压限制什么

假设每个前台 handler 都要 `await` 一次 HTTP 请求、或者一次数据库查询。如果事件持续到达且分发不受限制，大量请求会同时占用连接和下游服务资源。派发背压限制同时持有调度许可的前台事件处理链数量；普通的 HTTP、数据库调用或 `asyncio.sleep()` 不会自动归还许可，因此这些等待期间仍计入容量。

队列容量和派发容量分别约束两个阶段：

| 配置 | 限制范围 | 容量用完时 |
| --- | --- | --- |
| `APIXIS.EVENT.event_pipe_max_len` | 仍在本地队列中的事件数 | 异步发布等待空位 |
| `APIXIS.EVENT.event_loop_backpressure` | 同时持有许可的前台处理链数 | 新分发或挂起后的恢复等待许可 |

配置的默认值与有效范围见[配置文档](../config/README.md#队列与并发)。

派发背压不是全部 task 的数量上限。同一处理链内的并行节点或子任务共享许可，后台 handler 不持有前台许可，挂起等待的 task 也仍然存活。因此它既不等于单个 HTTP／数据库资源的精确并发限制，也不保证整体在途请求数和内存占用有界。整体过载保护还需要在外部请求入口限制接纳数量，并在具体资源所属位置控制并发。

### 挂起为什么要借出许可

handler 等待的工作有时也需要事件循环继续分发。如果等待方一直持有许可，所有许可都被占用时会出现循环等待：

- **等待回复事件**：处理链 A 发布请求后等待回复 B；B 需要新的分发许可才能执行，但许可被 A 等等待方占用。
- **向满队列发布事件**：A 持有许可并等待队列空位；消费者已经取出 B，但正在等分发许可，无法继续取出队列中的事件腾出空位。

挂起会在这类等待期间临时借出当前处理链的许可，让请求、回复或其他排队事件能够继续推进；等待结束后，再按处理链的状态恢复许可。挂起不等于取消或结束 task，普通 `await` 也不等于借出许可。并行子任务共享的许可由内部统一协调，避免重复释放，以及取消时为了恢复许可而阻碍处理链退出。

这也解释了为什么存活的 dispatch task 可以多于派发容量：等待方让出了执行容量，但仍保留继续执行所需的状态。队列中的积压可以转移为挂起的 task，不能仅凭队列有界就认定整体资源有界。

### 在 handler 内发布和等待

`suspend_process()` 是内部许可管理机制，未从 `apixis`、`apixis.core` 或 `apixis.core.event` 导出。业务代码通过公开接口获得相应的挂起与恢复行为：

- 发布后续事件使用 `pipe.post_event()`，它会在发布期间借出当前许可；直接 `pipe.put()` 不提供这层协调。
- 等待事件使用 `await_for()`、`wait_for_event()` 或 `EventWaiter.wait()` / `get_result()`，它们在实际等待结果时借出许可。`wait_for_event` 的上下文进入只注册监听，不借出许可。
- 请求／回包流程使用 `wait_for_event()`，按“注册监听 → 发布请求 → 等待结果”的顺序执行，避免漏掉提前到达的回包。
- 图中的嵌套 `invoke()` 和 `interrupt()` 已协调相应的等待，见[图 API](../graph/README.md)。

如果自行等待应用的 `Future`、`Event` 等信号，且其完成依赖另一个事件 handler，框架无法自动识别这项依赖。应改用上述事件等待接口，或将后续逻辑放到结果事件的订阅处理器中，避免持有许可等待依赖方。

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
