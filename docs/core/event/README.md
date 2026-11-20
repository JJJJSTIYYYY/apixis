# 事件 API

事件 API 负责创建、发布、路由和处理 `ApixEvent`。常用接口可直接从 `apixis` 导入。

## 共享事件系统

### `await start_core()`

唤起共享 event core。已运行时重复调用不会重复启动；返回不表示后台消费者已开始处理事件，也不表示已发布事件处理完成。启动连接时发生的异常会传播给调用方。

共享 core 在同一个 asyncio 事件循环中使用，只构建一次，后续调用复用同一组 registry、pipe、handler registry 和 event loop。
`start_core()`、同步和异步 getters 均不接受 `core` 参数，订阅、等待器和图都使用这组共享组件。

```python
from apixis import start_core

await start_core()
```

### `await stop_core()`

暂停共享 core：取消尚未完成的自动启动，依次停止事件消费者和 pipe。未构建时直接返回，重复调用安全；保留组件、订阅、队列和待分发事件，供后续 `start_core()` 或 getter 恢复使用。启动与停止通过同一把锁串行执行。

已启动的前台处理和后台任务继续运行，接口不等待它们完成。应用关闭前应先等待自身需要完成的业务工作。

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

- `EventType.DEFAULT`（发布接口省略类别时使用）
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

使用全局 `post_event()`，或缓存共享 `ApixEventPipe`：

```python
from apixis import EventType, get_event_pipe, post_event, start_core

await start_core()
pipe = get_event_pipe()

await post_event(
    event_type=EventType.INFO,
    event_name="job.created",
    context={"job_id": 42},
)
await pipe.join()
```

`post_event()` 会创建 `ApixEvent` 并放入本地队列；本地队列满时等待空位。

发布时捕获当前 `ContextVar` 绑定，整条前台 handler chain 在该快照中执行，后台 handler 继承启动时的绑定。发布者之后的 `set/reset` 不影响已发布事件，handler 的绑定修改也不回写发布者；变量值中的可变对象仍按引用共享。直接使用 `put()` / `put_nowait()` 注入手工构造的事件不捕获发布者上下文。

`pipe.join()` 等待队列项目完成交接，不等待 handler 执行结束。需要等待本次前台处理时使用 `post_and_wait()`；需要监听结果事件时使用 `wait_event()` 或 `expect_event()`；也可以由处理器通过应用自己的结果对象或信号通知调用方，完整示例见[快速开始](../../quickstart.md)。

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

事件管道直接使用有界 `asyncio.Queue`，详见[事件队列](./channels.md)。

## 订阅

```python
from apixis import subscribe


@subscribe("job.*")
async def observe_job(event):
    ...
```

支持 glob 风格模式。详细排序、错误语义与取消语义见 [Event handlers](./handlers.md)。

## 等待事件

`wait_event()` 注册后立即等待外部产生的事件；`expect_event()` 在进入上下文时同步完成注册，允许在上下文主体中运行其他操作，再读取结果。`await_for` 和 `wait_for_event` 分别是这两个本地接口的原有别名。

监听外部事件并按内容筛选：

```python
from apixis import wait_event

reply = await wait_event(
    "job.finished", point="processed", time_out=40,
    predicate=lambda event: event.context["job_id"] == 42,
)
```

先注册监听，在其他操作期间保留收到的事件：

```python
from apixis import expect_event

async with expect_event(
    "job.finished", point="processed", time_out=40,
    predicate=lambda event: event.context["job_id"] == 42,
) as waiter:
    await start_external_job(42)
    reply = await waiter.wait()
```

`expect_event()` 不会自动发布事件。注册不让出当前任务，因此上下文主体开始时监听已就绪；直接创建 `wait_event()` 协程或使用 `create_task()` 则不保证它已经开始注册。已经消费过的事件不会重放。

| 参数 | 含义 |
| --- | --- |
| `event_name` | 单个非空 glob 字符串，或非空字符串 list/set。字符串返回一个事件；list/set 返回按请求模式索引的字典 |
| `point` | `received` 在普通 handler 之前观察，`processed` 在前台 handler 之后观察。均不等待后台 handler |
| `filter` | 一个 glob 字符串或字符串列表，排除匹配的事件名 |
| `predicate` | 可选同步函数，在名称匹配和排除过滤后执行。返回假值时继续等待，抛出异常时失败并清理监听 |
| `time_out` | 从注册时起算的固定秒数，包含读取前的工作。`None` 无期限，非正数立即超时 |

每个模式仅收集第一个通过过滤的事件。多个模式可以匹配同一事件，并引用同一个 `ApixEvent`；重叠模式可能分别执行 predicate。多模式等待需要全部匹配后才返回，超时不返回部分结果。

两个观察点都返回原始可变事件。`received` 的结果可能在后续 handler 中继续变化；`processed` 表示前台链已走到该观察点，后台工作仍可能继续。事件被接受、发生前台错误或派发被取消时，适用的 processed 监听仍能得到通知。

### 等待器生命周期

`EventWaiter` 构造时同步注册监听，整个生命周期绑定一个 Future。完成、超时、取消和上下文退出都会注销临时 handler 并取消计时器；即使没有任务正在读取结果，Future 到达终态也会清理资源。

```python
from apixis import EventWaiter

async with EventWaiter(["job.finished", "job.saved"], time_out=40) as waiter:
    events = await waiter.wait()

assert await waiter.get_result() is events
```

`wait()` 和 `get_result()` 是同一个读取接口，均无参数。重复读取保留同一 Future 的结果、异常或取消状态，不重新注册监听，也不延长期限。上下文仅能进入一次。

超时抛出 `TimeoutError`。取消任意未完成的读取会取消共享 Future，其他读取也会收到取消；`close()` 同步、幂等地取消未完成 Future 并释放资源。已完成的结果或异常保持可读。predicate 必须是同步 callable，非法参数在注册前报错。

handler 内等待时会暂时归还派发许可，使其他事件可以推进；监听清理先于许可恢复。等待器不提供延迟超时续等或重设期限的能力。

## 发布并等待前台处理

```python
from apixis import post_and_wait

result = await post_and_wait(
    event_name="job.process", context={"job_id": 42}, time_out=10,
)
if result.has_error:
    print(result.error_stack)
```

`post_and_wait()` 在发布前安装 processed 监听，使用本次生成的 `event_id` 关联结果，因此同名并发事件不会串结果。支持与 `post_event()` 相同的 `event_type`、`event_name`、`context` 参数，额外的 `time_out` 覆盖队列入队和前台处理，返回本次发布的事件。

前台 handler 的错误保留在事件的 `error_stack` 中。后台 handler 和 handler 另行发布的后续事件不在等待范围内。超时或取消会删除临时监听，已经入队的事件及其 handler 继续处理；未完成入队的取消不会发布事件。

## 运行状态

```python
from dataclasses import asdict
from apixis import get_event_runtime_status

status = get_event_runtime_status()
print(asdict(status))
```

同步查询返回不可变 `EventRuntimeStatus`，不创建 core，也不触发自动启动。未初始化时 `initialized=False`、`event_loop=None`。在共享事件循环中查询：

| 字段 | 含义 |
| --- | --- |
| `initialized` / `running` | 是否已构建，以及 pipe 与消费者是否均已启动 |
| `event_queue_size` / `event_queue_capacity` | 当前队列长度和容量 |
| `wait_listeners` | 临时等待监听数，每个请求模式占一个监听 |
| `event_loop` | 不可变 `EventLoopStatus`，包含消费者状态、许可容量/占用、派发任务数、待派发事件标记和后台任务数 |

任务数包含已挂起的任务及回调尚未清理的已完成任务。挂起的 handler 可以借出许可，因此任务数与许可占用不同。快照不会随着后续运行状态变化而改变。

## 模块职责

`factory.py` 管理共享组件及启动、停止；`event_pipe.py` 管理本地队列；`event_loop.py` 管理消费和派发。`base/` 定义事件、handler、许可上下文和状态快照；`handler_registry/` 管理订阅匹配、排序和独立等待缓存；`subscription/` 分别提供发布、订阅和等待入口。graph 使用同一组共享事件组件。

[事件处理器](./handlers.md) · [事件队列](./channels.md) · [图 API](../graph/README.md) · [文档首页](../../README.md)
