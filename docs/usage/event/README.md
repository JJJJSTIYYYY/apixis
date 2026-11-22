# 事件 API

事件 API 用来发布事件、注册处理器，以及等待外部事件或本次发布的处理结果。以下接口均可从 `apixis` 导入。

## 创建与启动

先调用 `create_core()`，再注册订阅或创建等待器；调用 `await start_core()` 后才开始消费队列。发布可以早于启动，队列满时仍会等待空位。

```python
from apixis import close_core, create_core, post_event, start_core

create_core()
await post_event(event_name="job.created", context={"job_id": 42})
try:
    await start_core()
    # 在这里运行应用，或等待业务结果。
finally:
    await close_core()
```

应用共享同一个 core，并在同一个 asyncio 事件循环中操作它。`get_core()` 同步读取已有实例，不创建或启动；创建前调用 `get_core()` 或 `start_core()` 会抛出 `RuntimeError`。暂停、恢复、关闭及关闭回调见[生命周期](../lifecycle.md)。

## `EventType`

| 值 | 用途 |
| --- | --- |
| `DEFAULT` | 发布时的默认类别 |
| `TRANSIENT` | 正常派发，但不保留名称供订阅诊断查询 |
| `WORKFLOW`、`LIFECYCLE` | 工作流与生命周期事件 |
| `INFO`、`WARNING`、`ERROR` | 应用自选的信息、警告与错误类别 |

类别不会自动接受事件或改变 handler 的错误处理规则。

## `ApixEvent`

处理器和等待器收到同一个可变事件对象：

| 字段或属性 | 含义 |
| --- | --- |
| `event_id` | 本次事件的唯一 ID |
| `event_type` | 事件类别 |
| `event_name` | 用于订阅匹配的名称 |
| `context` | 应用传入的载荷，保留原始 Python 对象 |
| `timestamp` / `datetime` | Unix 时间戳，以及转换后的本地 `datetime` |
| `accepted` | 是否调用过 `event.accept()`，初始为 `False` |
| `cancelled` | 前台事件执行是否请求取消或进入取消清理，初始为 `False`；单个后台任务取消不修改它 |
| `seen` | 已进入业务函数的 handler 名称列表，在函数执行前追加 |
| `error_stack` | 前台 handler 的 `ApixEventError` 列表 |
| `has_error` | 是否已有前台错误记录 |

`event.accept()` 标记事件已被接受，后续 handler 跳过业务函数，但仍可能执行适用的错误通知和接受通知。排序、错误与取消规则见[事件处理器](./handlers.md)。等待方只观察结果，事件的业务修改应由处理器负责。

## 发布事件

```python
from apixis import EventType, post_event

await post_event(
    event_type=EventType.INFO,
    event_name="job.created",
    context={"job_id": 42},
)
```

`post_event()` 创建并入队一个事件，返回时只保证入队成功；不要求已注册业务处理器。需要等待处理完成时使用 `post_and_wait()`。未启动或已暂停时，事件留在队列中，直到显式启动消费。

应用应使用非空事件名。当前 `post_event(event_name="")` 可以入队，但消费时会跳过它；`post_and_wait(event_name="")` 在注册等待监听时抛出 `ValueError`。

发布时捕获当前 `ContextVar` 绑定。前台链使用这份绑定，后台 handler 继承创建任务时的绑定。发布者之后的 `set/reset` 不影响已发布事件，handler 的绑定修改也不回写发布者；变量值中的可变对象仍按引用共享。

队列容量、背压与满队列等待见[本地事件队列](./channels.md)。

## 订阅

先创建 core，再注册订阅。注册不会启动消费。

```python
from apixis import subscribe


@subscribe("job.*")
async def observe_job(event):
    print(event.event_name, event.context)
```

事件名支持区分大小写的 glob 模式。排序、替换、查询、分组注销和通知回调见[事件处理器](./handlers.md)。

## 等待事件

`wait_event()` 注册后立即等待外部产生的事件；`expect_event()` 先注册，再运行上下文主体。`await_for` 是 `wait_event` 的别名，`wait_for_event` 是 `expect_event` 的别名。

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

`expect_event()` 不会自动发布事件。上下文进入时同步完成注册，不让出当前任务，因此主体开始时监听已就绪。直接创建 `wait_event()` 协程或使用 `create_task()` 不保证注册已完成。

监听必须在目标事件出队前注册。出队时已经确定全部监听和业务处理器，即使事件还在等待派发许可，后注册的 processed 监听也不会加入该次处理。历史事件不会重放。

| 参数 | 含义 |
| --- | --- |
| `event_name` | 非空 glob 字符串，或非空字符串 list/set。字符串返回单个事件；list/set 返回以请求模式为键的字典 |
| `point` | 默认 `received`，在普通 handler 之前观察；`processed` 在前台 handler 之后观察。均不等待后台 handler |
| `filter` | 一个 glob 字符串或字符串列表，排除匹配的事件名 |
| `predicate` | 可选同步函数，在名称匹配和排除过滤后执行。返回假值时继续等待，抛出异常时等待失败并清理监听 |
| `time_out` | 默认 `None`，无期限；否则从注册起算固定秒数，包含读取前的工作。非正数将期限设在当前或过去，终态规则见下文 |

每个模式只收集第一个通过筛选的事件。多个模式可以匹配同一个事件，并引用同一个 `ApixEvent`；重叠模式可能分别调用 predicate。多模式等待需要全部匹配后才返回，超时不返回部分结果。

`EventPredicate` 是公开的类型别名，签名为 `Callable[[ApixEvent], bool]`，可用于给同步 predicate 添加类型标注。

两个观察点均返回原始可变事件。`received` 的结果可能在后续 handler 中变化；`processed` 表示前台链已到达该观察点，后台工作仍可能继续。事件已接受或出现前台错误时，适用的 processed 监听仍能得到事件。

### 等待器生命周期

`EventWaiter` 与上述接口采用相同的等待规则，也可直接使用：

```python
from apixis import EventWaiter

async with EventWaiter(["job.finished", "job.saved"], time_out=40) as waiter:
    events = await waiter.wait()

assert await waiter.get_result() is events
```

`EventWaiter` 在构造时注册；`wait()` 和 `get_result()` 是同一个无参读取接口。重复读取保留同一次收集的结果、异常或取消状态，不重新注册，也不延长期限。上下文只能进入一次。

`active` 表示结果是否尚未完成。完成、超时、取消和上下文退出都会移除临时监听与定时器，即使没有任务正在读取结果也会清理。`close()` 同步取消尚未完成的等待并清理资源，可重复调用；已有结果或异常仍可读取。取消任意未完成的读取会影响同一等待器的所有读取者，均保持 asyncio 的 `CancelledError` 语义。

正常匹配和真实事件取消在调用 predicate 前检查固定期限；已过期时抛出 `TimeoutError`，不会再调用 predicate。定时器也会使未完成的等待超时。core 关闭另有直接完成路径，见[关闭时的等待结果](#关闭时的等待结果)。

handler 内等待时会暂时归还派发许可，让其他事件推进；监听清理完成后才恢复许可。等待器不支持延迟超时续等或重设期限。

## 发布并等待前台处理

```python
from apixis import post_and_wait

result = await post_and_wait(
    event_name="job.process", context={"job_id": 42}, time_out=10,
)
if result is None:
    print("Processing was cancelled")
elif result.has_error:
    print(result.error_stack)
```

`post_and_wait()` 先安装 processed 监听，再发布事件，用本次生成的 `event_id` 关联结果；同名并发请求不会串结果。它不要求额外的业务订阅：core 消费本次事件后，仍会返回这个事件。

参数 `event_type`、`event_name`、`context` 与 `post_event()` 相同，`time_out` 默认 `None`，覆盖队列入队和前台处理。正常完成返回本次事件，已入队事件在前台处理完成前被关闭取消时返回 `None`；关闭拒绝尚未完成的入队时抛出 `CoreClosedError`。

前台 handler 的错误保留在 `error_stack`。后台 handler 和另外发布的后续事件不在等待范围内。超时或取消会移除临时监听，已经入队的事件及其处理器继续执行；未完成入队的取消不会发布事件。

## 关闭时的等待结果

| 情况 | 等待结果 |
| --- | --- |
| 正常收集完成 | 单个事件或按请求模式索引的字典 |
| 固定期限超时已使等待结束 | `TimeoutError` |
| 匹配的真实事件取消 | 通过期限检查与 predicate 后返回 `None` |
| core 关闭直接结束尚未完成的等待 | `None`，不调用 predicate |
| 主动 `close()` 或取消未完成的读取 | `CancelledError` |

关闭直接完成与超时以先使等待结束的路径为准。即使期限已经到达，只要超时回调尚未执行且结果仍未完成，关闭也可能先使其返回 `None`；已经完成的超时、取消或结果不会被改写。多模式部分收集在关闭时统一返回 `None`。

core 关闭会在汇合业务清理前结束剩余等待器。等待返回 `None` 时，业务 `on_cancelled` 可能仍在执行，不能据此判断 core 已经关闭；需要确认清理完成时应等待应用入口的 `close_core()`。详细用法见[生命周期](../lifecycle.md#关闭)。

## 运行状态

```python
from dataclasses import asdict
from apixis import get_event_runtime_status

status = get_event_runtime_status()
print(asdict(status))
```

同步查询返回不可变 `EventRuntimeStatus`，不创建或启动 core。未创建时 `initialized=False`、`state=None`、`event_loop=None`；关闭实例在下次 create 前仍可查询。

| 字段 | 含义 |
| --- | --- |
| `initialized` / `running` | 是否已创建，以及是否处于 `CoreState.RUNNING` |
| `state` | `CoreState`，未创建时为 `None` |
| `event_queue_size` / `event_queue_capacity` | 当前队列长度和容量 |
| `wait_listeners` | 临时等待监听数，每个不同请求模式占一个监听 |
| `event_loop` | 不可变 `EventLoopStatus`，包含消费者状态、许可容量/占用、派发任务数、待派发事件标记和后台任务数 |

任务数包含挂起的任务，以及完成回调尚未清理的任务。挂起的 handler 可以借出许可，因此任务数不等于许可占用。快照不会随运行状态变化而更新。

[事件处理器](./handlers.md) · [本地事件队列](./channels.md) · [接口概览](../README.md)
