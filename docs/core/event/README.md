# 事件 API

事件 API 负责创建、发布、路由和处理 `ApixEvent`。常用接口可直接从 `apixis` 导入。

## 共享事件系统

完整契约见[五阶段生命周期](../runtime-lifecycle.md)：**create → start → running → stop → close**。

- `create_core() -> ApixisCore`：同步创建组件，不启动消费；已有未关闭实例时返回同一实例。
- `await start_core()`：启动或恢复已有 core 的队列消费，不创建组件。
- `await stop_core()`：只停止队列消费，已安排的 handler chain 和后台 handler 继续执行；组件、队列、注册和等待器保留。
- `await close_core()`：立即永久禁入并取消运行任务；队列与 `_pending_dispatch` 直接删除，运行中的链沿既有 `on_cancelled` 清理，剩余等待器完成为 `None` 后汇合任务。

启动/停止可反复切换。close 完成后旧实例不能启动，需要显式再次 create，重新注册应用处理器和图。

`get_core()`、`get_event_registry()`、`get_event_pipe()`、`get_handler_registry()`、`get_event_loop()` 都只同步读取，不创建、不启动、不恢复。创建前调用 getter 或 start 抛出 `RuntimeError`。异步 getter 已移除。

```python
from apixis import create_core, start_core, close_core, get_event_pipe

create_core()
pipe = get_event_pipe()
try:
    await start_core()
    # 注册/发布与图执行使用这组共享组件。
finally:
    await close_core()
```

core 使用 `CoreState` 状态机，运行快照包含 state。关闭进行中禁止创建替代实例，避免清理回调意外操作新 core。

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
    error_stack: list[ApixEventError],
    cancelled: bool
)
```

`accepted` 和 `cancelled` 默认为 `False`，`seen` 和 `error_stack` 各自默认为独立的空列表。

主要字段：

| 字段 | 说明 |
| --- | --- |
| `event_id` | 事件 ID |
| `event_type` | `EventType` |
| `event_name` | 用于 handler 匹配的事件名 |
| `context` | 用户上下文 |
| `timestamp` | Unix timestamp |
| `accepted` | 是否已被 accept |
| `cancelled` | 是否已请求取消，或已收到取消通知 |
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
from apixis import EventType, create_core, get_event_pipe, post_event, start_core

create_core()
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

先显式 create，再注册订阅。注册和 getter 都不会启动消费。

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

两个观察点在正常完成时都返回原始可变事件。`received` 的结果可能在后续 handler 中继续变化；`processed` 表示前台链已走到该观察点，后台工作仍可能继续。事件被接受或发生前台错误时，适用的 processed 监听仍能得到事件；派发取消通知使尚未完成的等待返回 `None`。

### 等待器生命周期

`EventWaiter` 构造时同步注册监听，整个生命周期绑定一个 Future。完成、超时、取消和上下文退出都会注销临时 handler 并取消计时器；即使没有任务正在读取结果，Future 到达终态也会清理资源。

```python
from apixis import EventWaiter

async with EventWaiter(["job.finished", "job.saved"], time_out=40) as waiter:
    events = await waiter.wait()

assert await waiter.get_result() is events
```

`wait()` 和 `get_result()` 是同一个读取接口，均无参数。重复读取保留同一 Future 的结果、异常或取消状态，不重新注册监听，也不延长期限。上下文仅能进入一次。

`active` 只读属性直接判断 Future 是否尚未完成。框架对活跃 waiter 使用 `finish()`，执行 `_future.set_result(None)` 后调用 `close()`；调用方须先检查 active。registry/cache 通过下层生命周期协议记录 waiter，不依赖上层具体实现。

超时抛出 `TimeoutError`。匹配事件取消或 core 关闭的通知使等待返回 `None`，单 pattern 和多 pattern 一致，不返回处理一半的事件或部分结果。取消任意未完成的读取会取消共享 Future，其他读取也会收到取消；`close()` 同步、幂等地取消未完成 Future 并释放资源。已完成的结果或异常保持可读。predicate 必须是同步 callable，非法参数在注册前报错。

handler 内等待时会暂时归还派发许可，使其他事件可以推进；监听清理先于许可恢复。等待器不提供延迟超时续等或重设期限的能力。

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

`post_and_wait()` 在发布前安装 processed 监听，使用本次生成的 `event_id` 关联结果，因此同名并发事件不会串结果。支持与 `post_event()` 相同的 `event_type`、`event_name`、`context` 参数，额外的 `time_out` 覆盖队列入队和前台处理，正常完成返回本次发布的事件，事件处理取消返回 `None`。

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
| `initialized` / `running` | 是否已构建，以及 core 是否处于 RUNNING |
| `state` | `CoreState`；未创建时为 None |
| `event_queue_size` / `event_queue_capacity` | 当前队列长度和容量 |
| `wait_listeners` | 临时等待监听数，每个请求模式占一个监听 |
| `event_loop` | 不可变 `EventLoopStatus`，包含消费者状态、许可容量/占用、派发任务数、待派发事件标记和后台任务数 |

任务数包含已挂起的任务及回调尚未清理的已完成任务。挂起的 handler 可以借出许可，因此任务数与许可占用不同。快照不会随着后续运行状态变化而改变。

## 模块职责

`factory.py` 管理共享实例引用；`core.py` 中的 `ApixisCore` 协调创建、启动、停止和关闭；`event_pipe.py` 管理本地队列；`event_loop.py` 管理消费和派发。`base/` 定义事件、handler、许可上下文和状态快照；`handler_registry/` 管理订阅匹配、排序和独立等待缓存；`subscription/` 分别提供发布、订阅和等待入口。graph 使用同一组共享事件组件。

### 注册与注销边界

registry 只有普通 handler 和 waiter 两类注册对象，复用内部公共步骤：

| 对象 | 注册 | 注销 |
| --- | --- | --- |
| 普通 handler | `register_handler(handler, exist_ok=False)` | `unregister_handler(name, missing_ok=False)` |
| waiter 及其全部临时 handler | `register_waiter(waiter, handlers, point=...)` | `unregister_waiter(waiter)` |

`register_handler()` 只处理 numeric priority / between_handlers 排序，不接受 point。`register_waiter()` 统一处理 Future 所属 waiter 和全部临时 handler；`received` / `processed` 只属于 waiter 的观察时机，不增加独立的 listener 注册类别。图中断通过 `post_and_wait()` 使用 processed waiter，不注册兜底 handler。

命名按职责区分：`register/unregister` 是完整注册事务，`prepare` 负责校验和规范化输入，`add/replace ... record` 只维护共享登记信息，`index/unindex` 只维护匹配索引。

```text
subscribe() / ApixEventHandler.register()
└─ register_handler()
   ├─ _prepare_handler_registration()
   ├─ _replace_handler_record()
   │  ├─ unregister_handler()        清理旧登记
   │  └─ _add_handler_record()        名称、分组、注册顺序
   ├─ 更新数字优先级桶
   ├─ _index_handler_patterns()
   └─ _invalidate_matching_chains()

EventWaiter.__init__()
├─ _build_handlers()                  只构造回调
└─ register_waiter()
   └─ _add_waiter_handler()           每个请求模式一次
      ├─ _add_handler_record()
      └─ _waiter_cache.index_handler()
```

`unregister_handler()` 根据实际登记位置调用 `_unindex_handler_patterns()` 或 `_waiter_cache.unindex_handler()`，并移除分组和归属关系。`unregister_waiter()` 逐一注销仍属于该 waiter 的临时 handler，再释放 waiter 记录。等待回调不进入业务 `event.seen`，也不修改事件的业务错误状态。

`EventWaiter` 负责构造回调、Future、筛选和计时器；registry 负责一次性登记 waiter 及全部 handler、匹配缓存和组索引，失败时回滚本次安装项。Future 结束后，waiter 仅请求一次整体注销，不再逐个管理 handler 的注册状态。按名称或组注销 handler 时也会更新归属关系，避免旧 waiter 误删同名替代 handler；`clear()` 清空路由后仍保留活跃 Future 的关闭追踪。注销接口只移除注册，不改变 Future 的终态。

[事件处理器](./handlers.md) · [事件队列](./channels.md) · [图 API](../graph/README.md) · [文档首页](../../README.md)


## 关闭时的等待结果

真实事件取消通过 waiter 的 `on_cancelled` / `_cancel` 按 predicate 匹配，保留 event_id 请求关联，随后调用 `finish()` 返回 `None`。core 关闭则直接遍历仍活跃的 waiter 并调用 `finish()`，不产生补充事件、不调用等待监听回调或 predicate，也不使用空 event_id 的隐式协议。两者均不把处理一半的业务事件交给等待方，多 pattern 的部分收集也统一返回 `None`。`wait()`、`get_result()`、`wait_event()`、`await_for()` 和 `post_and_wait()` 的返回类型均包含 `None`。原有 `_resolve` 保持正常解析语义。业务 `EventWaiter.close()` 仍取消 Future；固定期限到达仍抛出 TimeoutError。

在途事件取消、未派发事件丢弃、任务取消隔离及重建约束见[生命周期](../runtime-lifecycle.md)。
