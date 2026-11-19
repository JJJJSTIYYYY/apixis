# 事件 API

事件 API 负责创建、发布、路由和处理 `ApixEvent`。常用接口可直接从 `apixis` 导入。

## 源码职责

`base`、`handler_registry`、`subscription` 按职责组织为包，包的 `__init__.py` 只导出接口。应用常用接口从 `apixis` 或 `apixis.core.event` 导入。

| 实现位置 | 职责 |
| --- | --- |
| [base/event.py](../../../src/apixis/core/event/base/event.py) | 事件类型、事件数据、错误记录与回调类型 |
| [base/handler.py](../../../src/apixis/core/event/base/handler.py) | handler 执行、错误通知、取消清理与实例注册接口 |
| [base/process.py](../../../src/apixis/core/event/base/process.py) | `HandlerChainToken`、`handler_chain_token_context` 与许可的暂停/恢复 |
| [handler_registry/registry.py](../../../src/apixis/core/event/handler_registry/registry.py) | 订阅索引、优先级、分组与普通 handler chain 缓存 |
| [handler_registry/waiter_cache.py](../../../src/apixis/core/event/handler_registry/waiter_cache.py) | received/processed 临时监听索引及独立缓存 |
| [handler_registry/matching.py](../../../src/apixis/core/event/handler_registry/matching.py) | 普通订阅与临时监听共用的匹配规则 |
| [subscription/subscriber.py](../../../src/apixis/core/event/subscription/subscriber.py) | 线程内订阅、取消订阅、跨线程注册与注册信息查询 |
| [subscription/publication.py](../../../src/apixis/core/event/subscription/publication.py) | 线程内发布与显式跨线程发布入口 |
| [subscription/waiter.py](../../../src/apixis/core/event/subscription/waiter.py) | 主线程与工作线程的等待入口、超时、结果收集、注册就绪与清理确认 |
| [factory.py](../../../src/apixis/core/event/factory.py) | 线程本地核心的创建、启动、停止、访问与状态查询 |
| [base/status.py](../../../src/apixis/core/event/base/status.py) | 不可变的核心、派发与跨线程桥接状态快照 |
| [event_pipe.py](../../../src/apixis/core/event/event_pipe.py) | 事件创建、上下文快照与本地有界队列 |
| [event_loop.py](../../../src/apixis/core/event/event_loop.py) | 消费、捕获处理链、派发与并发容量管理 |
| [thread_bridge.py](../../../src/apixis/core/event/thread_bridge.py) | 进程级命名路由、各目标线程的有界接收队列、取消和关闭协议 |
| [event_registry.py](../../../src/apixis/core/event/event_registry.py) | 已观察事件名的记录与查询 |

临时监听缓存只依赖 handler 类型和共享匹配规则，不依赖 registry 实现；工作线程等待复用异步等待器，跨线程通信仍由桥接层负责。调整执行逻辑时进入所属实现文件，公开入口无需增加转发函数。

跨线程请求通过锁保护的有界队列转交目标 loop；容量不足时由线程安全 Future 通知调用方继续。线程版等待上下文在每个目标上启动一条结果收集任务，`wait()` 在目标 loop 更新期限后复用该任务的结果。上下文退出确认监听清理，已经完成的清理直接返回。

## 线程内事件系统

### `create_core(thread_id: str)`

为当前 OS 线程创建并注册唯一命名的 core，返回 `EventCore`。每个线程独立持有 event registry、handler registry、pipe、dispatcher 和派发许可。重复指定同一个 ID 返回同一个 core；当前线程改用另一个 ID、或 ID 已被其他活跃线程占用时抛出 `ValueError`。

创建不会启动消费者，也不会创建 asyncio loop。主线程 getter 首次访问可自动创建 ID 为 `"main"` 的 core，支持导入期间的同步装饰器。工作线程先显式调用 `create_core("worker")`。随后在当前线程自己的 asyncio loop 中调用 `await start_core()`，完成启动后该 core 才能接收跨线程请求。

```python
from apixis import create_core, start_core, subscribe

create_core("worker")

@subscribe("job.*")
async def observe(event):
    ...

# 在 worker 自己的 asyncio loop 中：
await start_core()
```

### `await start_core()`

启动当前线程的 core，并将其接收端绑定至当前 asyncio loop。同步 getter 在运行中的 loop 内也会安排自动启动；需要确认跨线程可访问时，显式等待 `start_core()`。

同一线程的重复访问复用组件。core 一旦绑定 loop，改用另一个 loop 调用 getter、启动或停止都会抛出 `RuntimeError`。线程归属保存在 `threading.local`，不会随 `ContextVar` 或 `asyncio.to_thread()` 复制到其他线程。factory 的 getter 不接受远程目标参数。

### `await stop_core()`

暂停当前线程 core，关闭其接收端并释放路由名称。尚未完成的远程请求得到错误并完成清理，等待接收队列容量的调用方也会被唤醒。保留本地组件、订阅和事件队列，可在同一个 loop 重新启动。未创建时停止不会创建 core。

已开始的本地派发及后台处理继续运行；停止不会等待这些业务任务。应用在线程退出前应完成业务清理并调用 `stop_core()`。停止和启动使用当前 core 自己的生命周期锁。

### 同步 getters

- `get_event_registry()` → `ApixEventRegistry`
- `get_event_pipe()` → `ApixEventPipe`
- `get_handler_registry()` → `ApixHandlerRegistry`
- `get_event_loop()` → `ApixEventLoop`

同步 getter 会立即返回当前线程组件；处于运行中的 asyncio loop 时会触发启动，但**不保证启动完成**。

### 异步 getters

- `await aget_event_registry()`
- `await aget_event_pipe()`
- `await aget_handler_registry()`
- `await aget_event_loop()`

异步 getter 以 `await` 方式唤起并返回当前线程的组件，不等待已发布事件处理完成。

### 运行状态查询

`get_event_runtime_status() -> EventRuntimeStatus` 同步返回当前线程的只读快照，不创建核心、不触发启动，也不等待任务完成。未注册的线程返回 `initialized=False`。快照只包含状态值，不暴露任务、事件或可变集合；可用 `dataclasses.asdict()` 转成字典。

```python
from dataclasses import asdict
from apixis import get_event_runtime_status

status = get_event_runtime_status()
print(asdict(status))
if status.event_loop is not None:
    print(status.event_queue_size, status.event_loop.dispatch_permits_in_use)
```

| 位置 | 字段与含义 |
| --- | --- |
| 顶层 | `initialized`：核心是否已构建；`running`：本地 pipe 与消费者是否已启动。 |
| 顶层 | `event_queue_size` / `event_queue_capacity`：尚未交接的队列积压及容量；`wait_listeners`：临时监听数，多模式等待按模式计数。 |
| `event_loop` | `running`：消费者是否已启动；`dispatch_capacity`：派发容量，`None` 为无限制；`dispatch_permits_in_use`：当前已占用或已为唤醒方预留的许可。 |
| `event_loop` | `dispatch_tasks`：仍在跟踪的前台任务；`pending_dispatch`：是否有已出队但尚未派发的事件；`background_tasks`：仍在跟踪的后台任务。 |
| `thread_bridge` | `running`：桥接是否开放且所属 loop 正在运行；`queue_size` / `queue_capacity`：待执行请求及队列容量。 |
| `thread_bridge` | `admission_waiters`：等待桥接队列空位的提交；`active_requests`：已开始执行、尚未完成清理回调的请求。 |

未构建核心时，`initialized=False`、计数为零，`event_queue_capacity`、`event_loop`、`thread_bridge` 为 `None`。已构建但尚未启动桥接时，只有 `thread_bridge` 仍为 `None`。`EventLoopStatus`、`EventThreadBridgeStatus` 和 `EventRuntimeStatus` 均可从 `apixis` 导入。

任务计数包含挂起等待以及已结束但清理回调尚未运行的任务，不等于正在执行的 handler 数。等待结果的处理链借出许可后，`dispatch_tasks` 仍可大于 `dispatch_permits_in_use`。暂停核心不会清空已有任务和队列，快照会如实保留这些数量。查询不缓存状态、不遍历任务集合，也不在事件热路径增加统计回调。

## `EventType`

```python
from apixis import EventType
```

可选值：

- `EventType.DEFAULT`
- `EventType.TRANSIENT`
- `EventType.WORKFLOW`
- `EventType.LIFECYCLE`
- `EventType.INFO`
- `EventType.WARNING`
- `EventType.ERROR`

`TRANSIENT`（值为 `"transient"`）表示临时事件：事件正常分发给订阅的处理器，但其名称不会被 `ApixEventRegistry` 记录。

`DEFAULT`（值为 `"default"`）用于未指定类别的普通事件，正常分发并记录事件名。

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

使用本地 `post_event()` 发布事件：

```python
from apixis import EventType, post_event

await post_event(
    event_type=EventType.INFO,
    event_name="job.created",
    context={"job_id": 42},
)
```

`post_event()` 委托当前线程的 `ApixEventPipe.post_event()` 创建 `ApixEvent` 并放入本地队列；本地队列满时等待空位。当前线程的事件核心会按需启动，也可以通过 `await start_core()` 显式等待启动。需要直接访问管道时，使用 `pipe = get_event_pipe()`。

所有发布接口的 `event_type` 均默认为 `EventType.DEFAULT`：`post_event()`、`ApixEventPipe.post_event()`、`post_event_threadsafe()`、`post_and_wait()` 和 `post_and_wait_threadsafe()`。可省略该参数，也可显式指定其他事件类别：

```python
await post_event(event_name="job.created", context={"job_id": 42})
```

发布时捕获当前 `ContextVar` 绑定，整条前台 handler chain 在该快照中执行，后台 handler 继承启动时的绑定。发布者之后的 `set/reset` 不影响已发布事件，handler 的绑定修改也不回写发布者；变量值中的可变对象仍按引用共享。直接使用 `put()` / `put_nowait()` 注入手工构造的事件不捕获发布者上下文。

`pipe.join()` 等待队列项目完成交接，不等待 handler 执行结束。需要等待结果事件时，可使用下文的 `wait_event()` 或 `expect_event()`；也可以由处理器通过应用自己的结果对象或信号通知调用方，完整示例见[快速开始](../../quickstart.md)。

### 发布并等待处理结束：`post_and_wait`

```python
from apixis import EventType, post_and_wait

event = await post_and_wait(
    event_type=EventType.INFO,
    event_name="job.created",
    context={"job_id": 42},
    time_out=30,
)
if event.has_error:
    print(event.error_stack)
```

`post_and_wait(*, event_type=EventType.DEFAULT, event_name, context=None, time_out=None, keep_listening=False)` 先注册完成监听，再发布事件，通过本次事件的 **`event_id`** 精确匹配结果。同名事件并发发布时，各自返回自己的原始 `ApixEvent`，可以读取 handler 对 `context` 的修改及 `accepted`、`error_stack` 等处理状态。没有普通订阅者时同样正常返回。

完成位置与 `point="processed"` 一致：等待该事件的前台处理链结束，后台 handler 以及 handler 发布的后续事件独立继续执行。需要业务回包时，仍通过 `expect_event()` 订阅对应的结果事件。

`time_out` 在当前线程开始执行时计时，覆盖入队和处理过程，省略或传入 `None` 表示无限等待。默认超时抛出 `TimeoutError` 并清理完成监听；`keep_listening=True` 时，已入队事件的处理超时会返回仍在监听的 `EventWaiter`，完成时仍返回事件。使用 `await waiter.get_result()` 或再次 `await waiter.wait()` 继续等待，放弃时调用 `waiter.close()`：

```python
from apixis import EventWaiter, post_and_wait

result = await post_and_wait(event_name="job.created", time_out=1, keep_listening=True)
if isinstance(result, EventWaiter):
    result = await result.get_result()
```

入队超时表示事件尚未发布，无论 `keep_listening` 如何设置都会抛出 `TimeoutError` 并清理监听。取消始终清理监听；已入队的事件继续处理。在 handler 内调用时，发布和等待期间自动借出当前线程的调度许可。工作线程中的普通调用同样操作本地 core，支持 `keep_listening=True`。跨线程使用 `post_and_wait_threadsafe()`，不返回远程 waiter。

## 跨线程发布与等待

普通接口始终操作当前线程，`*_threadsafe` 才通过进程级 `ThreadBridge` 转发到目标 core：

| 操作 | 当前线程 | 显式跨线程 |
| --- | --- | --- |
| 同步订阅 | `@subscribe(...)` 或 `subscribe(...)(handler)` | `await subscribe_threadsafe(..., thread_ids=...)(handler)` |
| 移除订阅 | `unsubscribe(name)` | `await unsubscribe_threadsafe(name, thread_ids=...)` |
| 发布 | `await post_event(...)` | `await post_event_threadsafe(..., thread_ids=...)` |
| 发布并等待处理 | `await post_and_wait(...)` | `await post_and_wait_threadsafe(..., thread_ids=...)` |
| 等待事件 | `await wait_event(...)` | `await wait_event_threadsafe(..., thread_ids=...)` |
| 先注册监听再执行操作 | `async with expect_event(...)` | `async with expect_event_threadsafe(..., thread_ids=...)` |

`thread_ids=None` 默认选择主线程已注册的 core，即使主线程显式使用了其他名称。字符串选择一个 core；非空 `list`、`tuple` 或 `set` 选择多个 core，重复 ID 合并。目标必须已完成 `start_core()`。无效 ID、未启动或已停止的目标抛出错误；解析全部目标后才开始提交请求，不会自动向其他线程广播，也不会将收到的事件再次转发。

单个目标的等待和 `post_and_wait_threadsafe()` 返回事件；事件模式为集合时返回按模式索引的事件字典。多个目标始终返回按 thread ID 索引的结果字典，包括只有一个 ID 的集合；多目标等待要收齐所有目标。每个目标的发布都创建独立事件 ID；payload 仍按引用共享，修改约束见[共享 payload 与修改归属](#共享-payload-与修改归属)。

```python
from apixis import expect_event_threadsafe, post_event_threadsafe

# 先在两个已运行的 core 上确认监听，再发布。
async with expect_event_threadsafe(
    "job.reply", point="processed", thread_ids=["worker-a", "worker-b"],
) as waiter:
    await post_event_threadsafe(
        event_name="job.request", context={"value": 21},
        thread_ids=["worker-a", "worker-b"],
    )
    replies = await waiter.wait(time_out=30)
    print(replies["worker-a"].context, replies["worker-b"].context)
```

订阅的所有排序、过滤和分组选项与本地接口一致。回调在目标线程执行。传入函数时返回原函数；传入 `ApixEventHandler` 时各目标使用独立浅拷贝，隔离注册元数据、保留 callback 引用。多目标订阅返回按 thread ID 索引的注册对象。移除可以按名称或 `group`，等待返回确认全部目标已执行。

```python
from apixis import subscribe_threadsafe, unsubscribe_threadsafe

registered = await subscribe_threadsafe(
    "job.*", group="observers", thread_ids=["worker-a", "worker-b"],
)(observe)
await unsubscribe_threadsafe(group="observers", thread_ids=["worker-a", "worker-b"])
```

`subscribe_threadsafe()` 返回异步注册装饰器，必须显式等待应用，不能直接用作 `@` 装饰器。`subscribe()` 和 `handler.register()` 在任意已初始化线程均同步生效。

`expect_event_threadsafe()` 进入完成表示所有目标的监听已注册；提前到达的结果会保留。`wait_event_threadsafe()` 从协程实际运行时提交监听，`create_task()` 本身不保证注册完成。`predicate` 在每个目标的 loop 同步执行。`received` 在普通前台 handler 之前收集，`processed` 在之后收集，不等待后台处理。

`time_out` 从各目标开始注册计时，不包含桥接队列准入等待。`waiter.wait(time_out=...)` 重设各目标的期限，省略或传 `None` 清除期限；`get_result()` 沿用现有期限。跨线程不返回保留监听的远程 `EventWaiter`；需要 `keep_listening=True` 时在所属线程使用普通等待接口。

每个目标拥有独立的有界桥接队列，容量与其本地 `event_pipe_max_len` 相同。队列满时异步等待容量通知，不占用线程池，也不阻塞调用方 loop。转交发布串行执行，等待结果的请求独立收集。多目标调用并发提交，任一目标失败、超时或调用方取消时，取消并等待其他未完成请求清理。已生效的注册、移除和已入队事件不会回滚；多目标操作不提供跨线程事务。

线程安全调用借出调用方线程自己的派发许可，目标使用目标线程的许可。`ContextVar` 业务绑定会传递，`HandlerChainToken` 不传递；即使 `asyncio.to_thread()` 复制了调用上下文，其他 loop 的 `suspend_process()` 也不会释放源线程许可。没有处理链的调用方不操作许可。

直接 `await asyncio.to_thread(...)` 不会自动借出来源处理链的许可。若工作线程反向请求来源 core，并等待该 core 处理事件，来源 core 的许可耗尽时会形成循环等待。需要在来源线程用 `suspend_process()` 包住这段等待，详见下文“`asyncio.to_thread()` 与反向请求”。

线程桥接器通过 `get_thread_bridge()` 获取；它是唯一的进程级通信目录，普通 getter 和本地事件操作不访问目录锁。`ThreadBridge.call()` / `call_sync()` 可将明确的操作提交到指定 core；生命周期由各所属线程管理。

`call()` 默认使用 `background=True`，操作开始后允许接收端继续转交后续请求，因此操作中可以再次请求同一 core。这里的 `background` 控制桥接请求的执行方式，与 handler 的后台属性无关。显式使用 `background=False` 时，接收端会等待操作结束再转交下一条请求；该操作不能等待后续桥接请求或事件 handler 完成，否则可能阻塞自身依赖。普通 `post_event_threadsafe()` 只等待事件入队，继续使用串行转交来限制发布 task 的数量；`call_sync()` 也串行执行同步回调，回调必须保持非阻塞。

容量为 1 的回路、多目标清理和压力测试见[测试说明](../../../tests/README.md#跨线程事件验证)。

### 共享 payload 与修改归属

标准发布接口创建的同一事件，其前台和后台 handler 都在目标 core 的 loop 中运行；跨线程发布到多个 core 时，各目标创建独立事件，但 `context` 不会被自动复制。**不同 core 同时修改同一份可变 payload 会产生数据竞争**，即使它们处理的是不同事件、修改都发生在各自线程的 handler 中，也不能保证安全。例如，多个 core 对同一字典执行 `event.context["count"] += 1`，仍可能丢失更新。

业务数据的使用约定：

- 每份可变 payload 明确由一个 core 负责修改，发布者和其他 core 不与其并发写入。跨线程发起修改时，先通过 `subscribe_threadsafe()` 将 handler 注册到数据所属 core，再向该 core 发布请求；本线程可直接使用 `subscribe()`。
- 多个 core 需要分别修改数据时，由调用方按目标构造独立可变状态并分别发布；多目标发布本身不会隔离 payload 及其可变子对象。
- 确需多个 core 共享写入时，由业务对象或所属服务提供跨线程同步。`suspend_process()` 只协调框架派发许可，不保护 payload，也不释放业务锁；资源依赖约束见[死锁保证边界](#框架与应用的死锁保证边界)。

等待接口仅用于观测，修改由 handler 完成。等待结果仍是原始事件引用，不是不可变快照；`received` 可以观察处理中状态，`processed` 只表示前台处理链完成，不等待后台 handler 或其他 core 对共享对象的操作。需要一致性快照时，应由数据所属 core 的 handler 生成独立结果。

同样的归属约束适用于回调闭包、绑定的服务实例，以及 `ContextVar` 值中的可变对象。`subscribe_threadsafe()` 对 handler 实例的浅拷贝只隔离注册元数据，回调及其业务对象仍可能共享。这些约束在启用 GIL 时也成立；关闭 GIL 后，各 core 可以真正并行执行，框架不提供共享业务数据的跨 core 事务。

### 发布与监听目标

普通事件接口使用调用线程的 core。能否收到事件取决于**发布目标和注册监听的 core**。以下示例假设 A、B、C 是三个工作线程，监听已在发布前注册，名称和谓词均匹配。

| 发布方式 | 等待调用位置和方式 | 是否收到 |
| --- | --- | --- |
| A 中 `post_event(...)`，进入 A | B 中 `expect_event_threadsafe(..., thread_ids="A")` | 是 |
| A 中 `post_event_threadsafe(..., thread_ids="B")`，进入 B | B 中 `expect_event(...)` | 是 |
| 同上 | B 中 `expect_event_threadsafe(..., thread_ids="B")` | 是 |
| 同上 | B 中 `expect_event_threadsafe(..., thread_ids="A")` | 否 |
| 同上 | B 中 `expect_event_threadsafe(..., thread_ids="C")` | 否 |

`thread_ids` 省略或传 `None` 始终选择主线程 core。等待器只匹配已选目标中的事件，直到匹配、超时或取消。`received` 和 `processed` 使用相同的目标选择规则，通知时点分别位于目标前台处理的前后。

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

所有事件均通过 `ApixEventPipe` 内部的本地有界队列处理，详见[事件队列](./channels.md)。

## 订阅

```python
from apixis import subscribe


@subscribe("job.*")
async def observe_job(event):
    ...
```

支持 glob 风格模式。详细排序、错误语义与取消语义见 [Event handlers](./handlers.md)。

## 等待事件：`wait_event` 与 `expect_event`

`wait_for_event` 是 `expect_event` 的别名，`await_for` 是 `wait_event` 的别名；旧名称保留相同的签名与行为。

等待监听器使用独立的精确事件索引与有界缓存，增删监听器不会使普通订阅的 handler_chain 缓存失效。事件出队时仅生成一份 handler 引用快照，按 `received` 监听器、普通订阅、`processed` 监听器的顺序执行；同一等待位置按注册顺序执行，不受普通订阅的数值优先级影响。

`get_subscription_chain_for_event()` 查询普通订阅的缓存名称链；`get_handlers_chain_for_event()` 查询包含等待监听器的完整名称链，`get_handlers_for_event()` 返回完整派发引用快照。

先看等待之前，当前代码是否还需要发布请求：

| 你要做的事 | 应该使用 | 执行顺序 |
| --- | --- | --- |
| 立即等待其他任务接下来产生的事件 | `await wait_event(...)` | 注册 → 等待 → 返回事件 |
| 先订阅回包，再由当前代码发布请求 | `async with expect_event(...) as waiter` | 注册 → 发布请求 → `await waiter.wait()` |

**两个接口都能等待一个或多个事件。区别在于是否需要在“注册”和“等待”之间执行自己的代码。**

等待器可通过注册时的 `predicate` 按事件内容进一步筛选。`post_and_wait()` 使用 `predicate=lambda event: event.event_id == event_id` 关联本次发布与处理结果。

### 正反例：发布请求并等待回包

假设 `job.start` 的处理器会向 `context["reply"]` 指定的事件名发布结果。以下代码在 async 函数中执行，`reply` 是本次请求独有的回包名。

**正例：使用 `expect_event`，先订阅，再发布。**

```python
async with expect_event(reply) as waiter:
    await pipe.post_event(
        event_name="job.start",
        context={"reply": reply},
    )
    event = await waiter.wait(time_out=40)
```

进入上下文时订阅已注册；即使回包在 `.wait()` 前到达，也会保留结果。

**反例：先发布，再使用 `wait_event` 订阅。**

```python
await pipe.post_event(
    event_name="job.start",
    context={"reply": reply},
)
event = await wait_event(reply, time_out=40)
```

回包可能在 `wait_event` 注册前就已出队，导致错过结果并超时。两个接口都不回放注册前已出队的事件；需要自己发起请求时，应使用上面的正例。

### 同时等待多个事件

接口的选择仍取决于是否需要先注册再发布，与事件数量无关：

```python
# 事件由其他任务独立产生：直接等到两个事件都收到。
events = await wait_event(["job.validated", "job.saved"], time_out=40)

# 当前代码需要发起任务：先注册两个订阅，再发布请求。
async with expect_event(
    ["job.validated", "job.saved"], point="processed",
) as waiter:
    await get_event_pipe().post_event(
        event_name="job.start",
    )
    events = await waiter.wait(time_out=40)
    validated = events["job.validated"]
    saved = events["job.saved"]
```

传入字符串时，`wait_event()` 和 `.wait()` 返回单个 `ApixEvent`；传入列表或集合时，收齐所有模式的事件后返回以**订阅模式**为键的字典，即使集合中只有一个模式也是如此。模式不能为空，重复模式合并，字典不保证键顺序。每个模式只保留首个匹配，后续事件不覆盖；重叠模式可以由同一个事件满足。在上下文内重复调用 `.wait()` 返回同一个事件或字典。

### 按业务条件匹配

`EventWaiter`、`expect_event()`、`wait_event()` 及对应线程等待接口接受 `predicate: EventPredicate | None = None`；`EventPredicate` 的类型为 `Callable[[ApixEvent], bool]`。`None` 不增加内容筛选，谓词返回假值时忽略当前事件，保留监听与已有的部分结果。

```python
request_id = "request-42"
async with expect_event(
    "job.reply",
    point="processed",
    predicate=lambda event: event.context["request_id"] == request_id,
) as waiter:
    await post_event(event_name="job.request", context={"request_id": request_id})
    reply = await waiter.wait(time_out=30)
```

事件名与 `filter` 先完成筛选，谓词随后在所选 `point` 执行，所以 `processed` 的谓词可以读取前台 handler 更新后的内容。谓词在监听所属 core 的 loop 同步执行，必须快速返回，不支持异步函数。重叠模式可能对同一事件多次调用谓词，建议使用无副作用的判断。

谓词抛出的普通异常会结束对应等待并注销全部监听，等待方收到原异常；事件本身和其他监听继续处理，不写入该事件的 `error_stack`。谓词不改变原有截止时间、延迟收集及取消规则。等待器仍为一次收集，不支持完成后重新注册复用。

### 共同参数与超时

```text
event_name: str | list[str] | set[str]
point: Literal["received", "processed"] = "received"
filter: list[str] | str | None = None
predicate: EventPredicate | None = None
time_out: float | None = None
keep_listening: bool = False
```

`event_name` 支持大小写敏感的 glob 模式；`filter` 排除匹配的事件名。`point="received"` 在普通前台 handler 之前取得事件；`point="processed"` 在普通前台 handler 完成后取得事件，不等待后台 handler。事件被 accept、发生前台错误或事件分发被取消时，适用的通知回调同样可以返回该事件；调用方可检查 `accepted`、`has_error` 和 `error_stack`。返回的是原始、可变的事件对象，不是内容快照，后续 handler 仍可能修改它。

`time_out=None` 表示没有截止时间，非正值表示立即超时。`wait_event(time_out=...)` 限制这次等待；`expect_event(time_out=...)` 从注册完成开始计时，包括调用 `.wait()` 前的工作。每次 `.wait(time_out=...)` 都会替换当前截止时间，从本次调用计时；省略或传入 `None` 则取消尚未到期的截止时间，无限等待。已经超时关闭的监听不会恢复。所有等待项都需要在同一截止时间前到达所选 `point`，只收到部分事件也会超时，不返回部分结果。

**截止时间不会中断 `expect_event` 的上下文主体**：默认超时使等待结果失效，随后 `.wait()` 抛出 `TimeoutError`；若所有结果已在截止时间前收齐，之后调用 `.wait()` 仍能取到。如需限制整个上下文主体，可在外层使用 `asyncio.timeout()`。

超时抛出 `TimeoutError`，取消或退出上下文会清理尚未完成的等待。监听由所属 core 的 `EventWaiter` 管理，收齐结果或普通超时后立即注销，不必等到上下文退出。已有完整结果时，退出上下文后再次调用 `wait()` 或 `get_result()` 会直接返回保留的结果，不因上下文已退出而抛错，也不会重新注册监听或设置期限。字符串模式返回同一事件对象，模式集合返回保留的结果字典。

普通接口的 `EventWaiter` 和跨线程接口的 `ThreadEventWaiter` 均支持退出后读取完整结果；跨线程已完成结果的读取不再访问目标 core，目标停止后仍可读取。未完成就退出的等待仍会关闭，不返回部分结果；原有超时或谓词异常仍会传播。其他跨线程等待语义见[跨线程发布与等待](#跨线程发布与等待)。

多模式等待在收齐所有结果后统一注销全部监听；部分结果到达时保留监听与路由缓存。每个模式只保留首次结果，同一事件可以同时满足重叠模式。无效的 `point`、模式和超时参数在注册前拒绝；超时必须是有限数字，布尔值不属于数字。

在 handler 内，两种接口都在实际等待结果时通过 `suspend_process()` 借出当前处理链的调度许可，结束等待时恢复。`expect_event` 的上下文进入本身不借出许可；上下文主体中的 `pipe.post_event()` 继续使用其自身的许可借出逻辑。

### 超时后继续收集结果

默认超时会结束监听。任意本地 core 都支持设置 `keep_listening=True`，超时返回 `EventWaiter`，后续通过 `await waiter.get_result()` 等待完整结果；跨线程不支持此选项：

```python
from apixis import EventWaiter, wait_event

result = await wait_event("job.finished", time_out=1, keep_listening=True)
if isinstance(result, EventWaiter):
    # 首次等待已超时，监听仍在；这里可以先处理其他工作。
    result = await result.get_result()
```

请求／回包场景仍用原来的 `async with expect_event(...)`；将等待改为 `await waiter.wait(time_out=1, keep_listening=True)` 即可。每次 `.wait()` 都只使用本次传入的等待时长：数字表示从本次调用计时，省略或传入 `None` 均无限等待，首次调用也相同。`keep_listening` 省略时沿用等待器配置。同一等待器的并发等待共享这个预算与策略。已有结果会直接返回，只有尚未收齐且超时才返回等待器；已经按默认策略超时关闭的等待器不能恢复，应在超时前启用该选项。

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
- `unsubscribe(group="workers", missing_ok=True)`：通过分组索引批量移除组内 handler，与 name 二选一；
- `get_handler(name)`：返回 handler 或 `None`；
- `get_handler_meta(name)`：读取独立的注册元数据副本，修改返回值不影响订阅、过滤或排序；
- `is_registered(name)`：检查是否注册；
- `get_unmatched_subscriptions(name)`：返回当前尚未观察到匹配事件名的订阅模式。

`ApixEventHandler` 默认 `group="global"`。`subscribe(..., group="workers")` 或 `handler.register(..., group="workers")` 可指定分组；省略 group 或传入 `None` 时保留已有实例的分组。`handler.unregister(same_group=True)` 注销其 group 的全部成员。分组名是区分大小写的非空字符串，handler 名称仍全局唯一。

`ApixHandlerRegistry(event_registry, *, cache_size=8192)` 将精确订阅与 glob 订阅分别索引，并按原有优先级和相对插入顺序组合处理器链。匹配链使用有界 LRU；每个 core 的容量由 `APIXIS.EVENT.handler_chain_cache_size` 设置。淘汰只影响后续查询是否需要重建，不改变已取出事件的处理器引用。

独立 registry 可使用 `unregister_group(group, missing_ok=False)` 按组注销，或使用 `clear()` 同时清理订阅、分组、索引和链缓存；该操作不清空 `ApixEventRegistry` 的观察历史。更新订阅、分组和排序时使用 `handler.register()`、`subscribe()` 或 registry 注册接口，不单独修改内部索引、bucket 或缓存。

## 已观察到的事件名

`ApixEventRegistry` 可查询本地已经观察到的精确事件名；它不保存事件内容，也不能用于查询某个事件是否处理完成。

registry 与本地 handler registry、pipe 一样，只在所属线程访问，不在逐事件记录路径取得线程锁。跨线程查询使用 `get_thread_bridge().call_sync(lambda: get_event_registry().get_registered_events(), thread_ids="worker")`，返回不可变快照。自行跨线程共享独立创建的 registry 时，由调用方同步访问。

```python
from apixis import get_event_registry

registry = get_event_registry()
seen = registry.get_registered_events()  # frozenset[str]
registry.clear()
```

## 背压与挂起

### 背压限制什么

假设每个前台 handler 都要 `await` 一次 HTTP 请求、或者一次数据库查询。如果事件持续到达且分发不受限制，大量请求会同时占用连接和下游服务资源。派发背压限制同时持有调度许可的前台事件处理链数量；普通的 HTTP、数据库调用或 `asyncio.sleep()` 不会自动归还许可，因此这些等待期间仍计入容量。

跨线程请求队列、事件队列和派发许可分别约束三个阶段：

| 配置 | 限制范围 | 容量用完时 |
| --- | --- | --- |
| `APIXIS.EVENT.event_pipe_max_len` | 仍在本地队列中的事件数 | 异步发布等待空位 |
| `APIXIS.EVENT.event_pipe_max_len` | 等待各目标线程转交的跨线程请求数 | 工作线程的协程等待容量通知 |
| `APIXIS.EVENT.event_loop_backpressure` | 每个线程内同时持有许可的前台处理链数 | 新分发或挂起后的恢复等待许可 |

`event_loop_backpressure` 默认为 `null`（Python `None`），此时只维护许可计数，不限制分发并发；设为正整数时启用派发背压。事件队列仍受 `event_pipe_max_len` 限制。配置的有效范围见[配置文档](../config/README.md#队列与并发)。

同一处理链内的并行子任务共享许可，后台 handler 独立运行，挂起等待的 task 仍然存活。派发背压限制的是前台处理链执行并发。整体过载保护还需要在外部请求入口限制接纳数量，并在具体资源所属位置控制并发。

### 挂起为什么要借出许可

handler 等待的工作有时也需要事件循环继续分发。如果等待方一直持有许可，所有许可都被占用时会出现循环等待：

- **等待回复事件**：处理链 A 发布请求后等待回复 B；B 需要新的分发许可才能执行，但许可被 A 等等待方占用。
- **向满队列发布事件**：A 持有许可并等待队列空位；消费者已经取出 B，但正在等分发许可，无法继续取出队列中的事件腾出空位。

挂起会在这类等待期间临时借出当前处理链的许可，让请求、回复或其他排队事件能够继续推进；等待结束后，再按处理链的状态恢复许可。挂起不等于取消或结束 task，普通 `await` 也不等于借出许可。并行子任务共享的许可由内部统一协调，避免重复释放，以及取消时为了恢复许可而阻碍处理链退出。

这也解释了为什么存活的 dispatch task 可以多于派发容量：等待方让出了执行容量，但仍保留继续执行所需的状态。队列中的积压可以转移为挂起的 task，不能仅凭队列有界就认定整体资源有界。

### 在 handler 内发布和等待

`suspend_process()` 是内部许可管理机制，未从 `apixis`、`apixis.core` 或 `apixis.core.event` 导出。业务代码通过公开接口获得相应的挂起与恢复行为：

- 发布后续事件使用 `pipe.post_event()`，它会在发布期间借出当前许可；直接 `pipe.put()` 不提供这层协调。
- 等待事件使用 `wait_event()`、`expect_event()` 或 `EventWaiter.wait()` / `get_result()`，它们在实际等待结果时借出许可。`expect_event` 的上下文进入只注册监听，不借出许可。
- 请求／回包流程使用 `expect_event()`，按“注册监听 → 发布请求 → 等待结果”的顺序执行，避免漏掉提前到达的回包。
- 前台 handler 中使用线程安全接口时，同样协调派发许可；`expect_event_threadsafe()` 的进入需要异步确认注册，因此进入期间也借出许可。调用方只借出自己线程的许可，目标使用自身线程的许可。

如果自行等待应用的 `Future`、`Event` 等信号，且其完成依赖另一个事件 handler，框架无法自动识别这项依赖。应改用上述事件等待接口，或将后续逻辑放到结果事件的订阅处理器中，避免持有许可等待依赖方。

### 框架与应用的死锁保证边界

框架协调的是自己的队列、派发许可和临时监听。以下保证以目标 loop 持续运行、应用回调能够协作让出执行权为前提：

| 路径 | 框架提供的保证 | 应用需要保证 |
| --- | --- | --- |
| 本地发布与事件等待 | 托管处理链在等待期间借出自己的许可，同一处理链的并行分支共享一次释放和恢复 | 使用本文列出的接口处理依赖其他事件派发的等待 |
| A→B→A 往返 | 每次线程安全调用只协调来源 core 的许可，目标使用自己的许可；容量为 1 也可推进 | 业务最终会产生回包，不形成只等待彼此结果、没有生产者的依赖环 |
| 先监听再发布 | `expect_event_threadsafe()` 进入后，所有目标监听均已完成注册 | 选择正确的发布/监听 core，并在注册后发布；历史事件不重放 |
| 满队列、取消和目标停止 | 容量通知不占线程池；取消等待目标监听清理；单个目标失败会清理其他未完成目标 | loop 不被同步 I/O、`thread.join()`、阻塞锁或 `Future.result()` 阻塞；自定义操作不吞掉取消后无限等待 |
| 应用锁、连接池及其他资源 | 借出派发许可，不操作应用资源 | 不跨事件等待持有依赖 handler 也要取得的资源，或由应用自行打破资源依赖环 |
| `asyncio.to_thread()` 反向调用 | 不允许子线程修改来源 core 的许可 | 在来源 loop 明确挂起这段等待；线程函数的退出和取消由应用管理，见下节 |

例如，父 handler 持有 `app_lock` 时等待子事件，而子 handler 也需要 `app_lock`：父虽已借出派发许可，子仍会卡在应用锁上。这是应用资源环，增加队列容量或框架再次释放许可都不能解决，应在发起等待前释放应用资源或调整业务顺序。

超时和取消也不是这类依赖环的通用解法。挂起主体结束（包括超时、取消）后，存活处理链通常先重新取得许可再把结果或异常交给调用方，防止调用方捕获取消后绕过容量继续执行。因此，如果占用许可的依赖 handler 又在等待父处理链持有的应用资源，许可恢复也会被卡住。取消在恢复等待期间到达所属派发 task 可以中断该恢复；框架不自动识别、取消相关业务 handler 来打破环。`time_out` 不保证包含清理和许可恢复的整个接口在该秒数内返回。

取消发布或结果等待只会撤销尚未执行的桥接请求、清理监听，**不会撤回已经入队的事件或取消其业务 handler**；多目标中已经完成的发布也不回滚。`stop_core()` 关闭接收端并暂停消费，同样不等待或取消既有业务派发。应用应自行管理业务任务、外部资源和线程退出，不能把这些操作当作全局业务取消。

### `asyncio.to_thread()` 与反向请求

`asyncio.to_thread()` 会复制调用方的 `Context`，但不会深拷贝其中的值。工作线程可能读到来源 handler 的同一个 `HandlerChainToken`；子线程中重新 `set()` 只改变自己的绑定，而修改 token 对象会影响共享状态。token 中的许可仍由来源 loop 所有，子线程不能直接释放或重新取得它。`suspend_process()` 会忽略不属于当前 loop 的 token，跨线程桥接也会清除 token，而不会接管来源许可。

因此，`await asyncio.to_thread(worker)` 本身仍占用来源处理链的许可。若 `worker` 向来源 core 发布事件并等待处理完成，来源 handler 等待工作线程，工作线程又等待新的派发许可；当来源 core 容量为 `1`，或全部许可都被这类等待占用时，就会死锁。工作线程使用 `*_threadsafe` 也不能自动借出来源 handler 的许可。

对于这类依赖，在来源 handler 所在的 loop 中先借出许可，再等待工作线程。以下示例使用 ID 为 `"main"` 的来源 core：

```python
import asyncio

from apixis import post_and_wait_threadsafe, subscribe
from apixis.core.event.base import suspend_process


def worker(value):
    return asyncio.run(post_and_wait_threadsafe(
        event_name="job.child", context=value, thread_ids="main",
    ))


@subscribe("job.parent")
async def parent(event):
    async with suspend_process():
        child_event = await asyncio.to_thread(worker, event.context)
    event.context = child_event.context


@subscribe("job.child")
async def child(event):
    event.context *= 2
```

释放和恢复均由来源 loop 完成。进入 `suspend_process()` 后，当前 token 绑定暂设为 `None`，因此随后启动的工作线程也不会继承该 token；退出时恢复来源处理链的绑定并按需重新取得许可。普通计算或 I/O 的线程卸载仍默认计入来源处理链的背压，是否借出许可取决于业务是否需要该 core 在等待期间继续派发。

取消 `await asyncio.to_thread(...)` 不会停止已经运行的线程函数。需要提前结束工作时，应用须提供协作退出信号，并在线程结束前保持其依赖的目标 loop 可用。

## 暂停消费与停止管道

应用入口可以显式管理当前线程组件：

```python
from apixis import stop_core

await stop_core()
```

`stop_core()` 统一管理当前线程组件。独立创建的组件仍可分别调用 `event_loop.stop()` 和 `pipe.stop()`，详见[事件队列](./channels.md)。停止后在 asyncio 中调用 getter 会触发自动重启。

[处理器](./handlers.md) · [事件队列](./channels.md) · [文档首页](../../README.md)
