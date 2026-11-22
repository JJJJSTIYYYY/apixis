# 创建、暂停与关闭 core

事件订阅、等待器和图共用一个 core。应用显式创建和启动它，在退出时等待关闭；所有操作在同一个 asyncio 事件循环中完成。

## 应用入口

```python
import asyncio
from apixis import close_core, create_core, post_and_wait, start_core, subscribe


async def main():
    create_core()

    @subscribe("job")
    async def handle(event):
        print(event.context)

    try:
        await start_core()
        await post_and_wait(event_name="job", context={"value": 1})
    finally:
        await close_core()


asyncio.run(main())
```

先 create，再导入使用订阅装饰器或构图的业务模块；也可以像上例一样，在应用入口中注册。注册订阅、编译图和查询状态都不会隐式启动消费。

## 生命周期函数

| 接口 | 用途与返回时点 |
| --- | --- |
| `create_core()` | 同步创建；已有未关闭实例时返回同一实例 |
| `await start_core()` | 启动或恢复队列消费，启动确认后返回，不等待业务处理 |
| `await stop_core()` | 暂停消费，消费者退出后返回，不取消或等待已经安排的业务任务 |
| `await close_core()` | 永久关闭准入，取消并清理框架拥有的工作，清理完成后返回 |
| `get_core()` | 同步读取已有实例，不创建、启动或恢复 |
| `get_event_runtime_status()` | 同步读取不可变状态快照，未创建时也可调用 |

未创建时，`start_core()` 和 `get_core()` 抛出 `RuntimeError`，`stop_core()` 和 `close_core()` 直接返回。重复 start 不创建多个消费者；并发 start、stop 或 close 共享各自操作。

`get_core().state` 和状态快照使用 `CoreState`：`CREATED`、`STARTING`、`STARTED`、`RUNNING`、`STOPPING`、`STOPPED`、`CLOSING`、`CLOSED`。`STARTED` 表示消费者任务已创建，`RUNNING` 表示启动调度已确认。应用通常等待 `start_core()` 完成后再进入业务主循环。

## 暂停与恢复

```python
from apixis import post_event, start_core, stop_core

await stop_core()
await post_event(event_name="job", context={"value": 2})
await start_core()
```

暂停保留队列、订阅、等待器及其固定期限，允许继续注册和入队。队列满时发布等待空位，只有显式 start 恢复消费后才能继续推进。

已经安排的前台链和后台 handler 继续运行。业务链若等待后续队列事件，需要恢复消费才能完成；stop 不会替应用暂停这些链。事件出队时已确定监听与处理器，暂停期间变更订阅不会改变它已有的处理链。

## 关闭

`close_core()` 开始执行后即拒绝新发布和注册，等待满队列的发布者会收到 `CoreClosedError`。未派发的事件直接丢弃，不执行业务函数或补发 `on_cancelled`；运行中的任务沿取消路径执行清理。

剩余等待器会返回 `None`，无需事件出现，也不调用 predicate。这个完成过程可以与在途业务的 `on_cancelled` 交错：等待器返回不代表全部业务清理结束。应用入口的 `await close_core()` 返回后，才确认框架拥有的任务与通知已清理完成。等待期限和关闭结果的关系见[事件等待结果](./event/README.md#关闭时的等待结果)。

每个 `on_cancelled` 使用独立的 5 秒协作式超时，不使用订阅的 `time_out`。业务协程需要响应取消并返回；无限吞掉取消或持续阻塞的回调会延迟关闭。取消通知异常只写日志，不触发 `on_error`，也不追加业务错误。详见[处理器取消清理](./event/handlers.md#取消清理)。

通常由应用入口负责关闭 core。在框架直接执行的取消回调中再次 `await close_core()` 会立即返回，避免等待自身；这次返回不代表整个关闭结束。不要在该回调中用新 Task 包装关闭调用再等待子任务，它会与正在汇合父回调的关闭流程互相等待，直至取消或清理超时解除等待。

关闭完成后，旧 core 永久关闭，`start_core()` 抛出 `CoreClosedError`。需要重新运行时，先等待关闭，再显式 `create_core()`，重新注册订阅和图。关闭进行中 create 会抛出 `RuntimeError`。

## 取消与操作失败

取消某个等待 start、stop 或 close 的调用者，只取消它的等待，共享生命周期操作会继续。应用需要确认最终状态时，可以再次调用并等待相同接口。

生命周期操作自身失败时，异常向调用者传播，已经完成的步骤不回滚。修复导致失败的问题后，再次调用同一接口重试。应用配置的日志回调也应正常返回；日志器抛异常可能中断操作。

## 关闭完成回调

```python
from apixis import get_core

core = get_core()

def on_closed(task):
    print("close task completed", task.cancelled())

core.add_close_task_done_callback(on_closed)
# 如无需接收通知：
core.remove_close_task_done_callback(on_closed)
```

回调是同步函数，接收原生关闭 Task；成功、失败或取消都会通知，应用应检查 Task 的状态。可在关闭前或关闭期间注册，注册本身不触发关闭；同一回调重复注册会重复调用。移除接口删除全部同一回调注册并返回数量。回调异常交给 asyncio 异常处理器。

成功关闭后不能新增回调，已有注册释放。失败或取消后的重试会继续使用仍保留的回调。图由框架管理自己的关闭通知，应用无需为每次图调用额外注册。

[接口概览](./README.md) · [事件 API](./event/README.md) · [生命周期设计](../development/runtime-lifecycle.md)
