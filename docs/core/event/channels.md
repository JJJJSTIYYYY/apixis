# 本地事件队列

core 内部使用有界队列传递事件。载荷保留原始 Python 对象，无需序列化。应用通过发布接口入队，通过 subscribe 注册处理函数，消费和队列确认由框架管理。

## 发布与等待

```python
from apixis import EventType, post_event, post_and_wait

await post_event(event_type=EventType.INFO, event_name="demo.event", context={"value": 1})
result = await post_and_wait(event_name="demo.event", context={"value": 2})
```

调用前先显式 create_core、注册处理函数并 start_core。

`post_event()` 等待事件成功入队，队列满时等待空位；返回时不保证处理结束。`post_and_wait()` 等待本次事件的前台处理结束，后台 handler 不在等待范围内。在 handler 内发布或等待事件时，这些接口会协调派发许可，避免占着许可等待其他事件。

需要等待外部事件时使用 `wait_event()`；需要保证监听先注册再发布请求时使用 `expect_event()`。

## 容量与状态

队列容量通过 `APIXIS.EVENT.event_pipe_max_len` 配置，必须为正整数，默认 65536。前台派发并发容量由 `APIXIS.EVENT.event_loop_backpressure` 独立配置。

```python
from apixis import get_event_runtime_status

status = get_event_runtime_status()
print(status.event_queue_size, status.event_queue_capacity)
```

状态查询返回不可变快照，不创建或启动运行时。pipe、event loop、registry 类和组件 getter 不公开；应用不直接取出、确认、清空框架队列，也不单独启动或停止消费者。

## 暂停与关闭

`stop_core()` 只暂停消费，保留队列，仍允许发布。队列满时发布会继续等待，直到显式 start_core 恢复消费或 close_core 关闭。

`close_core()` 永久拒绝发布，等待满队列空位的发布者也会收到 `CoreClosedError`。业务主动取消发布任务仍为 `CancelledError`。关闭直接丢弃尚未派发的事件，不为它们补发取消通知；已经处理中的事件通过原有 on_cancelled 清理，剩余事件等待器返回 None。

关闭旧 core 后，应用显式 create_core 构造新实例，并重新注册订阅和图。

[完整生命周期](../runtime-lifecycle.md) · [事件 API](./README.md) · [配置](../config/README.md) · [文档首页](../../README.md)
