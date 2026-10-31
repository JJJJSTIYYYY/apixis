# 本地事件通道

`ApixEventPipe` 通过一个有界 `BuiltinChannel` 发布和接收事件。所有事件都在当前进程内传递，载荷保留原始 Python 对象，无需序列化。

## 发布与队列

```python
from apixis import EventType, get_event_pipe

pipe = get_event_pipe()
await pipe.post_event(
    event_type=EventType.INFO,
    event_name="demo.event",
    context={"value": 1},
)
await pipe.put(event)
builtin = pipe.get_channel()
```

队列满时异步发布会等待空位。`post_event()` 适合在 handler 内发布后续事件，会在等待期间暂时归还当前 handler 的并发许可。

| 接口 | 行为 |
| --- | --- |
| `maxsize` | 队列容量 |
| `get_channel()` | 返回当前 pipe 的 `BuiltinChannel` |
| `put_nowait(event)` | 立即写入；满时抛出 `asyncio.QueueFull` |
| `get()` / `get_nowait()` | 取出事件；后者在空队列上抛出 `asyncio.QueueEmpty` |
| `qsize()` / `empty()` / `full()` | 查看队列当前状态 |
| `task_done()` | 确认一个已取出的队列项目 |
| `join()` | 等待队列项目完成确认 |
| `clear()` | 丢弃并确认当前仍在队列中的项目，返回清理数量 |

共享 event core 会自动消费队列。应用通常通过订阅处理事件；直接调用 `get()` 会与框架竞争同一队列。

共享 core 在事件分发后调用 `task_done()`，因此 `join()` 不表示 handler 已执行完成。自行消费独立队列时，由调用方负责确认每个取出的项目。

## 独立队列

```python
from apixis import ApixEventPipe, BuiltinChannel

pipe = ApixEventPipe(builtin=BuiltinChannel(maxsize=128))
await pipe.put(event)
received = await pipe.get()
pipe.task_done()
await pipe.join()
```

`BuiltinChannel(maxsize=...)` 提供上述队列读写、检查和确认方法。单独构造时允许 `maxsize=0` 表示无限容量；注入 `ApixEventPipe` 的通道必须有界。默认容量由 `APIXIS.EVENT.event_pipe_max_len` 决定。
`maxsize` 必须是非负整数，不接受布尔值或浮点数；注入 pipe 时必须为正整数。

## 启停

```python
await pipe.start()
print(pipe.is_running)
await pipe.stop()
```

`start()` 和 `stop()` 只设置 pipe 的启动状态，重复调用安全，不会清空队列，也不会禁用队列读写。pipe 本身不创建消费者；共享 core 的消费者由事件循环管理。需要暂停事件消费时使用 `event_loop.stop()`，恢复时使用 `start_core()`。

[事件 API](./README.md) · [配置](../config/README.md) · [文档首页](../../README.md)
