# 快速开始

## 安装

```bash
pip install apixis
```

运行环境为 Python 3.12+ 和 `asyncio`。事件发布、处理与跨线程通信均在当前进程内运行。

## 发布和处理事件

将下面代码保存为 `event_demo.py`，运行 `python event_demo.py`：

```python
import asyncio

from apixis import (
    EventType, get_event_pipe, start_core, stop_core, subscribe, unsubscribe,
)


async def main() -> None:
    handled = asyncio.Event()

    @subscribe("app.started")
    async def observe(event):
        print(event.event_name, event.context)
        handled.set()

    pipe = get_event_pipe()
    try:
        await start_core()
        await pipe.post_event(
            event_type=EventType.INFO,
            event_name="app.started",
            context={"ready": True},
        )
        await handled.wait()
    finally:
        unsubscribe("observe")
        await stop_core()


asyncio.run(main())
```

`subscribe()` 注册异步处理函数，`post_event()` 创建并发布事件。示例使用 `asyncio.Event` 等待应用需要的处理结果；`pipe.join()` 只等待队列项目完成交接，不表示所有 handler 已执行结束。

共享 event core 服务整个应用，启动和停止通常由应用入口统一管理。继续阅读：[事件 API](./core/event/README.md)、[事件处理器](./core/event/handlers.md)。

## 调整运行参数

在启动应用的工作目录中创建 `config.yaml`：

```yaml
APIXIS:
  EVENT:
    event_pipe_max_len: 65536
    event_loop_backpressure: null
  LOG:
    show_event_dispatch: false
```

配置在导入时读取，修改后需要重启应用。完整选项见[配置](./core/config/README.md)。

[返回文档首页](./README.md)
