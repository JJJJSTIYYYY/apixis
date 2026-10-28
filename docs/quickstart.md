# 快速开始

## 安装

```bash
pip install apixis
```

运行环境为 Python 3.12+ 和 `asyncio`。事件发布和图执行均在当前进程内运行。

## 执行一个图

将下面代码保存为 `graph_demo.py`，运行 `python graph_demo.py`：

```python
import asyncio

from apixis import Command, GraphManager


def increment(state: dict) -> Command:
    return Command(update={"value": state["value"] + 1}, goto="double")


def double(state: dict) -> dict:
    return {"value": state["value"] * 2}


async def main() -> None:
    graph = (
        GraphManager()
        .add_nodes([increment, double])
        .compile_graph(entry_point="increment")
    )
    with graph:
        result = await graph.invoke({"value": 1})
        print(result)  # {'value': 4}


asyncio.run(main())
```

`Command.update` 更新状态，`Command.goto` 指定下一步。返回普通字典只更新状态并结束当前分支。`with graph` 在退出时释放图的注册资源。

继续阅读：[构图与执行](./core/graph/README.md)、[状态与下一跳](./core/graph/state.md)。

## 发布和处理事件

将下面代码保存为 `event_demo.py`，运行 `python event_demo.py`：

```python
import asyncio

from apixis import (
    EventType, get_event_loop, get_event_pipe, start_core, subscribe, unsubscribe,
)


async def main() -> None:
    handled = asyncio.Event()

    @subscribe("app.started")
    async def observe(event):
        print(event.event_name, event.context)
        handled.set()

    pipe = get_event_pipe()
    event_loop = get_event_loop()
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
        await event_loop.stop()
        await pipe.stop()


asyncio.run(main())
```

`subscribe()` 注册异步处理函数，`post_event()` 创建并发布事件。示例使用 `asyncio.Event` 等待应用需要的处理结果；`pipe.join()` 只等待队列项目完成交接，不表示所有 handler 已执行结束。

共享 event core 服务整个应用，启动和停止通常由应用入口统一管理。继续阅读：[事件 API](./core/event/README.md)、[事件处理器](./core/event/handlers.md)。

## 调整运行参数

在启动应用的工作目录中创建 `config.yaml`：

```yaml
LOG:
  show_event_dispatch: false

PIPELINE:
  event_pipe_max_len: 65536
  event_loop_backpressure: 1024
```

配置在导入时读取，修改后需要重启应用。完整选项见[配置](./core/config/README.md)。

[返回文档首页](./README.md)
