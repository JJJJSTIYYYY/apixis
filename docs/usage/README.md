# 使用文档

先阅读[快速开始](quickstart.md)和[生命周期](lifecycle.md)，再按任务查阅下面的接口。推荐应用代码直接从 `apixis` 导入接口。`apixis.core` 提供相同的核心接口；顶层还提供 `VERSION` 和 `__version__` 供查询版本。

```python
from apixis import GraphManager, EventType, subscribe, __version__
```

快速开始的完整示例可以直接保存为 Python 文件运行。接口页的短片段省略应用入口与资源清理，含 `await` 的片段应放入异步函数；发布、订阅和构图前先创建 core，需要消费事件时显式启动，退出时等待关闭。

## 事件系统

| 接口 | 用途 |
| --- | --- |
| `create_core()` | 只创建共享 core 的组件 |
| `start_core()` | 启动已有组件的队列消费 |
| `get_core()` | 同步读取已有 core 的状态并配置关闭回调，不创建、不启动 |
| `stop_core()` | 只停止队列消费，已安排的链继续执行 |
| `close_core()` | 永久禁入，取消在途事件并通知全部 waiter |
| `CoreState` | 统一生命周期状态 |
| `ApixEvent`、`EventType` | 表示事件及其类别 |
| `subscribe()`、`unsubscribe()` | 注册、配置与注销处理器 |
| `post_event()`、`post_and_wait()` | 发布事件，或等待本次事件的前台处理结束 |
| `wait_event()`、`expect_event()` | 等待外部事件，或先注册监听再运行上下文主体 |
| `get_event_runtime_status()` | 查询不可变运行状态，不触发启动 |

详见[生命周期](lifecycle.md)、[事件 API](./event/README.md)、[处理器](./event/handlers.md)和[事件队列](./event/channels.md)。

## 图执行

| 接口 | 用途 |
| --- | --- |
| `GraphManager`、`NodeGraph` | 注册节点、编译和执行图 |
| `Command` | 更新状态并指定下一跳 |
| `AutoMerge`、`KeepRef`、`Reset` | 声明状态合并和复制行为 |
| `GraphContext` | 查看执行状态、保存快照与恢复 |
| `get_stream_writer()` | 从节点发送流式数据 |
| `interrupt()`、`Block` | 等待外部输入并恢复执行 |

详见[图 API](./graph/README.md)和[状态与 Command](./graph/state.md)。

## 配置与工具

- [配置](./config/README.md)：队列、并发、日志参数。
- [异常与日志](./utils/README.md)：框架异常和日志接口。

[返回文档首页](../README.md)

## 公开接口边界

应用通过生命周期、订阅、发布、等待、状态与元数据查询及图执行接口管理运行时。`get_core()` 保留状态和关闭回调接口；内部组件通过私有成员持有。

使用 `subscribe()` 注册和配置 handler，使用 `get_handler_meta()` 查询独立元数据快照，使用 `unsubscribe()` 注销。通过 `get_graph_dispatch_name()` / `get_graph_interrupted_name()` 获取图的事件名称和插件边界，使用 `GraphContext` 和快照接口管理调用状态。

以下划线开头的成员、内部实现类和组件 getter 供框架内部使用。组件归属和依赖设计见[开发文档](../development/README.md)。
