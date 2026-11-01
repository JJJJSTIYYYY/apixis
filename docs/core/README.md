# 接口概览

推荐应用代码直接从 `apixis` 导入接口。`apixis.core` 提供相同的核心接口；顶层还提供 `VERSION` 和 `__version__` 供查询版本。

```python
from apixis import GraphManager, EventType, subscribe, __version__
```

## 事件系统

| 接口 | 用途 |
| --- | --- |
| `start_core()`、`get_*()`、`aget_*()` | 唤起并访问共享事件系统 |
| `stop_core()` | 暂停共享事件系统，保留组件和待处理事件 |
| `ApixEvent`、`EventType` | 表示事件及其类别 |
| `subscribe()`、`unsubscribe()`、`ApixEventHandler` | 注册、配置与注销处理器 |
| `ApixEventPipe` | 发布本地事件、跨节点发送、检查队列 |
| `BuiltinChannel` | 使用本地事件队列 |

详见[事件 API](./event/README.md)、[处理器](./event/handlers.md)和[通道](./event/channels.md)。

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
