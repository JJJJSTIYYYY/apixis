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
| `post_event()`、`post_and_wait()` | 发布事件，或发布并等待本次事件的前台处理结束 |
| `wait_event()`、`expect_event()` | 等待结果事件，或先注册监听再发布请求；可用 `predicate` 匹配业务条件 |
| `get_event_runtime_status()` | 查询队列、派发许可、任务与跨线程桥接的只读快照，不启动核心 |
| `post_event_threadsafe()`、`post_and_wait_threadsafe()`、`wait_event_threadsafe()`、`expect_event_threadsafe()` | 在工作线程的 asyncio loop 中发布和等待，主循环统一处理 |

详见[事件 API](./event/README.md)、[处理器](./event/handlers.md)和[事件队列](./event/channels.md)。

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
