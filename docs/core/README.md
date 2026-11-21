# 接口概览

推荐应用代码直接从 `apixis` 导入接口。`apixis.core` 提供相同的核心接口；顶层还提供 `VERSION` 和 `__version__` 供查询版本。

```python
from apixis import GraphManager, EventType, subscribe, __version__
```

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

详见[五阶段生命周期](./runtime-lifecycle.md)、[事件 API](./event/README.md)、[处理器](./event/handlers.md)和[事件队列](./event/channels.md)。

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

以下实现不再从 `apixis`、`apixis.core` 或事件/图公开门面导出：

| 内部实现 | 应用使用的接口 |
| --- | --- |
| core 实现类、event loop、pipe 和 registry 类及组件 getter | 生命周期函数、`get_core()`、`get_event_runtime_status()` |
| handler 实现类、原始 `get_handler()` | `subscribe()` 配置名称、分组、执行选项和通知回调；`get_handler_meta()` 查询独立快照 |
| namespace 登记表、申请/释放和验证函数、内部事件名前缀 | 编译和分解图；查询插件边界名称使用 `get_graph_dispatch_name()` / `get_graph_interrupted_name()` |
| 状态复制、schema 解析和图定义校验工具 | 图的状态标记、编译、context 和快照接口 |

handler 配置变化通过同名 `subscribe()` 替换，注销使用 `unsubscribe()`；不直接修改内部记录。内部模块路径供框架实现和组件测试使用，不属于应用接口，也不提供旧导出的兼容别名。
