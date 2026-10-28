# APIXIS 使用文档

APIXIS 为 Python `asyncio` 应用提供事件处理和图执行能力。应用可以单独使用事件系统，也可以通过图组织多步、并发和可中断的任务。

要求 Python 3.12 或更高版本。安装后，推荐直接从 `apixis` 导入公开接口：

```bash
pip install apixis
```

## 从这里开始

1. [快速开始](./quickstart.md)：运行第一个图和事件订阅示例。
2. [接口概览](./core/README.md)：选择事件、图或工具接口。
3. [配置](./core/config/README.md)：调整队列容量、并发限制、日志。

## 按任务查阅

| 功能 | 文档 |
| --- | --- |
| 发布事件、管理共享事件系统 | [事件 API](./core/event/README.md) |
| 等待事件，或先订阅回包再发布请求 | [事件 API：await_for 与 wait_for_event](./core/event/README.md#等待事件await_for-与-wait_for_event) |
| 订阅、排序、替换 handler，处理错误与取消 | [事件处理器](./core/event/handlers.md) |
| 使用本地事件队列 | [事件通道](./core/event/channels.md) |
| 构建图、执行、流式输出、快照恢复和中断 | [图 API](./core/graph/README.md) |
| 更新状态、选择下一跳和组织并发分支 | [图状态与 Command](./core/graph/state.md) |
| 捕获框架异常、输出日志 | [异常与日志](./core/utils/README.md) |

## 阅读约定

[快速开始](./quickstart.md)中的示例可以保存为 Python 文件直接运行。接口页中的短示例省略了应用入口；含 `await` 的代码应放在异步函数内，并在运行中的 asyncio 事件循环中调用。

共享 event core 应在同一个 asyncio 事件循环中使用。以下划线开头的属性和模块内部对象不属于应用应依赖的接口。
