# 配置

APIXIS 在导入配置模块时读取当前工作目录中的 `config.yaml`。没有文件时使用默认值；配置不会在运行中自动重新加载。只读取顶层 `APIXIS` 映射，其他顶层配置不参与读取；不兼容旧的配置结构。`APIXIS` 缺失或为 `null` 时使用默认值，其他非映射值会抛出 `ValueError`。

## 本地应用示例

```yaml
APIXIS:
  EVENT:
    event_pipe_max_len: 65536
    event_loop_backpressure: null
    handler_chain_cache_size: 8192
  GRAPH:
    max_snapshots: 16
  LOG:
    show_event_dispatch: false
```

## 事件日志输出

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `APIXIS.LOG.show_event_dispatch` | `false` | 是否通过 print 打印每次事件分发的信息，独立于 Logger 回调 |

事件分发开关只读取 `APIXIS.LOG.show_event_dispatch`，原 `APIXIS.EVENT.show_event_dispatch` 不再读取。日志由应用通过 `Logger.set_{level}()` 接入，默认不输出。使用方式见[日志接口](../utils/README.md)。

请使用 YAML 布尔值 `true` / `false`，不要加引号。当前该开关按 Python 真值判断，没有布尔类型校验；`show_event_dispatch: "false"` 会成为非空字符串，实际开启打印。

## 队列与并发

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `APIXIS.EVENT.event_pipe_max_len` | `65536` | 共享 core 的本地事件队列容量，必须为正整数 |
| `APIXIS.EVENT.event_loop_backpressure` | `null` | `null`（Python `None`）不限制前台事件分发并发，只维护许可计数；设为正整数时启用背压，最小值为 1，按配置值使用 |
| `APIXIS.EVENT.handler_chain_cache_size` | `8192` | 精确事件名的处理器链 LRU 缓存容量，必须为正整数；淘汰后按当前订阅重建，不影响事件观察历史及已捕获的分发链 |

队列满时异步发布会等待空位。并发限制针对前台事件处理；后台 handler 不受单独的并发上限保护，应由应用控制后台工作量。

运行时在同一个 asyncio 事件循环中共享 core。设为 `1` 时，共有一个前台派发许可；handler 通过框架接口发布或等待事件时，会暂时借出当前链的许可，让其他事件推进。队列容量与前台许可独立，不存在额外的跨线程接收队列。

## 图快照

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `APIXIS.GRAPH.max_snapshots` | `16` | 每个 context 保留的最近快照数量，必须为正整数；超出时删除最旧记录 |

`compile_graph(..., max_snapshots=...)` 或 `NodeGraph(..., max_snapshots=...)` 可覆盖此默认值。自动和手动快照、恢复的历史均遵循该上限。

上述队列、并发、缓存和快照容量不接受布尔值、浮点数、数字字符串、NaN 或无穷大。类型错误抛出 `TypeError`，超出数值范围抛出 `ValueError`。省略或设为 YAML `null` 时沿用默认值；派发背压的默认值为 `None`，表示无上限。

[接口概览](../README.md) · [本地事件队列](../event/channels.md)
