# 配置

APIXIS 在导入配置模块时读取当前工作目录中的 `config.yaml`。没有文件时使用默认值；配置不会在运行中自动重新加载。只读取顶层 `APIXIS` 映射，其他顶层配置不参与读取；不兼容旧的配置结构。`APIXIS` 缺失或为 `null` 时使用默认值，其他非映射值会抛出 `ValueError`。

## 本地应用示例

```yaml
APIXIS:
  EVENT:
    event_pipe_max_len: 65536
    event_loop_backpressure: null
    handler_chain_cache_size: 8192
  LOG:
    show_event_dispatch: false
```

## 事件日志输出

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `APIXIS.LOG.show_event_dispatch` | `false` | 是否通过 print 打印每次事件分发的信息，独立于 Logger 回调 |

事件分发开关只读取 `APIXIS.LOG.show_event_dispatch`，原 `APIXIS.EVENT.show_event_dispatch` 不再读取。日志由应用通过 `Logger.set_{level}()` 接入，默认不输出。使用方式见[日志接口](../utils/README.md)。

## 队列与并发

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `APIXIS.EVENT.event_pipe_max_len` | `65536` | 每个 core 的本地事件队列和跨线程接收队列各自的容量，必须为正整数 |
| `APIXIS.EVENT.event_loop_backpressure` | `null` | `null`（Python `None`）不限制前台事件分发并发，只维护许可计数；设为正整数时启用背压，最小值为 1，按配置值使用 |
| `APIXIS.EVENT.handler_chain_cache_size` | `8192` | 精确事件名的处理器链 LRU 缓存容量，必须为正整数；淘汰后按当前订阅重建，不影响事件观察历史及已捕获的分发链 |

队列满时异步发布会等待空位。并发限制针对前台事件处理；后台 handler 不受单独的并发上限保护，应由应用控制后台工作量。

每个线程的 core 独立维护派发背压。设为 `1` 时，每个线程各有一个前台许可；本地等待和跨线程请求期间仅借出调用方当前处理链的本地许可。每个目标的跨线程接收队列与事件队列相互独立，均使用 `event_pipe_max_len`；接收队列满时，调用方协程异步等待容量通知，不占用默认线程池。

容量与并发配置项接受正整数，省略或设为 YAML `null` 时使用默认值。类型错误抛出 `TypeError`，超出数值范围抛出 `ValueError`。

[返回文档首页](../../README.md)
