# 配置

APIXIS 在导入配置模块时读取当前工作目录中的 `config.yaml`。没有文件时使用默认值；配置不会在运行中自动重新加载。

## 本地应用示例

```yaml
LOG:
  show_event_dispatch: false

PIPELINE:
  event_pipe_max_len: 65536
  event_loop_backpressure: 1024
  handler_chain_cache_size: 8192

GRAPH:
  max_snapshots: 16
```

## 事件分发输出

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `LOG.show_event_dispatch` | `true` | 是否额外输出每次事件分发的信息 |

彩色日志函数直接输出到控制台，无需配置。旧的 `LOG.debug_level`、`LOG.trace`、`LOG.buffer_size`、`LOG.max_log_file_size` 和日志目录配置已移除。使用方式见[日志接口](../utils/README.md)。

## 队列与并发

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `PIPELINE.event_pipe_max_len` | `65536` | 默认本地事件队列容量，必须为正整数 |
| `PIPELINE.event_loop_backpressure` | `1024` | 并发前台事件分发数量上限，必须为正整数；1–127 按 128 使用 |
| `PIPELINE.handler_chain_cache_size` | `8192` | 精确事件名的处理器链 LRU 缓存容量，必须为正整数；淘汰后按当前订阅重建，不影响事件观察历史及已捕获的分发链 |

队列满时异步发布会等待空位。并发限制针对前台事件处理；后台 handler 不受单独的并发上限保护，应由应用控制后台工作量。

## 图快照

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `GRAPH.max_snapshots` | `16` | 每个 context 保留的最近快照数量，必须为正整数；超出时删除最旧记录 |

`compile_graph(..., max_snapshots=...)` 或 `NodeGraph(..., max_snapshots=...)` 可覆盖此默认值。自动和手动快照、恢复的历史均遵循该上限。

容量与条数不接受布尔值、浮点数、数字字符串、NaN 或无穷大。类型错误抛出 `TypeError`，超出数值范围抛出 `ValueError`。省略或设为 YAML `null` 时沿用配置加载器的默认值。

[返回文档首页](../../README.md)
