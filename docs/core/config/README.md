# 配置

APIXIS 在导入配置模块时读取当前工作目录中的 `config.yaml`。没有文件时使用默认值；配置不会在运行中自动重新加载。

## 本地应用示例

```yaml
SERVER:
  base_dir: ./.apix/

LOG:
  debug_level: INFO
  show_event_dispatch: false
  buffer_size: 1024

PIPELINE:
  event_pipe_max_len: 65536
  event_loop_backpressure: 1024
  handler_chain_cache_size: 8192
```

## 数据目录

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `SERVER.base_dir` | `./.apix/` | 数据根目录；APIXIS 在其下使用 `apixis` 子目录 |

## 日志

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `LOG.debug_level` | `DEBUG` | 日志级别，如 `DEBUG`、`INFO`、`WARN`、`ERROR` |
| `LOG.trace` | `true` | 是否输出 trace 信息 |
| `LOG.show_event_dispatch` | `true` | 是否输出事件分发信息 |
| `LOG.buffer_size` | `1024` | 所有 logger 共享的待写日志条数上限，必须为正整数；满时淘汰最旧记录 |
| `LOG.max_log_file_size` | `10485760` | 单个日志文件的轮转大小阈值，单位为字节 |

文件写入的启停方式见[日志接口](../utils/README.md)。

## 队列与并发

| 路径 | 默认值 | 说明 |
| --- | --- | --- |
| `PIPELINE.event_pipe_max_len` | `65536` | 默认本地事件队列容量，必须大于 0 |
| `PIPELINE.event_loop_backpressure` | `1024` | 并发前台事件分发数量上限；小于 128 时按 128 使用 |
| `PIPELINE.handler_chain_cache_size` | `8192` | 精确事件名的处理器链 LRU 缓存容量，必须为正整数；淘汰后按当前订阅重建，不影响事件观察历史及已捕获的分发链 |

队列满时异步发布会等待空位。并发限制针对前台事件处理；后台 handler 不受单独的并发上限保护，应由应用控制后台工作量。

[返回文档首页](../../README.md)
