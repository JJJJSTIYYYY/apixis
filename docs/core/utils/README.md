# 异常与日志

## 框架异常

以下异常可从 `apixis` 导入。应用应按异常类型处理失败，避免依赖具体错误文本。

| 异常 | 含义 |
| --- | --- |
| `BlockHookNotRegisteredError` | 图发生中断，但没有可执行的中断处理器 |
| `BlockNotResolvedError` | 中断处理器返回时，Block 既未完成也未被接管 |
| `EventHandlerAlreadyRegisteredError` | 禁止覆盖时注册了同名处理器 |
| `EventHandlerNotRegisteredError` | 所需处理器尚未注册 |
| `GraphNodeError` | 图节点执行错误 |
| `InvalidContextError` | 图执行上下文不合法 |
| `InvalidNodeReturnsError` | 节点返回值不符合要求 |

参数校验还可能抛出 `ValueError` 或 `TypeError`。异步取消使用 `asyncio.CancelledError`，不属于普通 `Exception`。

## 输出日志

```python
from apixis import logger

logger.info("Application started")
logger.warning("A retry is needed")
logger.error("Operation failed")
logger.debug("Preparing request")

logger.success("Job %s completed", "example")
```

`logger` 是模块，`debug/info/success/warning/error/exception` 均为模块级函数；也可直接导入函数：

```python
from apixis.core.utils.logger import info

info("Job received")
```

每次调用立即输出 ANSI 彩色文本，包含时间、`[APIX]`、级别和消息。`error()`、`exception()` 写入 stderr，其余写入 stdout。额外的位置参数使用 `%` 格式化，不传参数时保留消息原文。

在 `except` 分支中调用 `exception()` 会附带当前异常的 traceback；没有正在处理的异常时只输出消息。

日志模块无缓存、后台任务或文件操作，所有级别均直接输出。无需实例化日志类，也无需启动、停止或刷新。需要文件持久化或级别过滤时，由应用使用自己的日志设施。

[日志配置](../config/README.md) · [文档首页](../../README.md)
