# 异常与日志

## 框架异常

以下异常可从 `apixis` 导入。应用应按异常类型处理失败，避免依赖具体错误文本。

| 异常 | 含义 |
| --- | --- |
| `CoreClosedError` | core 已开始关闭，拒绝新的发布、注册或图调用 |
| `BlockHookNotRegisteredError` | 中断事件完成前台处理后，无错误且未被接受，Block 未完成，`event.seen` 仍为空 |
| `BlockNotResolvedError` | 中断事件无错误且未被接受，Block 未完成也未被接管，`event.seen` 已有业务处理器记录 |
| `EventHandlerAlreadyRegisteredError` | 禁止覆盖时注册了同名处理器 |
| `EventHandlerNotRegisteredError` | 所需处理器尚未注册 |
| `GraphNodeError` | 图节点执行错误 |
| `InvalidContextError` | 图执行上下文不合法 |
| `InvalidNodeReturnsError` | 节点返回值不符合要求 |

参数校验还可能抛出 `ValueError` 或 `TypeError`。异步取消使用 `asyncio.CancelledError`，不属于普通 `Exception`。

中断事件的普通订阅也会影响 `seen`，具体接管方式见[图中断](../graph/README.md#interrupt--block)。已经接纳的图调用在 core 关闭时可能以 `CancelledError` 结束；新调用的准入错误为 `CoreClosedError`。

## 输出日志

应用通过 `Logger.set_{level}(logger)` 为各等级设置同步日志函数：

```python
import logging
from apixis import Logger

logging.basicConfig(level=logging.INFO)
application_logger = logging.getLogger("application")
Logger.set_debug(application_logger.debug)
Logger.set_info(application_logger.info)
Logger.set_warning(application_logger.warning)
Logger.set_error(application_logger.error)
Logger.set_exception(application_logger.exception)
# 标准 logging 没有 success 等级，可由应用选择映射。
Logger.set_success(application_logger.info)

Logger.info("Job %s completed", "example")
Logger.success("Application started")

try:
    raise ValueError("invalid node")
except ValueError:
    Logger.exception("Cannot run graph")

Logger.set_debug(None)  # 停用该等级
```

支持 `debug/info/success/warning/error/exception` 六个等级，分别使用 `set_debug/set_info/set_success/set_warning/set_error/set_exception` 设置。每个回调初始值均为 `None`，未设置时调用对应方法无操作。各等级独立，设置 `error` 不会启用 `exception`。

共享运行时使用类上的回调，无需实例化 `Logger`。后续设置会替换该等级的回调，传入 `None` 可关闭该等级。建议在启动 core 前统一配置。

消息、额外位置参数和关键字参数原样传给回调。格式化语法、输出目标、时间、颜色、日志过滤和 traceback 均由应用的日志器负责；APIXIS 不再读取 `APIXIS.LOG.level`，也不自行格式化或输出日志。回调应为同步可调用对象，返回值被忽略，异常直接向调用者传播。

应用负责保证设置的日志器正常工作，没有抛异常的 bug。框架不捕获或忽略日志器异常；生命周期操作若因此中断，应用修复日志器后可再次调用同一生命周期接口，重新执行该阶段流程。

`APIXIS.LOG.show_event_dispatch` 仍控制事件分发时的直接 `print`，与日志回调独立。

[日志配置](../config/README.md) · [接口概览](../README.md)
