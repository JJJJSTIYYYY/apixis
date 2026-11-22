# 事件处理器

处理器通过 `subscribe()` 注册，在事件名称匹配时执行。先创建 core，再定义订阅；普通业务函数和通知回调均须在调用后返回 awaitable，通常直接使用 `async def`。

## `@subscribe(...)`

```text
subscribe(
    *event_names: str,
    name: str | None = None,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
    group: str | None = None,
    on_accepted: EventHandlerFunc | None = None,
    on_has_error: EventHandlerFunc | None = None,
    on_error: EventHandlerErrorFunc | None = None,
    on_cancelled: EventHandlerFunc | None = None,
)
```

最常见用法：

```python
from apixis import subscribe


@subscribe("order.*")
async def audit(event):
    ...
```

### 事件名匹配

`event_names` 支持 glob 模式，例如：

```python
@subscribe("order.*", "payment.?")
async def observe(event):
    ...
```

`filter_event` 用于排除匹配项。匹配区分大小写，至少提供一个非空订阅模式。

### 替换语义

默认 `exist_ok=True`。如果同名 handler 已注册，新注册会替换旧注册。

设置 `exist_ok=False` 时，同名注册会抛出 `EventHandlerAlreadyRegisteredError`。

事件从队列取出时确定它使用的 handler 及执行顺序。之后的新增、注销、同名替换或排序调整，只影响后续取出的事件；已取出的事件仍使用原来的 handler，即使它尚未开始执行。

名称、分组、匹配模式、排序、回调和执行选项均由内部记录持有。`name` 默认使用被装饰函数的名称，并在注册时捕获；后续修改函数的 `__name__` 不会重命名已有注册。变更配置时用相同 name 再次 subscribe，旧事件仍使用已捕获的旧记录。

## 排序

### `priority`

数值越大，排序越靠前；相同优先级按注册顺序执行。`subscribe()` 接受有限数值，未指定排序参数时默认优先级为 `1`。

```python
@subscribe("order.*", priority=100)
async def validate(event):
    ...
```

### `between_handlers`

用于将 handler 放到两个已注册 handler 之间，不能与显式 `priority` 同时使用。

```python
@subscribe(
    "order.*",
    between_handlers=("validate", "persist"),
)
async def enrich(event):
    ...
```

边界的一侧可以为 `None`，但不能两侧同时为 `None`。`between_handlers` 具体插入规则如下：

- (left, right)：插入 left 与 right 之间，若其间已有 handler，则插入已有 handler 之后（即 right 前）
- (left, None)：插入 left 之后
- (None, right)：插入 right 之前
- (None, None)：抛出 ValueError

## 通知回调

直接在订阅时配置回调：

```python
from apixis import subscribe


async def on_error(event, error):
    print(event.event_name, error)


async def on_cancelled(event):
    print("cancelled", event.event_name)


@subscribe("work.*", name="worker", group="workers",
           on_error=on_error, on_cancelled=on_cancelled)
async def process(event):
    ...
```

回调必须可调用，并在调用后返回 awaitable。注册时会检查 callable 及排序、名称等配置；已执行的参数校验失败会保留原有同名订阅。参数签名和返回值不在注册时完整验证，错误可能延后到实际派发时出现。例如同步函数返回 `None` 可以注册成功，但执行时会产生 `TypeError`。

### 回调语义

| 回调 | 触发条件 |
| --- | --- |
| `core_func(event)` | handler 正常执行 |
| `on_has_error(event)` | 前置 handler 已产生错误 |
| `on_accepted(event)` | 事件已被 accept |
| `on_error(event, exc)` | 当前 handler 的业务函数、`on_has_error` 或 `on_accepted` 抛出普通异常 |
| `on_cancelled(event)` | 框架通知任务取消，使用独立清理规则 |

`on_has_error` 是**前序 handler 错误通知**，不是 `core_func` 自身异常处理的替代品。

### `stop_when_error`

默认 `True`。当事件已有错误时：

- `True`：在 `on_has_error` 之后跳过当前 core callback；
- `False`：仍继续执行当前 core callback。

### `time_out`

正数表示业务函数、`on_has_error`、`on_accepted` 和 `on_error` 各次调用的独立超时时间，单位秒。默认 `None`，非正值也表示不限制。超时按普通异常记录，并按适用规则触发 `on_error`；`on_error` 自身失败只记录，不递归调用自己。框架取消使用下述独立清理规则。

### 取消清理

前台链收到取消时，按已捕获的处理顺序通知其前台 handler；后台任务取消只通知该后台 handler。尚未进入派发执行的事件在关闭时直接丢弃，不补发通知。

每个 `on_cancelled` 使用独立的 **5 秒** `asyncio.timeout()`，不采用订阅的 `time_out`。这是协作式超时，回调必须响应取消才能结束。清理异常（包括自行抛出的 `CancelledError`）只写日志，不触发 `on_error`，不追加 `event.error_stack`。

关闭取消后，正常回调即使吞掉取消或改抛其他异常，也不会继续进入后续业务阶段；清理由 `on_cancelled` 完成。`on_cancelled` 可能与等待器返回 `None` 交错执行，不能通过等待结果判断清理结束。关闭的返回时点及重入边界见[生命周期](../lifecycle.md#关闭)。

### `background`

默认 `False`。`background=True` 时，后续 handler 不等待此 handler 完成。后台 handler 的错误仍会记录日志并触发自身 `on_error`，但不会写入事件 `error_stack`，也不会阻断其他 handler。

后台 handler 在实际开始时检查事件的错误与接受状态；它的业务执行和后续前台 handler 可能交错。不要依赖后台业务一定早于或晚于后续 `event.accept()`，尤其是应用自行设置 asyncio Task 工厂时。

## Accept

任意 handler 可调用：

```python
event.accept()
```

已接受事件的 handler 按以下规则执行：

1. 如适用，执行 `on_has_error`；
2. 执行 `on_accepted`；
3. 不再执行 core callback。

## 错误记录

前台 handler 的未捕获异常会转成 `ApixEventError`，追加到：

```python
event.error_stack
```

其中记录：handler 名、阶段、异常类型、message 和 traceback 文本。

`asyncio.CancelledError` 也会被前台 handler 的正常执行阶段记录到 `error_stack`，随后继续向上传播，不调用业务 `on_error`。取消清理中的异常沿独立的日志路径处理。

## 替换、查询与注销

```python
from apixis import (
    get_handler_meta, get_unmatched_subscriptions, is_registered,
    subscribe, unsubscribe,
)

# 将新的处理函数与配置安装为同名订阅；不改变已出队事件的处理记录。
subscribe("work.*", name="worker", group="workers", on_error=on_error)(new_process)
meta = get_handler_meta("worker")
print(is_registered("worker"))
print(get_unmatched_subscriptions("worker"))
unsubscribe("worker")
# 或按组注销：
unsubscribe(group="workers")
```

| 查询 | 结果 |
| --- | --- |
| `get_handler_meta(name)` | 独立元数据字典，包含名称、分组、订阅、过滤、排序与执行选项；未知名称返回 `None` |
| `is_registered(name)` | 该名称当前是否已注册 |
| `get_unmatched_subscriptions(name)` | 该 handler 尚未匹配任何已观察事件名的订阅模式；未知名称抛出 `EventHandlerNotRegisteredError` |

修改元数据快照不会修改路由。未匹配查询根据 core 已观察的非 `TRANSIENT` 事件名称计算，并考虑该 handler 的排除过滤；未消费的事件和临时事件不提供历史匹配依据。该查询用于检查订阅覆盖情况，不代表 handler 业务函数是否已经执行。

handler 的 group 默认为 `"global"`，必须是非空字符串。名称在整个 registry 中唯一，组名不参与名称去重。变更分组时，使用同名 subscribe 并指定新的 group。

`unsubscribe(name)` 与 `unsubscribe(group=...)` 二选一；分组名精确匹配、区分大小写，不解释通配符。默认忽略不存在的名称或组，`missing_ok=False` 时抛出 `EventHandlerNotRegisteredError`。注销清理关联索引和链缓存，已出队事件保留捕获的 handler 引用。

等待器的临时监听也在默认 `"global"` 组。`unsubscribe(group="global")` 会一并移除这些监听，但不会主动结束等待结果；仍未完成的等待可能继续挂起，直到固定超时、主动取消或 core 关闭。应用宜为业务 handler 指定独立组，只按该组清理。已经出队捕获的临时监听仍适用于其原事件。

[事件 API](./README.md) · [生命周期](../lifecycle.md) · [接口概览](../README.md)
