# 事件处理器

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

`filter_event` 用于排除匹配项。

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

回调应为异步可调用对象。配置错误在注册时抛出异常，原有的同名订阅保留不变。

### 回调语义

| 回调 | 触发条件 |
| --- | --- |
| `core_func(event)` | handler 正常执行 |
| `on_has_error(event)` | 前置 handler 已产生错误 |
| `on_accepted(event)` | 事件已被 accept |
| `on_error(event, exc)` | 当前 handler 自身回调抛出普通异常 |
| `on_cancelled(event)` | event loop 通知取消 |

`on_has_error` 是**前序 handler 错误通知**，不是 `core_func` 自身异常处理的替代品。

### `stop_when_error`

当事件已有错误时：

- `True`：在 `on_has_error` 之后跳过当前 core callback；
- `False`：仍继续执行当前 core callback。

### `time_out`

正数表示每个被调用 callback 的超时时间，单位秒。`None` 或非正值表示不限制。

### `background`

`background=True` 时，后续 handler 不等待此 handler 完成。后台 handler 的错误仍会记录日志并触发自身 `on_error`，但不会写入事件 `error_stack`，也不会阻断其他 handler。

## Accept

任意 handler 可调用：

```python
event.accept()
```

之后的 handler：

1. 如适用，执行 `on_has_error`；
2. 执行 `on_accepted`；
3. 不再执行 core callback。

## 错误记录

前台 handler 的未捕获异常会转成 `ApixEventError`，追加到：

```python
event.error_stack
```

其中记录：handler 名、阶段、异常类型、message 和 traceback 文本。

`asyncio.CancelledError` 也会被前台 handler 记录到 `error_stack`，随后继续向上传播。

## 替换、查询与注销

```python
from apixis import subscribe, unsubscribe, get_handler_meta

# 将新的处理函数与配置安装为同名订阅；不改变已出队事件的处理记录。
subscribe("work.*", name="worker", group="workers", on_error=on_error)(new_process)
meta = get_handler_meta("worker")
unsubscribe("worker")
# 或按组注销：
unsubscribe(group="workers")
```

`get_handler_meta()` 返回独立元数据快照；修改其中的名称、分组或匹配列表不会修改路由。没有原始 handler 查询接口，handler 实现类也不属于公开 API。

handler 的 group 默认为 `"global"`，必须是非空字符串。名称在整个 registry 中唯一，组名不参与名称去重。变更分组时，使用同名 subscribe 并指定新的 group。

`unsubscribe(name)` 与 `unsubscribe(group=...)` 二选一；分组名精确匹配、区分大小写，不解释通配符。默认忽略不存在的名称或组，`missing_ok=False` 时抛出 `EventHandlerNotRegisteredError`。注销清理关联索引和链缓存，已出队事件保留捕获的 handler 引用。

[事件 API](./README.md) · [文档首页](../../README.md)
