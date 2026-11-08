# 事件处理器

## `@subscribe(...)`

```text
subscribe(
    *event_names: str,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
    group: str | None = None,
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

直接修改同一个 handler 实例的 callback 或执行选项，仍可能影响已经取出的事件。需要保留旧事件的处理行为时，使用新的 handler 实例进行替换。

## 排序

### `priority`

数值越大，排序越靠前；相同优先级按注册顺序执行。`subscribe()` 接受任意有限数值，未指定排序参数时默认优先级为 `1`。事件等待监听器不参与优先级排序：`received` 始终位于普通订阅之前，`processed` 始终位于普通订阅之后。

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

## `ApixEventHandler`

需要自定义生命周期回调时，可以显式创建 handler：

```python
from apixis import ApixEventHandler

handler = ApixEventHandler(
    core_func,
    on_accepted=on_accepted,
    on_has_error=on_has_error,
    on_error=on_error,
    on_cancelled=on_cancelled,
    stop_when_error=True,
    time_out=None,
    background=False,
    name="worker",
    group="global",
)
handler.register("work.*")
```

若参数 name 未指定，将默认使用 core_func 的名字作为 handler 名，后续动态 set_core_func 不会因为 core_func 改变而更新。

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

正数表示每个被调用 callback 的超时时间，单位秒。创建 handler 时 `None` 或非正值表示不限制；通过 `subscribe()` 配置已有 handler 实例时，`None` 保留原设置，非正值显式取消超时限制。

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

## 动态更新 callback

```python
handler.set_core_func(new_core_func)
handler.add_has_error_callback(callback)
handler.add_on_accepted_callback(callback)
handler.add_on_error_callback(callback)
handler.add_on_cancelled_callback(callback)
```

`set_core_func()` 不改变 handler 身份和当前注册元数据。

## 注册与注销

```python
handler.register("event.*", priority=10)
handler.unregister()
```

这些实例方法与 `subscribe()` / `unsubscribe()` 使用同一套 registry 语义。

handler 的 `group` 默认为 `"global"`，必须是非空字符串。可在构造函数、`subscribe()` 或 `register()` 中指定；后两者省略 `group` 或传入 `None` 时保留已有实例的分组，新函数默认使用 `"global"`。handler 名称仍在整个 registry 中唯一。

```python
from apixis import subscribe, unsubscribe

@subscribe("work.*", group="workers")
async def observe_work(event):
    ...

handler.register("work.*", group="workers")
handler.unregister(same_group=True)  # 注销 workers 组内全部 handler
# 也可直接按组注销：
unsubscribe(group="workers")
```

`unsubscribe(name)` 与 `unsubscribe(group=...)` 二选一；分组名精确匹配、区分大小写，不解释通配符。默认忽略不存在的组，`missing_ok=False` 时抛出 `EventHandlerNotRegisteredError`。独立 registry 可调用 `unregister_group(group, missing_ok=False)`。

registry 维护 group 到 handler 名称集合的索引，批量注销直接查找该组，不遍历整个 handler 表或其他分组。单个注销、同名替换和 `clear()` 同步更新索引并清理空组。修改已注册实例的 group 时应使用 `register(group=...)` 重新注册。注销会清理相关链缓存，已取出事件保留捕获的 handler 引用。

[事件 API](./README.md) · [文档首页](../../README.md)
