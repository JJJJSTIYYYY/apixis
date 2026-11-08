# Handler chain 缓存检查（2026-11-08）

正常发布、图执行和受支持的注册更新路径中，构建缓存稳定命中；发现一处可复现的缓存失效缺陷：直接修改已注册 handler 实例的 subscribe / filter_event，再重新注册时，旧链可能残留，注销后还可能导致 KeyError。

## 真实构建计数

通过审计专用 CountingRegistry 统计 get_handlers_chain_for_event、_resolve_handler_chain 和 get_handlers_for_event 调用次数；并通过真实事件核心、工作线程及 NodeGraph 验证。这里统计构建与命中次数，不将插桩运行时间当作性能基准。14 个正常场景断言通过，另记录 3 个缺陷观察。

| 场景 | 查询次数 | 实际匹配 / 排序构建次数 | 命中次数 |
| --- | ---: | ---: | ---: |
| 固定名称（精确 + glob），100,000 次查询 | 100,000 | 1 | 99,999 |
| 空链，100,000 次查询 | 100,000 | 1 | 99,999 |
| 1,000 轮无关精确订阅注册 / 注销 | 1,001 | 1 | 1,000 |
| 注销相关分组，保留无关热链 | 4 | 3 | 1 |
| 使用新实例替换同名注册 | 6 | 5 | 1 |
| glob 过滤排除 / 匹配时的定向失效 | 103 | 3 | 100 |
| 50,000 个新名称与热名称交替查询 | 100,000 | 50,001 | 49,999 |
| 8,193 个名称按相同顺序循环两遍 | 16,386 | 16,386 | 0 |
| 真实派发：32,768 个事件 / 32 个名称 | 32,768 | 32 | 32,736 |
| 跨线程：32 workers / 8,192 个同名事件 | 8,192 | 1 | 8,191 |
| 暂停 / 重启后再发同名事件（累计查询） | 8,193 | 1 | 8,192 |
| 通过 handler.register 参数更新同一实例 | 3 | 3 | 0 |
| 复用图：128 次调用 × 16 节点 | 2,176 | 1 | 2,175 |
| 512 次请求响应，逐次注册临时监听 | 1,024 | 513 | 511 |

- 当前容量为 8,192。50,000 个新名称穿插热名称时，热名称自身只构建 1 次；其余 50,000 次为新名称的首次构建。
- 8,193 个名称超过容量，按固定顺序循环会使前一轮缓存依次淘汰，出现 0 命中；这是该访问序列下的 LRU 行为。
- 32,768 个实际事件对应 32 个事件名，各构建 1 次；线程桥接复用主线程注册表，8,192 个同名事件只构建 1 次。
- 复用图实际收到 2,176 次 dispatch 查询，包括每次调用末尾的结束目标派发；2,048 次节点执行共用同一 dispatch 名，链仅构建 1 次。
- 请求响应中，request.fixed 只构建 1 次；reply.fixed 构建 512 次。每轮临时 handler 的身份和注册状态变化，注销会清除相关响应链，下一轮需要重建。请求链保持热缓存，未被全量清空。

## 缓存边界

[get_handlers_chain_for_event](../../src/apixis/core/event/handler_registry.py#L229) 缓存的是有序 handler 名称列表。仅缺失、值为 None、失效删除或 LRU 淘汰后才重新执行匹配与排序；空列表也是有效缓存。命中会刷新 LRU 顺序，没有时间过期机制。

[get_handlers_for_event](../../src/apixis/core/event/handler_registry.py#L243) 每次仍将名称映射为当前 handler 引用并生成独立列表，保证出队时捕获的链不受后续替换 / 注销影响。32,768 个事件因此生成了 32,768 份 handler 引用列表，但仅进行 32 次匹配 / 排序构建。

精确订阅变更只检查对应缓存键；glob 订阅变更检查有界缓存中的匹配键。过滤排除的缓存和无关分组的热链可保留。正常以新实例替换，或通过 handler.register 参数更新原实例时，旧 / 新匹配的相关缓存均失效。

## 缺陷：原地改元数据后重新注册，旧缓存未失效

位置：[失效匹配](../../src/apixis/core/event/handler_registry.py#L195)、[重新注册时注销旧条目](../../src/apixis/core/event/handler_registry.py#L327)、[注销时失效](../../src/apixis/core/event/handler_registry.py#L369)。

最小复现：

```python
from apixis import ApixEventHandler, ApixEventRegistry, ApixHandlerRegistry

async def callback(event):
    pass

registry = ApixHandlerRegistry(ApixEventRegistry())
worker = ApixEventHandler(callback, name="worker")
worker.subscribe = ["old"]
worker.priority = 1
registry.register_handler(worker)
registry.get_handlers_chain_for_event("old")  # ["worker"]

worker.subscribe = ["new"]
registry.register_handler(worker, exist_ok=True)
registry.get_handlers_chain_for_event("old")  # 错误：仍为 ["worker"]，应为 []

registry.unregister_handler("worker")
registry.get_handlers_for_event("old")  # KeyError("worker")
```

如果将订阅改为 new，注销旧条目时读取的是该活对象已经变更后的 subscribe；只检查 new，遗漏 old 的已有缓存。旧索引虽能通过 _handler_exact_names 清理，旧缓存仍存活。将 filter_event 改成排除 old 后重新注册，也会因失效判断读取新过滤条件而遗漏旧链。强制移除缓存后重新查询均得到正确空链，证明问题发生在失效路径。

该错误可让 old 事件调用已经改订阅的 handler；随后注销 handler，缓存中的名称无法从 registry 取到而触发 KeyError。真实 consumer 捕获链解析异常、确认队列项目并继续，因此这次事件不会进入正常派发。

handler_registry.py 与功能前 HEAD 5049c7d 完全一致，此问题不是线程桥接引入的新变更。常规通过 worker.register("new") 更新参数、无需先修改实例属性的路径已验证正常。

修复方向是让注册表根据上次成功注册的不可变订阅 / 过滤元数据进行失效，避免读取已被调用方修改的活对象；仅快照路由元数据，保留当前 handler 引用及执行配置语义。此报告记录检查结果，尚未改动运行时实现。

## 验证与复现

现有 handler registry、索引、分组、规模和背压边界相关测试：413 passed。现有用例没有覆盖上述同一实例原地修改路由元数据的边界。

```bash
.venv/bin/python -m pytest -q \
    tests/core/event/test_handler_registry.py \
    tests/core/event/test_handler_registry_index.py \
    tests/core/event/test_handler_groups.py \
    tests/integration/event/test_handler_registry_scale.py \
    tests/integration/event/test_backpressure_boundaries.py
.venv/bin/python scripts/audit_handler_chain_cache.py \
    --output docs/performance/data/2026-11-08-handler-chain-cache.json
```

审计脚本发现缺陷时故意返回退出码 1；正常场景的断言失败则会抛出异常。当前退出码 1 表示成功复现记录的缺陷，而非所有场景均通过。

- [审计脚本](../../scripts/audit_handler_chain_cache.py)
- [构建次数及完整缺陷观察 JSON](data/2026-11-08-handler-chain-cache.json)
- 运行时源码 SHA256（排除版本文件）：`5b095fd3525161b23a72d28ecf8233d36981dba5989a4df426f2b89c8fdaccee`；Python 3.12.12。
