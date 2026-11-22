# 2026-11-20 handler 查询入口优化

将 `get_handlers_for_event()` 中的生成器改为显式循环后，本轮 ABBA 对照中普通热查询耗时下降 8.99%，包含 LRU 淘汰的冷查询下降 2.47%。三组含等待者的对象列表查询微基准下降 31.71%–36.54%。

## 改动与边界

[查询入口](../../src/apixis/core/event/handler_registry/registry.py#L249)只替换了合并普通 handler 的循环：

```python
received, processed = self._waiter_cache.get(event_name)
handlers = received.copy()
for name in chain:
    handlers.append(self.registry[name])
handlers.extend(processed)
return handlers
```

原来的生成器捕获 self，使没有等待者的提前返回路径也要创建闭包 cell。修改后 `co_cellvars` 从 `('self',)` 变为 `()`，函数局部槽位仍为 7、栈大小仍为 5。等待者路径省去了生成器对象和逐项 yield，仍只创建最终结果列表，没有连续 `+` 的中间列表。

普通缓存与等待者缓存、匹配和 LRU 算法均未改动。返回列表仍保持 received → 普通 handler → processed 的顺序，不修改缓存中的列表，已捕获的 handler 引用在注销后仍有效。

这次只消除生成器及闭包成本，等待者状态检查和普通链局部变量仍然保留。此前回退的完整来源见[定位报告](./2026-11-20-handler-query-cause.md)。

## 方法

- before 为本次编辑前的单 core 工作区快照；after 只包含上面的循环改动。两版运行时仅 registry.py 一个文件不同，精确补丁和指纹已归档。
- 同一 Python 3.12.12、同一驱动，独立进程串行按 before → after → after → before 运行。每项预热一轮，每个 worker 测九轮，每版十八个样本，分布在两个进程中。
- GC 开启，阈值为 [700,10,10]，计时前 gc.collect()。端到端性能 worker 的 asyncio debug 为 false；正确性测试单独开启 debug。
- 先跑下表九项共同场景，再串行跑三项等待者查询。两组计时互不并行。

源码指纹排除版本文件：

| 版本 | SHA256 |
| --- | --- |
| before | e4a85c4a3fed1c66a6e1234c691b6118a375dc5bbc98b0c1800b342552d389b7 |
| after | 43bc003176db519ef0b4d390375f6de2516bb1fe8669589a7e186eb28a7f7ea8 |

## 查询与端到端对照

| 场景 | before ms | after ms | 耗时变化 |
| --- | ---: | ---: | ---: |
| 普通热查询，1,000,000 次 | 123.967 | 112.824 | -8.99% |
| 普通冷查询，100,000 次 | 46.194 | 45.055 | -2.47% |
| 缓存 pipe 发布，32,768 个事件 | 221.269 | 222.051 | +0.35% |
| 请求响应，2,048 次 | 181.948 | 176.826 | -2.82% |
| 混合监听，等待者 0% | 36.021 | 36.097 | +0.21% |
| 混合监听，等待者 25% | 116.056 | 115.873 | -0.16% |
| 混合监听，等待者 50% | 175.112 | 175.533 | +0.24% |
| 混合监听，等待者 75% | 234.671 | 235.143 | +0.20% |
| 混合监听，等待者 100% | 312.454 | 312.338 | -0.04% |

普通热查询每次约 123.97 → 112.82 ns，冷查询约 461.94 → 450.55 ns，两项均减少约 11 ns。冷查询预先注册 512 个精确 handler，再查十万个不匹配的新名称，缓存容量 8,192，包含淘汰。

混合监听每轮 512 个事件、64 个总监听，覆盖 exact/glob、received/processed，等待者逐事件注册和清理。所有 worker 的缓存审计一致：普通链仅构建一次，回调/结果数量正确，剩余等待者为零。

缓存 pipe 与混合监听变化均在约 0.4% 内，本轮没有发现明显端到端回退。请求响应的 -2.82% 是本轮观测，不把它全部归为循环改写的稳定收益，也不将查询微基准的比例外推为完整请求收益。

## 含等待者的对象查询

补充微基准直接注册普通 handler 和等待监听，固定命中名称，仅测查询与列表组装，不执行 handler 或等待 Future。断言顺序、缓存列表不变、普通链复用，以及注销后已捕获引用仍有效。

| 普通 / received / processed 数量 | 每轮查询次数 | before ns/次 | after ns/次 | 耗时变化 |
| --- | ---: | ---: | ---: | ---: |
| 1 / 1 / 1 | 1,000,000 | 325.66 | 206.65 | -36.54% |
| 32 / 16 / 16 | 300,000 | 987.91 | 631.78 | -36.05% |
| 0 / 32 / 32 | 1,000,000 | 363.71 | 248.36 | -31.71% |

纯等待者场景也省去了原先普通链为空时仍创建的生成器。以上负载不代表任意链长度；没有新增缓存或改变列表所有权来换取这些结果。

## 正确性与复现

`.venv/bin/python -m pytest -q --asyncio-debug`：**1440 passed，81.86 s**。现有回归覆盖顺序、缓存隔离、快照引用、等待者超时/取消/清理、背压及 graph 交互。

在源码副本上反向应用归档补丁可得到 before，使用当前 src 作为 after。性能进程应串行运行：

```bash
env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/benchmark_runtime.py \
  --baseline-source /tmp/apixis-handler-before/src --output /tmp/handler-optimization.json \
  --repeats 9 --events 32768 --registry-hot-queries 1000000 --registry-cold-queries 100000 \
  --mixed-listeners 64 --mixed-events 512 \
  --scenario registry/hot --scenario registry/cold --scenario event/cached-pipe \
  --scenario event/request-reply --scenario event/mixed-wait-0 --scenario event/mixed-wait-25 \
  --scenario event/mixed-wait-50 --scenario event/mixed-wait-75 --scenario event/mixed-wait-100

env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 .venv/bin/python \
  docs/testing/performance/data/2026-11-20-single-core/benchmark_handler_queries.py \
  --source src --label after --repeats 9 --output /tmp/waiter-query-after.json
```

第二个命令是单 worker，配对复现时按 before/after/after/before 指定对应源码，各输出不同文件。

- [九项共同场景和全部 worker 样本](../testing/performance/data/2026-11-20-single-core/handler-query-optimization.json)
- [含等待者查询和全部 worker 样本](../testing/performance/data/2026-11-20-single-core/handler-query-optimization-waiters.json)
- [精确补丁](../testing/performance/data/2026-11-20-single-core/handler-query-optimization.patch)与[源码、驱动和正确性核对](../testing/performance/data/2026-11-20-single-core/handler-query-optimization-proof.json)
- [共同驱动](../testing/performance/data/2026-11-20-single-core/driver.py)、[等待者查询驱动](../testing/performance/data/2026-11-20-single-core/benchmark_handler_queries.py)与[文件校验](../testing/performance/data/2026-11-20-single-core/manifest.json)

[性能说明](../testing/performance/README.md) · [优化前的回退定位](./2026-11-20-handler-query-cause.md)
