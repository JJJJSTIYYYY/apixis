# 2026-11-20 handler 查询回退定位

热查询和包含 LRU 淘汰的冷查询，新增成本都集中在 `get_handlers_for_event()` 的对象列表组装入口。普通匹配和 LRU 算法保持一致。这个回退随 `9227e95` 的等待者缓存隔离引入，初始单 core 重建继承了这段实现。

本文的“当前工作区”指优化前快照，定位阶段只补充诊断与文档。后续已用显式循环消除生成器及闭包成本，修复与复测见[查询入口优化](./2026-11-20-handler-query-optimization.md)。

## 提交来源与对照

分别提取 `f8e885d`、首次加入多线程的 `51f7bea`、等待者缓存优化的 `9227e95`，以及当前工作区的源码。八个独立进程串行按四版正序、倒序运行；每个负载预热一轮，再测五轮，每版合计十个样本。使用同一 Python 3.12.12，GC 开启，每轮计时前 gc.collect()，异步测量显式关闭 debug。

| 源码 | 对象热查询 ns/次 | 对象冷查询 ns/次 | 普通链热查询 ns/次 | 普通链冷查询 ns/次 |
| --- | ---: | ---: | ---: | ---: |
| f8e885d | 106.53 | 428.45 | 56.63 | 394.23 |
| 51f7bea | 107.19 | 431.49 | 57.22 | 389.22 |
| 9227e95 | 123.17 | 455.62 | 56.66 | 389.33 |
| 当前工作区 | 123.82 | 455.69 | 57.00 | 392.87 |

对象查询使用派发实际调用的 `get_handlers_for_event()`。普通链查询直接调用普通订阅的缓存方法：旧版的 `get_handlers_chain_for_event()`、新版的 `get_subscription_chain_for_event()`。没有经过新版包含等待者名称的同名包装方法，因此这项回退不能解释为多了一层 Python 方法调用。

热查询每轮一百万次，命中同一个精确 handler；冷查询每轮十万次，预先注册 512 个精确 handler，再查询互不重复、均不匹配的新名称。所有查询均无等待监听，与[上一轮长查询负载](./2026-11-20-single-core-recheck.md#长查询复测与定位)一致。

源码核对同时确认：

- `f8e885d` 与 `51f7bea` 的整个 handler registry 文件 Git blob 相同。
- 忽略方法重命名与 docstring 后，各版普通缓存查询及 `_resolve_handler_chain()` 的可执行 AST 相同。
- `9227e95` 与当前 `get_handlers_for_event()` 的可执行 AST 相同。

## 生成器让未执行的分支也产生开销

[优化前入口](./data/2026-11-20-single-core/handler-query-source-proof.json)的关键部分是：

```python
chain = self.get_subscription_chain_for_event(event_name)
if not self._waiter_cache.positions:
    return [self.registry[name] for name in chain]
received, processed = self._waiter_cache.get(event_name)
handlers = received.copy()
handlers.extend(self.registry[name] for name in chain)
```

最后一行生成器捕获 `self`，Python 编译器因此将整个函数的 `self` 放进闭包 cell。即使没有等待者、提前返回，这次调用仍以 `MAKE_CELL self` 开始，随后使用 `LOAD_DEREF` 读取 `self`。

旧版只有一个列表推导式。在本次 Python 3.12 下，列表推导式内联，不需要捕获 `self`；旧版 `co_cellvars=()`，新版为 `('self',)`。所以“没有进入等待者分支”并不等于“没有付出该分支引起的编译开销”。

此外，普通查询增加了 `_waiter_cache.positions` 的两次属性读取与分支，以及先保存再使用 `chain` 的局部变量操作。上一轮把整个入口的约 17 ns 增量笼统归为状态检查，不够准确；生成器引入的闭包也是原因。

## 隔离测量

在当前源码的独立诊断进程中临时替换方法，断言等待监听为空。六种写法正序、倒序各测一组，每组预热后七轮，每种合计十四个样本。退出时恢复原方法，未写回源码。

| 写法 | 热查询 ns/次 | 冷查询 ns/次 | self 为 cell |
| --- | ---: | ---: | --- |
| 当前实现 | 125.45 | 470.91 | 是 |
| 仅把后续生成器改为内联列表推导式 | 119.90 | 460.05 | 否 |
| 仅将无等待者条件固定为真 | 122.39 | 459.90 | 是 |
| 同时消除闭包与状态检查 | 114.75 | 447.40 | 否 |
| 仅保留普通查询，chain 使用局部变量 | 115.37 | 447.36 | 否 |
| 仅保留普通查询，直接内联取链 | 113.06 | 444.00 | 否 |

在仍保留闭包的条件下，跳过状态检查减少约 3.1 ns 热查询、11.0 ns 冷查询；进一步消除闭包减少约 7.6 ns、12.5 ns。最后两种写法显示局部 `chain` 还有约 2–3 ns 的影响。不同写法的栈与局部槽位存在差异，不能把这些中位数差值当作可精确相加的指令成本。

这些替换用于定位原因。固定条件只在本探针的空等待者前提下成立；生成器改列表推导式也会改变有等待者路径的临时分配，不能直接当作完整修复方案。

## 冷查询没有增加 LRU 工作

另一次不计时的插桩统计中，所有版本、所有 worker 的十万次冷查询均为：

| 操作 | 次数 |
| --- | ---: |
| 普通缓存 get | 100,000 |
| 普通链重建 | 100,000 |
| move_to_end | 100,000 |
| popitem 淘汰 | 91,808 |
| 最终缓存条目 | 8,192 |
| 等待者缓存 get | 0 |

没有第二次等待者 LRU 查询，也没有增加匹配遍历、普通链重建或淘汰次数。直接测普通链时也没有相应退化。冷查询的增量主要来自外层入口；因为冷查询本身包含匹配、建链和淘汰，其相对回退小于热查询。

范围限于当前微基准：精确订阅、无等待者，冷查询结果为空。没有据此推断大规模通配符或非空长链的全部成本。

## 请求响应收益的归属

此前的约 24% 是当前工作区相对 `f8e885d` 的累计差异，不能全部归给这次单 core 重建。等待者独立缓存避免临时监听反复使普通链失效，是已经保留的历史优化。

同一诊断驱动每轮执行 2,048 次请求响应、使用固定回包名和 processed 等待点，得到两组补充结果：

| 对照 | 基线 ms | 目标 ms | 耗时变化 |
| --- | ---: | ---: | ---: |
| f8e885d → 9227e95，四版本正倒序中的样本 | 234.80 | 222.65 | -5.17% |
| c838c16 → 7b05584，另外四进程 ABBA | 216.87 | 178.23 | -17.81% |

`9227e95` 同时包含缓存隔离、去掉冗余 shielding、延迟创建超时通知等变化，第一行不是“仅缓存”的独立收益。`7b05584` 的单 Future 改动将本地 `wait()` 的 `asyncio.wait((result, timeout))` 改为直接等待结果 Future，即使没有配置超时，也省去了原有等待协调成本。第二行说明它也贡献了明显收益。两组来自分别运行的对照，不把百分比相加。

## 复现与证据

准备相应提交及本文优化前工作区的 src 快照后，在仓库根目录串行执行；不要并行跑性能 worker。若从已优化的源码准备工作区快照，可在副本上反向应用[后续优化补丁](./data/2026-11-20-single-core/handler-query-optimization.patch)：

```bash
env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 .venv/bin/python \
  docs/performance/data/2026-11-20-single-core/handler_query_probe.py \
  --source /tmp/apixis-perf-f8e885d/src --label f8e885d \
  --mode history --repeats 5 --output /tmp/handler-f8e885d.json

env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 .venv/bin/python \
  docs/performance/data/2026-11-20-single-core/handler_query_probe.py \
  --source /tmp/apixis-handler-before/src --label current --mode ablation --repeats 7 \
  --output /tmp/handler-ablation.json
```

`history` 包含四种查询、路径计数和请求响应；`reply` 只测请求响应。四版本运行顺序及 ABBA 次序已记录在对应 JSON；ablation 自行按正倒序运行六种写法。

- [跨提交全部样本、指纹与汇总](./data/2026-11-20-single-core/handler-query-history.json)
- [隔离写法、反汇编与全部样本](./data/2026-11-20-single-core/handler-query-ablation.json)
- [单 Future 前后请求响应的全部样本](./data/2026-11-20-single-core/handler-query-reply.json)
- [规范化 AST、历史源码与路径计数](./data/2026-11-20-single-core/handler-query-source-proof.json)
- [诊断脚本](./data/2026-11-20-single-core/handler_query_probe.py)与[文件校验](./data/2026-11-20-single-core/manifest.json)

原始 worker 的 public_code.signature 含嵌套 code object 的进程内 repr，不用于跨进程相等判断；源码相等性以上述 AST 核对为准。

[性能核对](./2026-11-20-single-core-recheck.md) · [性能说明](./README.md)
