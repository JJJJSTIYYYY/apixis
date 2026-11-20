# 2026-11-20 共享 core 重建后的性能核对

当前工作区相对 `f8e885d` 存在可复现的 registry 查询微基准回退。实际事件发布、请求响应和含等待者的混合监听更快，graph 与状态快照变化幅度较小，细项见下表。本轮没有修改运行时代码。

本报告的“当前工作区”指查询入口优化前的快照。后续消除生成器及闭包成本的改动和配对结果见 [handler 查询入口优化](./2026-11-20-handler-query-optimization.md)，以下历史样本保持原值。

## 主要发现

| 项目 | 耗时变化 | 判断 |
| --- | ---: | --- |
| registry 热查询，放大到每轮 1,000,000 次 | +15.84% | 两次 ABBA 都出现回退，单次增加约 16.8 ns |
| registry 冷查询，放大到每轮 100,000 次 | +5.59% | 两次 ABBA 都出现回退，单次增加约 24.8 ns |
| 缓存 pipe 事件发布 | -8.71% | 相对 f8e885d 更快 |
| 全局 post_event | -9.32% | 相对首次引入该接口的 5049c7d 更快 |
| 请求/响应 | -24.00% | 历史等待优化与当前改动的累计结果，不能全部归给本轮重建 |
| 等待者占 25%–100% 的混合监听 | -34.45% 至 -42.16% | 更快，普通订阅链不再随临时监听反复重建 |
| graph 四种场景 | -1.05% 至 +0.72% | 约 1% 量级的小幅变化 |
| 状态及快照三种场景 | -0.85% 至 +1.32% | 约 1.3% 量级的小幅变化 |

正数表示耗时增加，负数表示耗时减少。百分比描述本机本轮负载，不是跨硬件保证。

## 方法与范围

- 主基线为 `5049c7d` 的父提交 `f8e885d`，目标为当前包含未提交重建改动的工作区。源码指纹排除版本文件。
- 两版使用同一 Python 3.12、同一驱动和输入，按 before → after → after → before 启动四个独立进程。每个场景先预热，再测量；正式轮次之间执行 gc.collect()，计时期间 GC 保持开启。
- 主比较每个 worker 测量 5 轮，每版共 10 个样本、21 项共同场景。长查询复测每版 30 个样本；全局发布补充对照每版 20 个样本。重复轮次分布在每版两个进程中，没有将同进程重复当成大量独立实验来声称统计显著。
- 显式清除 PYTHONASYNCIODEBUG，所有 worker 记录 asyncio_debug=false、gc_enabled=true、GC 阈值 [700,10,10]。性能测量与上一轮 debug 正确性测试分开。
- 主 graph 负载为 512 次调用、每次 16 个节点。状态场景及异步等待使用驱动内固定负载，详见 JSON 的 workload。事件队列容量 65,536，普通事件/graph 派发容量 128，异步等待另测 512/1024。
- registry 热查询为单个精确 handler 的缓存命中；冷查询是在已有 512 个精确 handler 后逐个查询未匹配的新名称。两者均无等待监听，不代表所有通配符匹配规模。
- 混合监听总数固定 64，分别包含精确/通配符、received/processed；每轮 512 个事件。结果、节点执行、许可清理、等待监听残留和缓存构建次数均有断言。
- f8e885d 尚无全局 post_event，正式主比较显式只选择两版共同的 21 项，执行顺序一致，排除新版独有负载。全局发布额外对照 5049c7d，每轮发布 65,536 个事件。

## 21 项主比较

| 场景 | 基线耗时 ms | 当前耗时 ms | 变化 |
| --- | ---: | ---: | ---: |
| registry 冷查询，2,000 次 | 0.816 | 0.926 | +13.50% |
| registry 热查询，30,000 次 | 2.998 | 3.640 | +21.41% |
| registry 注册/注销，单次微测量 | 0.003 | 0.003 | -2.06% |
| 共享图，串行 | 299.140 | 301.299 | +0.72% |
| 独立图，串行 | 383.218 | 379.188 | -1.05% |
| 共享图，并行 | 146.002 | 146.305 | +0.21% |
| 独立图，并行 | 210.192 | 208.664 | -0.73% |
| 小状态，固定字段 | 213.945 | 216.779 | +1.32% |
| 大状态，保留快照 | 200.511 | 200.592 | +0.04% |
| 大状态，关闭自动快照 | 109.016 | 108.085 | -0.85% |
| 异步等待，派发容量 128 | 737.319 | 740.257 | +0.40% |
| 异步等待，派发容量 512 | 418.105 | 405.081 | -3.12% |
| 异步等待，派发容量 1024 | 363.554 | 353.486 | -2.77% |
| 缓存 pipe 发布事件 | 251.331 | 229.439 | -8.71% |
| 请求/响应，2,048 次 | 237.191 | 180.264 | -24.00% |
| getter，200,000 次 | 18.875 | 17.418 | -7.71% |
| 混合监听，等待者占 0% | 36.563 | 36.677 | +0.31% |
| 混合监听，等待者占 25% | 181.478 | 118.960 | -34.45% |
| 混合监听，等待者占 50% | 283.291 | 180.062 | -36.44% |
| 混合监听，等待者占 75% | 413.577 | 239.202 | -42.16% |
| 混合监听，等待者占 100% | 513.494 | 320.738 | -37.54% |

以上为各版正式轮次的中位数。registry 注册/注销单次仅数微秒，不根据其百分比判断稳定收益；异步等待受系统定时器和调度影响，也不把小幅负值当作固定收益。原始进程中位数和范围保留在 JSON 中。

## 长查询复测与定位

| 场景 | 基线耗时 ms | 当前耗时 ms | 变化 |
| --- | ---: | ---: | ---: |
| registry/cold | 44.294 | 46.772 | +5.59% |
| registry/hot | 105.934 | 122.716 | +15.84% |

主比较热查询观察到 +21.41%、冷查询 +13.50%；长查询比较分别为 +15.84%、+5.59%。两轮中，当前版两个进程的查询中位数都高于基线两个进程。短冷查询只填充 2,000 个名称，长冷查询超过 8,192 的缓存上限、包含 LRU 淘汰，两轮比值对应不同缓存压力；热查询始终是同名命中。

[get_handlers_for_event()](../../src/apixis/core/event/handler_registry/registry.py#L249) 为统一普通订阅和等待监听，增加了等待缓存状态读取、分支和局部变量。后续等待者分支中的生成器还使 self 成为闭包 cell；即使没有等待监听，也会付出 cell 初始化与读取成本。这些变化由历史提交 9227e95 引入，当前重建继承了该实现。

最初的隔离探针在独立进程、已断言等待监听为空的 registry 中，临时替换为只获取普通订阅的等价路径，按 ABBA 测量，每次查询减少约 17.2 ns。这同时去掉了状态检查、闭包和局部变量，不能将整个差值只归为一次条件检查。进一步的逐项隔离、历史提交定位与 LRU 操作计数见 [handler 查询回退定位](./2026-11-20-handler-query-cause.md)。这些替换未写回运行时代码。

这项查询级成本未在本轮端到端负载中形成退化：事件发布、请求响应和等待混合负载的总耗时仍下降。此结论限于已测工作负载，不据此保证其他订阅规模或容量下的结果。

## 复现

在仓库根目录准备基线并运行；比较进程不要并行执行：

```bash
mkdir -p /tmp/apixis-perf-f8e885d
git archive f8e885d src | tar -x -C /tmp/apixis-perf-f8e885d
env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/benchmark_runtime.py \
  --baseline-source /tmp/apixis-perf-f8e885d/src --output /tmp/apixis-perf-main.json \
  --repeats 5 --count 512 --nodes 16 --events 32768 --mixed-listeners 64 --mixed-events 512 \
  --scenario registry/cold --scenario registry/hot --scenario registry/mutation \
  --scenario graph/shared-chain --scenario graph/independent-chain --scenario graph/shared-parallel \
  --scenario graph/independent-parallel --scenario state/fixed-small --scenario state/large-snapshots \
  --scenario state/large-no-snapshots --scenario sleep/capacity-128 --scenario sleep/capacity-512 \
  --scenario sleep/capacity-1024 --scenario event/cached-pipe --scenario event/request-reply \
  --scenario event/getter --scenario event/mixed-wait-0 --scenario event/mixed-wait-25 \
  --scenario event/mixed-wait-50 --scenario event/mixed-wait-75 --scenario event/mixed-wait-100

env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/benchmark_runtime.py \
  --baseline-source /tmp/apixis-perf-f8e885d/src --output /tmp/apixis-perf-registry.json \
  --repeats 15 --registry-hot-queries 1000000 --registry-cold-queries 100000 \
  --scenario registry/hot --scenario registry/cold
```

补充全局发布对照使用同样方式提取 5049c7d，将 repeats 设为 10、events 设为 65536，选择 event/cached-pipe 和 event/global-post。

## 原始证据

- [主比较及全部 worker/轮次](./data/2026-11-20-single-core/main.json)
- [长查询复测](./data/2026-11-20-single-core/registry-long.json)
- [全局发布补充对照](./data/2026-11-20-single-core/global-publication.json)
- [隔离探针数据](./data/2026-11-20-single-core/registry-diagnosis.json)与[脚本](./data/2026-11-20-single-core/diagnose_registry.py)
- [环境、参数、源码指纹](./data/2026-11-20-single-core/metadata.json)
- [三组比较使用的共同驱动](./data/2026-11-20-single-core/driver.py)与[文件校验](./data/2026-11-20-single-core/manifest.json)

比较文件已内嵌所有 worker 数据，worker 文件名仅标记原始阶段，不再重复保存相同样本。归档驱动可用 --worker-source 指定源码进行单 worker 复现；常规配对入口仍为 scripts/benchmark_runtime.py。隔离探针使用 --source src --output /tmp/registry-diagnosis.json。

[性能说明](./README.md) · [共享 core 重建记录](../core/single-core-rebuild.md)
