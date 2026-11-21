# 2026-11-21 生命周期重构性能复测

相对最近提交 `f09f638`，当前工作区存在明确的生命周期操作耗时增加：独立复测中，start/stop 每轮约 39.35 → 156.85 微秒，创建加关闭约 44.04 → 53.95 微秒。常规事件发布、请求响应和 handler 热/冷查询的主对照变化均在约 ±1% 内。固定状态图、注册/注销存在约 1.5%–1.6% 的小幅耗时增加。

本次比较覆盖该提交之后全部未提交的运行时改动，不能把差值仅归于最近两处冗余清理。本轮只修改基准工具、保存测量报告，没有修改运行时代码。

## 测量方式

- 基线从 `git archive f09f638 src` 提取；工作区源码单独复制并冻结。每组都使用同一驱动，按 before → after → after → before 串行运行四个独立进程，各组依次执行。
- Python 3.12.12，macOS arm64，asyncio debug 关闭，GC 开启且阈值为 `[700,10,10]`，`PYTHONHASHSEED=0`。不固定 CPU 核心。
- 主对照 25 项，每个 worker 先预热一轮再测七轮，每版 14 个样本；五项独立复测每个 worker 测 15 轮，每版 30 个样本。注册/注销两组放大复测各为九轮，每版 18 个样本。
- graph、事件、混合监听及生命周期每个正式测量轮次之前调用 `gc.collect()`；registry/getter 在预热后连续采样。
- 热查询每轮一百万次；冷查询十万个不同名称，预先注册 512 个精确 handler，缓存上限 8192，确认包含 LRU 淘汰。发布每轮 32768 个事件，请求响应每轮 2048 次。
- 四种 graph 拓扑使用 512 次并发调用、每次 16 个节点；固定状态为 256 次调用、每次 32 个节点，大载荷场景为 32 次调用、每次 16 个节点和 256 项载荷，分别开启/关闭快照。另测 2048 次四节点调用在 128/512/1024 三档背压下的 10 ms 等待。混合场景每轮 512 个事件、64 个总监听，覆盖 exact/glob、received/processed。
- worker 校验实际调用数、结果、步骤数、快照数、信号量容量与回收、缓存构建次数和剩余监听。共同驱动仅在构造测试 handler 时适配公开列表和私有元组两种历史表示，不修改运行时。

## 主对照

以下均为每轮耗时中位数，单位 ms；正值表示耗时增加。`registry/mutation` 是原有五次单独计时的中位数，其放大复测见后文。两项批量生命周期操作每轮包含 512 对调用，close-waiters 每轮关闭 256 个尚未产生事件的 waiter。

| 场景 | before ms | after ms | 耗时变化 |
| --- | ---: | ---: | ---: |
| registry/cold | 44.450 | 44.642 | +0.43% |
| registry/hot | 112.820 | 113.333 | +0.46% |
| registry/mutation | 0.002792 | 0.003062 | +9.69% |
| graph/shared-chain | 291.296 | 292.590 | +0.44% |
| graph/independent-chain | 361.346 | 363.519 | +0.60% |
| graph/shared-parallel | 142.512 | 143.458 | +0.66% |
| graph/independent-parallel | 201.763 | 201.060 | -0.35% |
| state/fixed-small | 205.286 | 209.969 | +2.28% |
| state/large-snapshots | 199.335 | 200.847 | +0.76% |
| state/large-no-snapshots | 107.335 | 108.470 | +1.06% |
| sleep/capacity-128 | 758.050 | 742.773 | -2.02% |
| sleep/capacity-512 | 383.342 | 408.052 | +6.45% |
| sleep/capacity-1024 | 357.882 | 347.656 | -2.86% |
| event/cached-pipe | 218.544 | 220.455 | +0.87% |
| event/global-post | 226.638 | 227.446 | +0.36% |
| event/request-reply | 177.755 | 177.602 | -0.09% |
| event/getter | 6.948 | 7.115 | +2.39% |
| event/mixed-wait-0 | 36.602 | 36.270 | -0.91% |
| event/mixed-wait-25 | 117.602 | 115.389 | -1.88% |
| event/mixed-wait-50 | 178.787 | 174.388 | -2.46% |
| event/mixed-wait-75 | 260.467 | 231.169 | -11.25% |
| event/mixed-wait-100 | 318.954 | 312.129 | -2.14% |
| lifecycle/start-stop | 20.205 | 79.897 | +295.44% |
| lifecycle/create-close | 22.558 | 27.416 | +21.54% |
| lifecycle/close-waiters | 0.361396 | 0.357125 | -1.18% |

## 独立复测与判断

| 场景 | before ms | after ms | 耗时变化 |
| --- | ---: | ---: | ---: |
| state/fixed-small | 203.217 | 206.175 | +1.46% |
| sleep/capacity-512 | 370.292 | 363.021 | -1.96% |
| lifecycle/start-stop | 20.145 | 80.309 | +298.65% |
| lifecycle/create-close | 22.548 | 27.624 | +22.51% |
| lifecycle/close-waiters | 0.361979 | 0.359125 | -0.79% |

生命周期操作的每对调用耗时（微秒）如下，注册等待器、构造用于循环启停的 core 均在对应计时之外；create-close 则包含组件构造。

| 操作 | before 微秒/对 | after 微秒/对 | 耗时变化 |
| --- | ---: | ---: | ---: |
| lifecycle/start-stop | 39.35 | 156.85 | +298.65% |
| lifecycle/create-close | 44.04 | 53.95 | +22.51% |

start/stop 的两次 ABBA 均接近四倍耗时，create-close 均约增加 22%，属于可复现的回退。结构上，新的共享启动/停止 Task、shield 等待和调度确认增加了操作步骤；这是根据代码路径作出的解释，没有把增量逐项归因到单个机制。

背压 512 的等待负载主对照为 +6.45%，独立复测为 -1.96%，变化方向没有保持，不认定为固定回退。两组原始样本都保留。固定状态图主对照 +2.28%、复测 +1.46%，记录为本轮观测到的小幅耗时增加。

注册/注销额外覆盖两种负载，构造 handler 和填充历史缓存均不计入对应注册/注销计时，缓存持续保留 8192 个名称：

| 负载 | before 微秒/对 | after 微秒/对 | 耗时变化 |
| --- | ---: | ---: | ---: |
| 同一 handler 连续注册/注销，每轮十万对 | 2.230 | 2.266 | +1.62% |
| 新建 handler 后逐对计时，每轮两万对 | 2.500 | 2.541 | +1.64% |

注册/注销的原始少量采样为 +9.69%，在新对象逐对计时放大到两万对后为 +1.64%，复用实例的整批计时为 +1.62%。本轮只确认约 0.04 微秒/对的小幅增加，不沿用初测约 10% 的幅度。

等待者较多的混合监听主对照更快，其中 75% waiter 的场景约减少 11.25%；这些收益未做独立的第二组 ABBA，不外推为任意监听组合的收益。

Python 内存峰值基本一致：大快照约 27.46 MiB、无快照约 3.19 MiB；registry 留存均为 8192 项、约 2.03 MiB。这里测量的是 tracemalloc 的 Python 分配，不是进程 RSS。

## 正确性和证据

- 缓存审计：13 个正常场景、两项受控重注册检查通过，缺陷数为零。
- 所有 performance worker 的结果、容量、缓存及清理断言均通过。测量期间两版源码与共同驱动指纹保持不变。
- [25 项主对照与全部 worker 样本](./data/2026-11-21-lifecycle-recheck/main.json)
- [五项独立复测](./data/2026-11-21-lifecycle-recheck/recheck.json)
- [复用实例注册/注销](./data/2026-11-21-lifecycle-recheck/registration.json)、[新对象逐对计时](./data/2026-11-21-lifecycle-recheck/registration-fresh.json)
- [缓存审计](./data/2026-11-21-lifecycle-recheck/cache-audit.json)、[环境与指纹](./data/2026-11-21-lifecycle-recheck/validation.json)
- [共同驱动](./data/2026-11-21-lifecycle-recheck/driver.py)、[复用实例驱动](./data/2026-11-21-lifecycle-recheck/registration_probe.py)、[新对象驱动](./data/2026-11-21-lifecycle-recheck/registration_fresh_probe.py)
- [运行时相对基线的完整补丁](./data/2026-11-21-lifecycle-recheck/runtime.patch)、[文件校验](./data/2026-11-21-lifecycle-recheck/manifest.json)

## 复现

从 Git 提取基线 src，使用当前工作区或应用 runtime.patch 后的源码作为 after。以下命令在仓库根目录执行；复现归档驱动时，将它放入 after 树的 scripts/benchmark_runtime.py，使其相邻的 src 为被测 after。两个注册驱动同样放在该 scripts 目录。

```bash
env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0 .venv/bin/python scripts/benchmark_runtime.py \
  --baseline-source /tmp/apixis-before/src --output /tmp/apixis-main.json \
  --repeats 7 --count 512 --nodes 16 --events 32768 \
  --registry-hot-queries 1000000 --registry-cold-queries 100000 \
  --mixed-listeners 64 --mixed-events 512
```

独立复测使用相同参数、repeats=15，选择 `state/fixed-small`、`sleep/capacity-512`、`lifecycle/start-stop`、`lifecycle/create-close`、`lifecycle/close-waiters`。两项注册驱动均使用 repeats=9、`--scenario registry/mutation`。原始进程阶段文件的完整内容已内嵌各 comparison JSON，无需单独保留同内容的 worker 文件。

[性能说明](./README.md) · [生命周期契约](../core/runtime-lifecycle.md)
