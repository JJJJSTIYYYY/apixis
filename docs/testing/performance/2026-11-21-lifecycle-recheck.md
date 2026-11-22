# 2026-11-21 生命周期重构性能复测

相对 `f09f638`，本报告冻结的 after 快照存在明确的生命周期操作耗时增加：独立复测中，start/stop 每轮约 39.35 → 156.85 微秒，创建加关闭约 44.04 → 53.95 微秒。常规事件发布、请求响应和 handler 热/冷查询的主对照变化均在约 ±1% 内。固定状态图、注册/注销存在约 1.5%–1.6% 的小幅耗时增加。

after 是 2026-11-21 测量时冻结的运行时，其源码指纹与后续提交 `a93d34e` 一致（排除 `_version.py`），不是最新工作区。比较包含相对基线的全部运行时差异，不能把差值仅归于两处冗余清理。测量过程中只修改基准工具、保存报告，未修改被测运行时。

## 测量方式

- 基线从 `git archive f09f638 src` 提取；当时 after 源码单独复制并冻结。每组都使用同一驱动，按 before → after → after → before 串行运行四个独立进程，各组依次执行。
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
- [25 项主对照与全部 worker 样本](data/2026-11-21-lifecycle-recheck/main.json)
- [五项独立复测](data/2026-11-21-lifecycle-recheck/recheck.json)
- [复用实例注册/注销](data/2026-11-21-lifecycle-recheck/registration.json)、[新对象逐对计时](data/2026-11-21-lifecycle-recheck/registration-fresh.json)
- [缓存审计](data/2026-11-21-lifecycle-recheck/cache-audit.json)、[环境与指纹](data/2026-11-21-lifecycle-recheck/validation.json)
- [共同驱动](data/2026-11-21-lifecycle-recheck/driver.py)、[复用实例驱动](data/2026-11-21-lifecycle-recheck/registration_probe.py)、[新对象驱动](data/2026-11-21-lifecycle-recheck/registration_fresh_probe.py)
- [运行时相对基线的完整补丁](data/2026-11-21-lifecycle-recheck/runtime.patch)、[文件校验](data/2026-11-21-lifecycle-recheck/manifest.json)

## 复现

以下命令在仓库根目录执行，先从 `f09f638` 和归档补丁重建两版源码，再将三个归档驱动放在 after 树的 `scripts/` 中。比较入口根据驱动相邻的 `src/` 选择 after，因此不能直接在归档数据目录启动配对比较，也不能用当前 `scripts/benchmark_runtime.py` 替代归档驱动。

```bash
apixis_perf_root=$(mktemp -d /tmp/apixis-lifecycle-perf.XXXXXX)
mkdir -p "$apixis_perf_root/before" "$apixis_perf_root/after/scripts"
git archive f09f638 src | tar -x -C "$apixis_perf_root/before"
git archive f09f638 src | tar -x -C "$apixis_perf_root/after"
git -C "$apixis_perf_root/after" apply \
  "$PWD/docs/testing/performance/data/2026-11-21-lifecycle-recheck/runtime.patch"
cp docs/testing/performance/data/2026-11-21-lifecycle-recheck/driver.py \
  "$apixis_perf_root/after/scripts/benchmark_runtime.py"
cp docs/testing/performance/data/2026-11-21-lifecycle-recheck/registration_probe.py \
  "$apixis_perf_root/after/scripts/registration_probe.py"
cp docs/testing/performance/data/2026-11-21-lifecycle-recheck/registration_fresh_probe.py \
  "$apixis_perf_root/after/scripts/registration_fresh_probe.py"

env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0 \
  .venv/bin/python "$apixis_perf_root/after/scripts/benchmark_runtime.py" \
  --baseline-source "$apixis_perf_root/before/src" \
  --output "$apixis_perf_root/main.json" \
  --repeats 7 --count 512 --nodes 16 --events 32768 \
  --registry-hot-queries 1000000 --registry-cold-queries 100000 \
  --mixed-listeners 64 --mixed-events 512
```

重建 after 的运行时指纹应为 `15e276d137e09fbe593ad85893980b4ba814f35ace44fdbbcd7513650a51363c`，before 应为 `337c7d0d800df33aaeb76b45de1a760d7111c230b0c7f4f148c019ad9cca16f9`。算法按相对 `src/` 的文件名排序，对每个 Python 文件的相对路径和内容依次更新 SHA-256，跳过 `_version.py`；worker 的 `environment` 会记录同一指纹及驱动指纹。也可用 `git archive a93d34e src` 提取等价 after，仍须使用本报告的归档驱动。

在同一 shell 继续运行独立复测和注册负载，逐组串行执行：

```bash
env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0 \
  .venv/bin/python "$apixis_perf_root/after/scripts/benchmark_runtime.py" \
  --baseline-source "$apixis_perf_root/before/src" \
  --output "$apixis_perf_root/recheck.json" \
  --repeats 15 --count 512 --nodes 16 --events 32768 \
  --registry-hot-queries 1000000 --registry-cold-queries 100000 \
  --mixed-listeners 64 --mixed-events 512 \
  --scenario state/fixed-small --scenario sleep/capacity-512 \
  --scenario lifecycle/start-stop --scenario lifecycle/create-close \
  --scenario lifecycle/close-waiters

env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0 \
  .venv/bin/python "$apixis_perf_root/after/scripts/registration_probe.py" \
  --baseline-source "$apixis_perf_root/before/src" \
  --output "$apixis_perf_root/registration.json" \
  --repeats 9 --scenario registry/mutation

env -u PYTHONASYNCIODEBUG PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0 \
  .venv/bin/python "$apixis_perf_root/after/scripts/registration_fresh_probe.py" \
  --baseline-source "$apixis_perf_root/before/src" \
  --output "$apixis_perf_root/registration-fresh.json" \
  --repeats 9 --scenario registry/mutation
```

每个比较 JSON 内嵌四个 worker 的完整样本。新测量应单独保存并比较源码与驱动指纹，不能期待不同机器、Python 或系统负载下复现同一耗时数值。

[性能说明](./README.md) · [生命周期设计](../../development/runtime-lifecycle.md)
