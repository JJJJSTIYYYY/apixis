# 测试

所有命令均在仓库根目录执行。先安装开发依赖：

```bash
uv sync --extra dev --locked
```

## 组织

| 目录 | 范围 |
| --- | --- |
| `core/event` | 事件、handler、订阅、通道、背压与生命周期的组件契约 |
| `core/graph` | 节点、Command、context、快照、流与图执行的组件契约 |
| `core/config`、`core/utils` | 配置与日志 |
| `integration/event` | 大规模通配符匹配与完整事件分发的正确性 |
| `integration/graph` | 插件、中断、取消、并发、运行期所有权与图分解 |
| `performance` | 大量多节点图同时调度，含建图耗时的性能测试 |

`tests/conftest.py` 集中管理共享 asyncio runtime 的隔离与清理，以及等待前台分发完成的 helper；根目录 `conftest.py` 统一提供测试筛选与性能参数。仅在这些测试基础设施和已有白盒组件测试中检查内部状态；新增性能 workload 通过 `apixis.core` 的公开接口构建、执行和检查图。

## 正确性测试

```bash
uv run --no-sync pytest
uv run --no-sync pytest -m unit
uv run --no-sync pytest -m integration
uv run --no-sync pytest tests/integration/graph
```

默认不收集性能测试，原有规模正确性测试仍参与默认测试。CI 在默认测试后额外运行十三组性能 workload 的小规模版本，验证测试本身能执行和清理，不设与硬件绑定的速度阈值。

背压边界回归位于 `integration/event/test_backpressure_boundaries.py`，使用实际前台容量 `1/2` 与队列容量 `1/2` 的交叉组合，检查满队列、嵌套并行图、请求响应、取消、重启以及许可归还。测试有异步 deadline；排查死锁时应另加进程级 watchdog，避免取消清理本身阻塞测试退出。

## 跨版本性能对比

`scripts/benchmark_runtime.py` 使用四个独立进程按 `before → after → after → before` 比较。每个场景先预热，再测量；结果和原始轮次分别保存为 JSON，不覆盖现有 Graph 性能报告。

```bash
mkdir -p /tmp/apixis-before
git archive 9743814 source | tar -x -C /tmp/apixis-before
.venv/bin/python scripts/benchmark_runtime.py \
  --baseline-source /tmp/apixis-before/source \
  --output /tmp/apixis-comparison.json --repeats 3
```

上述历史提交使用 `source/` 目录，所以解压路径保留原名；当前工作区代码由脚本从 `src/` 读取。

基准包含精确事件名的冷/热查询、临时订阅增删、独立/复用图、串行/并行图、大状态快照、关闭快照，以及三档背压下的异步等待。两版使用相同参数并逐个校验结果。运行时记录总耗时、构建/准备/执行分项、从共同启动门计时的批量完成延迟和事件循环延迟；内存另用 tracemalloc 测量，不能当作进程 RSS。完整场景说明及实测结果见[性能报告](../docs/performance/aduit-2026-10-22.md)。

## Graph 性能测试

```bash
uv run --no-sync pytest tests/performance --run-performance
```

运行结束后自动生成仓库根目录下的 `test-results/README.md`，文件包含本次运行场景的性能结果表格，末尾附上四种立即返回场景及两种等待场景的含义说明，并覆盖上次生成的文档。无需指定 `--graph-report`；该参数用于额外保存 JSON 原始指标。

常规测试使用两档规模：1,000 次调用、每次 16 个节点，以及 3,000 次调用、每次 32 个节点。每档均覆盖四种立即返回场景和一种 1 ms 等待场景，共十组。另有三组 50 ms 等待负载，使用 8,192 次调用、每次 4 个节点，依次比较 128、512、1024 背压；完整运行共十三组：

| 场景 | 建图数量 | 调度方式 |
| --- | ---: | --- |
| `independent/chain` | 1,000 | 每张独立图执行 16 个串行节点 |
| `independent/parallel` | 1,000 | 每张独立图执行 4 层，每层 4 个并发节点 |
| `shared/chain` | 1 | 同一张 16 节点图并发调用 1,000 次 |
| `shared/parallel` | 1 | 同一张 4 层并发图并发调用 1,000 次 |
| `independent/chain` | 3,000 | 每张独立图执行 32 个串行节点 |
| `independent/parallel` | 3,000 | 每张独立图执行 8 层，每层 4 个并发节点 |
| `shared/chain` | 1 | 同一张 32 节点图并发调用 3,000 次 |
| `shared/parallel` | 1 | 同一张 8 层并发图并发调用 3,000 次 |
| `shared/chain/sleep-1ms` | 1 | 同一张 16 节点串行图并发调用 1,000 次，其中 4 个节点等待 1 ms |
| `shared/chain/sleep-1ms` | 1 | 同一张 32 节点串行图并发调用 3,000 次，其中 8 个节点等待 1 ms |
| `shared/chain/sleep-50ms` | 1 | 同一张 4 节点串行图并发调用 8,192 次，所有节点等待 50 ms；背压分别为 128、512、1024 |

每组预热一轮，随后测量三轮；**每轮都重新注册节点并编译图**。同组所有调用通过共同的启动门同时释放；原有四种场景的节点使用同步函数立即返回最小 `Command`，携带下一跳和一个用于事后校验的访问标记。等待场景的第 1、5、9……个节点先执行 `await asyncio.sleep(0.001)`，其余节点仍立即返回。保留默认状态复制、AutoMerge 与快照行为。普通日志及事件分发日志关闭，错误日志保留；默认配置下使用本地运行时，无需外部服务。

仅运行新增的等待场景：

```bash
uv run --no-sync pytest tests/performance --run-performance -k sleeping
```

1 ms 等待场景沿用运行时的 `EVENT_LOOP_BACKPRESSURE`，不自行加载配置或修改背压。修改根目录 `config.yaml` 中的 `PIPELINE.event_loop_backpressure` 后，重新执行上述命令；保持其他参数相同，对比表格中的单轮耗时、吞吐量以及 JSON 中的延迟。表格显示实际信号量容量、每次调用的等待节点数和 `sleep` 参数（1 ms）。

## 一次运行三档背压

```bash
uv run --no-sync pytest tests/performance --run-performance \
  -k backpressure_sweep --graph-report=test-results/graph-performance.json
```

此命令在同一个 pytest 进程、同一个 asyncio 事件循环中依次运行背压 128、512、1024。每档使用同样的负载：同一张 4 节点串行图并发调用 8,192 次，每个节点执行 `await asyncio.sleep(0.050)`；每轮共执行 32,768 个节点。每档预热一轮、正式测量三轮，三行结果写入同一份 `test-results/README.md`，原始轮次数据写入同一份 JSON。

提高调用数和等待时长可让更多节点持续占用事件许可；使用短链可减少不断增长的状态及快照复制对等待成本的掩盖。50 ms 为请求的等待时间，实际唤醒仍受事件循环调度影响。等待属于异步等待负载，并非 CPU 计算负载。

只有这组三档对比在测试 fixture 中临时替换运行时使用的背压常量，并为每档重建 event core 和 `BoundedSemaphore`；测试前检查实际容量，完成后先清理任务再恢复原 runtime。它不重新读取或改写 `config.yaml`，不修改生产代码。原有十组测试继续使用配置模块的值。报告的背压列读取实际信号量上限；JSON 分别记录配置默认值、实际测试的背压列表及每组容量。

同一进程运行可减少分次启动带来的差异，**不会固定 CPU 到某个性能核心或能效核心**，系统仍可能迁移线程。三档固定按 128、512、1024 顺序运行，温度和频率变化仍可能带来顺序偏差。比较时查看各轮原始数据及中位数；需要确认趋势时可重复整组三档测试。不要将三档拆成独立命令或使用 pytest 多进程执行。

负载可以通过原有参数覆盖，例如缩小为 4,096 次调用、每次 4 个节点，保持三档容量一起运行：

```bash
uv run --no-sync pytest tests/performance --run-performance \
  -k backpressure_sweep --graph-count 4096 --graph-nodes 4 --graph-repeats 3
```

## 筛选与指标

例如，只运行 3,000 规模的五组测试，并保存 JSON 指标：

```bash
uv run --no-sync pytest tests/performance --run-performance \
  -k 3000 --graph-width 4 \
  --graph-repeats 3 --graph-timeout 180 \
  --graph-report=graph-performance.json
```

只跑独立图、串行场景或 1,000 规模：

```bash
uv run --no-sync pytest tests/performance --run-performance -k independent
uv run --no-sync pytest tests/performance --run-performance -k chain
uv run --no-sync pytest tests/performance --run-performance -k 1000
```

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `--graph-count` | 常规组 1,000／3,000；三档对比 8,192 | 指定时覆盖所有组的并发调用数；独立图模式的建图数也等于此值 |
| `--graph-nodes` | 常规组 16／32；三档对比 4 | 指定时覆盖所有组的每次调用节点总数 |
| `--graph-width` | 4 | 并发图每层最大节点数；不整除时最后一层使用剩余节点 |
| `--graph-repeats` | 3 | 测量轮数，另有一轮预热 |
| `--graph-timeout` | 120 | 单轮建图至执行结束的超时秒数 |
| `--graph-report` | 无 | JSON 输出路径；保留每轮原始指标及运行环境 |

常规测试 ID 包含默认规模（如 `3000-invocations-32-nodes`），可用 `-k` 筛选；三档对比的 ID 为 `test_backpressure_sweep[128]`、`[512]`、`[1024]`。显式传入 `--graph-count` 或 `--graph-nodes` 会覆盖所有组的对应值；例如 CI 使用 `--graph-count 32 --graph-nodes 8 --graph-repeats 1` 缩小全部十三组 workload。结果表格和 JSON 记录实际运行规模。

表格中的 **总耗时**是该用例的 pytest `setup + call + teardown` 累计时间（JSON 中的 `test_seconds`），包含预热、全部测量轮次、结果校验及清理。默认每组执行一轮预热和三轮测量，因此不能用单轮耗时中位数代替用例总耗时，也不能简单乘以四推算总耗时。

**单轮耗时中位数**是各正式测量轮 `total_seconds` 的中位数。每轮从首次建图开始，到全部调用完成为止，**包含建图、context 创建、任务创建、排队及执行**。吞吐量继续使用每轮框架耗时计算，不将预热、校验或清理混入性能指标；p50/p95/p99 也从同一轮首次建图开始计时，因此包含建图等待。`build_seconds`、`prepare_seconds`、`execute_seconds` 分列对应开销，三项之和等于该轮 `total_seconds`。表格的单轮建图中位数、单轮吞吐量中位数，以及终端的吞吐量和延迟均取正式测量轮的中位数，JSON 保留完整原始值。

每轮结果校验、等待剩余前台分发回调、分解图和测试清理不计入单轮框架耗时，但计入用例总耗时。每轮校验所有 context 的状态、执行步骤、快照数量、run ID 唯一性、命名空间、访问顺序和节点执行总数，避免漏执行产生虚假的高吞吐量。

表格末尾列出 **会话开销**及 **会话合计**。各场景总耗时加会话开销等于会话合计；混合运行普通测试时，另列“其他用例”，同样纳入合计。会话开销包含收集和用例之间的 pytest 调度等，使用实际会话时钟减去各用例的 pytest 阶段耗时计算。会话时钟从 pytest 会话开始记录，在最终结果汇总时取值；最终报告写入及终端汇总在此之后，因此与 pytest 最后一行的耗时可能有毫秒级差异。表格保留三位小数，各行相加还可能有舍入误差。JSON 的 `session` 保留未舍入的会话分项，`pytest_phase_seconds` 保留每个性能用例的 setup/call/teardown 耗时。

原有四种场景的节点不包含 sleep、等待、I/O、业务计算或运行期探针；等待场景加入 1 ms 或 50 ms 的 asyncio 等待。执行次数在计时结束后根据访问标记核对。立即返回基准覆盖框架的建图、调度、Command 合并、状态复制及快照开销。比较结果时使用相同 Python 版本、机器及参数，仅改变背压；并发调用数可以超过 `event_loop_backpressure`，但不代表所有节点都能同时持有事件许可。

如果使用 pip 安装开发依赖，将以上 `uv run --no-sync pytest` 替换为 `python -m pytest` 即可。
