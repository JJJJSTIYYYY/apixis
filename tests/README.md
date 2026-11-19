# 测试

所有命令均在仓库根目录执行。先安装开发依赖：

```bash
uv sync --extra dev --locked
```

## 组织

| 目录 | 范围 |
| --- | --- |
| `core/event` | 事件、handler、订阅、队列、跨线程等待、背压与生命周期的组件契约 |
| `core/config`、`core/utils` | 配置与日志 |
| `integration/event` | 大规模通配符匹配与完整事件分发的正确性 |
| `performance` | 真实工作线程的事件压力测试 |

`tests/conftest.py` 集中管理每个线程 asyncio runtime 的隔离与清理，以及等待前台分发完成的 helper。根目录 `conftest.py` 统一提供测试筛选与性能参数。

## 正确性测试

```bash
uv run --no-sync pytest
uv run --no-sync pytest -m unit
uv run --no-sync pytest -m integration
uv run --no-sync pytest tests/integration/event
```

默认不收集性能测试，原有规模正确性测试仍参与默认测试。CI 在默认测试后额外运行小规模线程压力用例，验证队列和监听清理。

背压边界回归位于 `integration/event/test_backpressure_boundaries.py`，使用实际前台容量 `1/2/3` 与队列容量 `1/2/3` 的九种交叉组合，检查满队列、并发请求响应、取消、重启以及许可归还。排查死锁时可在异步 deadline 之外使用进程级 watchdog。

谓词匹配回归位于 `core/event/test_waiter_predicate.py`，覆盖同名并发回包、重叠模式、延迟收集、异常清理及谓词在选定 core 的 loop 执行。`core/event/test_runtime_status.py` 检查不可变状态快照、查询无启动副作用、许可与挂起任务的区别，以及暂停和桥接队列满时的状态。

## 跨线程事件验证

`core/event/test_multicore.py` 验证命名 core 隔离、getter 不串 loop、多目标结果、超时与取消清理、A→B→A 在容量 1 下归还各自许可。普通接口操作本地 core。跨线程用例使用显式 `*_threadsafe(thread_ids=...)`。

`core/event/test_thread_events.py` 覆盖注册就绪、超时重设、取消、容量等待和停止／重启；`core/event/test_thread_deadlocks.py` 固定派发容量和两条队列容量为 `1`，在默认和 eager task factory 下检查 handler 内的线程版等待、满队列注册、默认线程池被占满，以及取消时的许可恢复和容量通知转交。

`integration/event/test_thread_event_routing.py` 用真实线程验证本地及跨线程发布与监听的目标匹配，并覆盖 `received`、`processed` 和容量 1 下的远程请求与本地回包。发布和监听选择同一个 core 时收到原事件，选择不同 core 时超时并清理监听。

`core/event/test_multicore_deadlocks.py` 验证默认桥接调用可重入同一接收端、默认／eager 调度下多个真实 core 的并发 A→B→A 往返、目标停止与多目标发布取消。应用边界测试对照 `to_thread()` 来源许可未挂起／已挂起的行为，并验证应用持锁时框架借出许可仍不能解除资源环，取消恢复许可也可能等待应用解除依赖。保证范围见[死锁保证边界](../docs/core/event/README.md#框架与应用的死锁保证边界)。

```bash
uv run --no-sync pytest tests/core/event/test_thread_events.py \
  tests/core/event/test_thread_deadlocks.py tests/core/event/test_multicore.py \
  tests/core/event/test_multicore_deadlocks.py tests/integration/event/test_thread_event_routing.py \
  -q -o asyncio_debug=true
```

真实线程压力测试位于 `performance/test_thread_events.py`，包含逐生产者顺序发布、请求／回复、广播与取消风暴、工作线程 loop 内并发发布四种负载。每种负载分别使用派发容量 `1`、`128`、`None`，共十二组。队列默认容量为 `64`，可通过 `--thread-queue-size` 同时调整事件队列与桥接队列。

只验证派发容量和两条队列容量均为 `1`，使用 128 个工作线程、每线程 128 次操作：

```bash
uv run --no-sync pytest tests/performance/test_thread_events.py \
  --run-performance -k '[1]' --thread-workers 128 --thread-iterations 128 \
  --thread-queue-size 1 -o asyncio_debug=true -q -s \
  --thread-report test-results/thread-capacity-one.json
```

去掉 `-k '[1]'` 可验证全部三档派发容量。结束时检查处理结果、许可恢复、临时监听、桥接请求、异步容量等待和 loop callback 错误。线程压力指标写入 `--thread-report` 指定的 JSON。

## 无 GIL 稳定性验证

使用 Python 3.14 的 free-threaded 构建，并在隔离环境中运行：

```bash
PYTHONASYNCIODEBUG=1 uv run --isolated --python 3.14t --extra dev --locked \
  pytest --require-free-threading -q -o faulthandler_timeout=60
```

`--require-free-threading` 在收集前检查解释器支持 free threading 且 GIL 已关闭，退出时再次检查，防止依赖导入后重新启用 GIL 导致验证失真。普通解释器仍可运行全部测试，但不能通过此选项的检查。环境变量 `PYTHONASYNCIODEBUG=1` 同时覆盖主线程和工作线程创建的 loop；`faulthandler_timeout` 只输出阻塞线程栈，不会终止进程。

CI 在 Python 3.14t 上运行完整正确性测试，并用跨线程事件压力测试覆盖派发与两条队列容量均为 1 的情况。Linux 使用进程级 `timeout`，为死锁场景提供退出边界。JUnit XML 保存验证结果，线程压力指标另外保存为 JSON。

## 跨版本性能对比

`scripts/benchmark_runtime.py` 使用四个独立进程按 `before → after → after → before` 比较。每个场景先预热，再测量。结果和原始轮次分别保存为 JSON。

```bash
mkdir -p /tmp/apixis-before
git archive 9743814 source | tar -x -C /tmp/apixis-before
.venv/bin/python scripts/benchmark_runtime.py \
  --baseline-source /tmp/apixis-before/source \
  --output /tmp/apixis-comparison.json --repeats 3
```

上述历史提交使用 `source/` 目录，所以解压路径保留原名；当前工作区代码由脚本从 `src/` 读取。

基准包含精确事件名的冷/热查询、临时订阅增删、事件发布、请求响应和多种等待与订阅比例。两版使用相同参数并逐个校验结果。运行时记录总耗时、事件循环延迟等指标，内存由 tracemalloc 测量。历史摘要与结果见[性能测试记录](../docs/performance/README.md)。

支持线程接口的版本还包含六个 `thread/` 场景：逐次异步发布、请求响应和工作线程内的 asyncio 并发发布，分别使用容量 1 和 64 的桥接队列。默认 32 个线程、每线程 64 次操作，可通过 `--thread-workers` 和 `--thread-iterations` 调整；事件管道容量与桥接队列容量分别记录。计时包含线程池创建、提交、派发和等待剩余任务结束，并校验无丢失、重复或残留监听器。worker 的默认 executor 大小作为环境参数记录，桥接入队本身不使用该 executor。

跨线程比较的基线应支持相同的 `await` / `async with` 接口；历史同步线程接口的版本需要使用与其匹配的驱动，不能直接用当前线程 workload 比较。

```bash
.venv/bin/python scripts/benchmark_runtime.py \
  --baseline-source /tmp/apixis-before/src \
  --output test-results/thread-bridge-abba.json --repeats 9 \
  --thread-workers 32 --thread-iterations 64 \
  --scenario thread/publication-queue-1 \
  --scenario thread/publication-queue-64 \
  --scenario thread/request-reply-queue-1 \
  --scenario thread/request-reply-queue-64 \
  --scenario thread/async-publication-queue-1 \
  --scenario thread/async-publication-queue-64
```

如果使用 pip 安装开发依赖，将以上 `uv run --no-sync pytest` 替换为 `python -m pytest` 即可。
