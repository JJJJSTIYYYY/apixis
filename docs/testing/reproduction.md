# 生命周期与问题复现

以下命令在仓库根目录执行，使用已安装开发依赖的 `.venv`。当前回归使用工作区源码；归档探针单独选择其原始基线。

## 当前生命周期与取消回归

```bash
.venv/bin/python -m pytest --asyncio-debug \
  tests/core/event/test_factory.py \
  tests/core/event/test_close_core.py \
  tests/core/event/test_cancellation.py \
  tests/core/event/test_waiter_lifecycle.py \
  tests/integration/graph/test_core_close.py
```

这些用例覆盖显式创建、同步 getter、暂停后恢复、关闭准入、真实事件取消、关闭时直接完成等待器，以及图调用在所属 core 关闭后的结果。`test_close_core.py` 中的 `test_close_stops_handler_phases_after_upstream_hook_swallows_cancellation` 覆盖通知回调吞掉取消后不再进入后续业务阶段，前台、后台和接受事件两个分支共四种组合。

## eager 调度

仓库的归档插件通过 `loop.set_task_factory(asyncio.eager_task_factory)` 切换整个事件循环。它不对应 `subscribe()` 的某个参数；业务接口没有单独配置后台 handler eager 模式的选项。

```bash
PYTHONPATH=docs/testing/performance/data/2026-11-20-lifecycle \
  .venv/bin/python -m pytest -p eager_plugin --asyncio-debug \
  tests/core/event/test_close_core.py tests/core/event/test_cancellation.py
```

全局 eager 调度会让 `create_task()` 在返回前开始执行协程。`tests/core/event/test_handler_execution.py::test_background_handler_checks_acceptance_when_it_starts` 假设后台任务晚于后续前台 accept 开始，不能把这一调度顺序断言直接用于 eager 环境。核对 eager 失败时，应区分业务契约与默认调度顺序，记录实际使用的插件和选定用例。

## L1 回调吞掉关闭取消

以下脚本核对[一致性审查 L1](../audits/2026-11-22-doc-code-consistency.md#生命周期)的已有修复，在安装当前源码的 Python 环境中直接运行。将 `background=True` 加到 `downstream` 的订阅装饰器，可复现后台路径。

```python
import asyncio

from apixis import close_core, create_core, post_event, start_core, subscribe


async def main():
    create_core()
    entered = asyncio.Event()
    calls = []

    @subscribe("demo", priority=2)
    async def upstream(event):
        raise ValueError("upstream failed")

    async def on_has_error(event):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            calls.append("on_has_error caught cancellation")

    async def on_cancelled(event):
        calls.append("on_cancelled")

    @subscribe(
        "demo", priority=1, stop_when_error=False,
        on_has_error=on_has_error, on_cancelled=on_cancelled,
    )
    async def downstream(event):
        calls.append("core ran")

    await start_core()
    await post_event(event_name="demo")
    await asyncio.wait_for(entered.wait(), 2)
    await asyncio.wait_for(close_core(), 2)
    print(calls)


asyncio.run(main())
```

基线输出为 `['on_has_error caught cancellation', 'core ran', 'on_cancelled']`。L1 修复后输出为 `['on_has_error caught cancellation', 'on_cancelled']`，与生命周期文档的承诺一致。

## 取消清理中的关闭重入

受框架管理的取消回调可以直接 `await close_core()`，该次重入提前返回，完整关闭由外部关闭调用继续等待。取消回调新建子任务并等待它再次关闭时，会形成相互等待。以下复现用 0.1 秒观察期限解除等待，不依赖默认的每个取消回调 5 秒协作式超时。

```python
import asyncio

from apixis import close_core, create_core, post_event, start_core, subscribe


async def main():
    create_core()
    entered = asyncio.Event()

    async def on_cancelled(event):
        print("on_cancelled entered")
        try:
            await asyncio.wait_for(asyncio.create_task(close_core()), 0.1)
            print("child close returned")
        except TimeoutError:
            print("child close timed out")

    @subscribe("demo", on_cancelled=on_cancelled)
    async def handler(event):
        entered.set()
        await asyncio.Future()

    await start_core()
    await post_event(event_name="demo")
    await asyncio.wait_for(entered.wait(), 1)
    await asyncio.wait_for(close_core(), 1)
    print("outer close returned")


asyncio.run(main())
```

预期输出依次为 `on_cancelled entered`、`child close timed out`、`outer close returned`。将内层等待改为直接 `await close_core()` 后，会进入 `child close returned` 分支。这一子任务场景保留为已知边界，没有增加子任务归属检测。设计背景见[关闭顺序](../development/runtime-lifecycle.md#close-的取消顺序)。

## 2026 年 11 月 22 日诊断基线

[原始诊断](../audits/2026-11-22-full-code-review.md)中的缺陷探针和生命周期矩阵使用了 `a93d34e` 当时的公开接口。先提取该提交的源码，通过 `PYTHONPATH` 选择它，避免从当前安装包导入已撤出的内部类型。

```bash
apixis_audit_root=$(mktemp -d /tmp/apixis-audit.XXXXXX)
git archive a93d34e src | tar -x -C "$apixis_audit_root"

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$apixis_audit_root/src" \
  .venv/bin/python docs/audits/data/2026-11-22/reproduce_findings.py \
  > "$apixis_audit_root/findings.json"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$apixis_audit_root/src" \
  .venv/bin/python docs/audits/data/2026-11-22/lifecycle_matrix.py \
  > "$apixis_audit_root/lifecycle-matrix.json"
```

矩阵覆盖 972 组交错；缺陷探针退出码 0 表示诊断执行完毕，并不表示缺陷已修复。原始输出见审查目录中的 [findings.json](../audits/data/2026-11-22/findings.json) 和 [lifecycle-matrix.json](../audits/data/2026-11-22/lifecycle-matrix.json)。

`reproduce_findings.py` 的 `quoted_false_config` 子进程会自行将 `PYTHONPATH` 指回仓库的 `src/`，因此这个字段测的是当前检出的配置加载器，其余缺陷场景使用上述 `a93d34e` 基线。当前配置仍按 Python 真值处理 `show_event_dispatch`，字符串 `"false"` 会启用输出。脚本保留原样，不把整份新输出描述成纯历史基线结果。

当前缓存验证使用现行工具：

```bash
.venv/bin/python scripts/audit_handler_chain_cache.py \
  --output /tmp/apixis-cache-audit.json
```

[测试文档](./README.md) · [Graph 性能测试](./graph-performance.md) · [审查文档](../audits/README.md)
