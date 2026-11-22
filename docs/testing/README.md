# 测试文档

这里收录正确性测试、问题复现指导、性能测量方法与测试结果汇总。代码审查和修复过程见[审查文档](../audits/README.md)。

所有命令均在仓库根目录执行。先安装开发依赖：

```bash
uv sync --extra dev --locked
```

## 组织

| 目录 | 范围 |
| --- | --- |
| `tests/core/event` | 事件、handler、订阅、通道、背压与生命周期的组件契约 |
| `tests/core/graph` | 节点、Command、context、快照、流与图执行的组件契约 |
| `tests/core/config`、`tests/core/utils` | 配置与日志 |
| `tests/integration/event` | 大规模通配符匹配与完整事件分发的正确性 |
| `tests/integration/graph` | 插件、中断、取消、并发、运行期所有权与图分解 |
| `tests/performance` | 大量多节点图同时调度，含建图耗时的性能测试 |

`tests/conftest.py` 集中管理共享 asyncio runtime 的隔离与清理，以及等待前台分发完成的 helper；根目录 `conftest.py` 统一提供测试筛选与性能参数。仅在这些测试基础设施和已有白盒组件测试中检查内部状态；新增性能 workload 通过 `apixis.core` 的公开接口构建、执行和检查图。

## 正确性测试

```bash
uv run --no-sync pytest
uv run --no-sync pytest -m unit
uv run --no-sync pytest -m integration
uv run --no-sync pytest tests/integration/graph
```

默认不收集性能测试，原有规模正确性测试仍参与默认测试。CI 在默认测试后额外运行十三组性能 workload 的小规模版本，验证测试本身能执行和清理，不设与硬件绑定的速度阈值。

背压边界回归位于 `tests/integration/event/test_backpressure_boundaries.py`，使用实际前台容量 `1/2/3` 与队列容量 `1/2/3` 的九种交叉组合，检查满队列、嵌套并行图、请求响应、取消、重启以及许可归还。测试有异步 deadline；排查死锁时应另加进程级 watchdog，避免取消清理本身阻塞测试退出。

等待器回归覆盖单 Future 固定期限、无读取者时的终态清理、predicate、取消传播及清理先于许可恢复。graph 压力测试中的直接等待和终态重读沿用同一语义。

```bash
APIXIS_STRESS_GRAPHS=4 APIXIS_STRESS_CALLS=6 APIXIS_STRESS_TIMEOUT=60 \
  uv run --no-sync pytest --asyncio-debug \
  tests/integration/graph/test_graph_backpressure_stress.py
```

pytest-asyncio 使用自身配置创建事件循环。通过 `--asyncio-debug` 或 `-o asyncio_debug=true` 启用调试，不应只依赖 `PYTHONASYNCIODEBUG` 环境变量。

## 复现与性能测量

- [生命周期、取消和历史问题复现](./reproduction.md)：当前回归入口、eager 调度、基线源码选择及归档探针。
- [Graph 性能测试](./graph-performance.md)：十三组负载、三档背压、筛选参数及生成报告的字段。
- [跨版本性能对比与历史结果](./performance/README.md)：当前比较工具、归档快照与原始测量。

## 既有结果汇总

| 记录 | 验证范围与结果 | 源码范围 |
| --- | --- | --- |
| [2026-11-20 显式生命周期验证](./performance/2026-11-20-runtime-lifecycle.md) | debug 全量 1461 项；eager 专项 83 项；小规模图负载 13 项 | 报告 validation 中的阶段性源码指纹 |
| [2026-11-21 生命周期性能复测](./performance/2026-11-21-lifecycle-recheck.md) | 主对照 25 项；独立复测 5 项；缓存 13 场景及 2 项重注册检查 | `f09f638` 与冻结 after；after 指纹与 `a93d34e` 一致，不含版本文件 |
| [2026-11-22 全量诊断](../audits/2026-11-22-full-code-review.md#范围与验证) | 初始 debug 全量 1552 项；eager 专项 182 项；生命周期矩阵 972 组 | `a93d34e`；后续修复结果在原报告分别列出 |
| [2026-11-22 一致性核对](../audits/2026-11-22-doc-code-consistency.md#验证结果) | 初始默认 1625 项；L1 修复后默认 1629 项 | `5220a2c` 及 L1 未提交修复，见对应修复说明 |

表格汇总既有记录。新运行应单独保存命令、源码版本或指纹、Python 环境、参数与结果；历史通过数量不代表新工作区已经完成同一验证。

## 文档重构验证

2026-11-22 按四类文档重组并对齐现有说明，运行时代码、源码 docstring、测试实现和历史原始数据均保持本轮开始时的内容。

| 检查 | 结果 |
| --- | --- |
| 文档结构与本地引用 | 34 份 Markdown；迁移后的文件链接与标题锚点核对通过 |
| 示例语法与导入 | 57 个 Python 代码块、29 项显式 Apixis 导入、34 个 shell 代码块通过解析或导入检查 |
| 完整 Python 示例 | 根 README 两段、快速开始两段、生命周期入口、L1 修复复现和关闭子任务边界，共 7 段独立执行通过 |
| 图与事件关键片段 | 活跃期手动快照与恢复、并发冲突保留已提交状态、KeepRef 无快照、启动前入队、无业务订阅的前台等待和按组注销边界验证通过 |
| 历史命令 | 提取 `a93d34e` 的诊断探针运行完成，972 组生命周期矩阵通过；`f09f638` 加归档补丁重建 after，两版指纹一致，归档驱动小规模四个 worker、25 项配对通过 |
| 文件完整性 | 本轮开始时的 360 个非 Markdown 文件内容不变；5 份 manifest 中 74 个文件的大小与 SHA-256 均匹配 |
| 差异检查 | `git diff --check` 通过 |

这些执行用于核对文档中的示例、基线选择和命令入口，不替代全量测试或原规模性能测量。归档数据未改写，新运行的输出保存在临时目录。

[文档首页](../README.md) · [使用文档](../usage/README.md) · [开发文档](../development/README.md)
