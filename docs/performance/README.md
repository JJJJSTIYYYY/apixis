# 性能验证与历史记录

显式 create/start/stop/close 与纯同步 getter 的最新验证、安装包检查及配对数据见 [2026-11-20 生命周期验证](./2026-11-20-runtime-lifecycle.md)。

当前运行时使用共享事件 core，支持本地事件、等待器和 graph。以下历史数据用于追溯各次实现的测量证据，**不代表重建后工作区的性能结果**。运行环境、源码指纹、驱动指纹和样本轮次以对应 JSON 为准。

重建后工作区的配对测量见 [2026-11-20 性能核对](./2026-11-20-single-core-recheck.md)，包含 registry 查询回退、事件与等待改进，以及完整原始样本。

查询回退的提交来源、闭包开销与 LRU 淘汰核对见 [handler 查询回退定位](./2026-11-20-handler-query-cause.md)。后续的显式循环修复、正确性验证和配对结果见 [handler 查询入口优化](./2026-11-20-handler-query-optimization.md)。

## 当前验证入口

- [Graph 性能测试](../../tests/README.md#graph-性能测试)：独立/共享图、串行/并行节点、异步等待及三档背压。
- [跨版本比较脚本](../../scripts/benchmark_runtime.py)：四个进程按 before/after/after/before 比较，覆盖 graph、状态快照、registry、全局事件发布、请求响应、getter 及混合等待监听。
- [缓存审计脚本](../../scripts/audit_handler_chain_cache.py)：验证真实派发、停止重启、重新注册、graph 和固定回包名称的缓存构建次数。

```bash
python scripts/benchmark_runtime.py \
  --worker-source src --output /tmp/apixis-local-events.json \
  --repeats 3 --events 4096 --mixed-events 128 --mixed-listeners 64 \
  --scenario event/cached-pipe --scenario event/global-post \
  --scenario event/request-reply --scenario event/mixed-wait-50

python scripts/audit_handler_chain_cache.py --output /tmp/apixis-cache-audit.json
```

比较两版源码时使用 `--baseline-source`，并确保所选场景在两版都可用。运行时 API 不提供旧实现兼容层，历史 API 的必要探测仅位于比较工具中。

缓存审计会额外报告直接修改已注册对象字段、再通过底层 registry 注册时的历史缓存问题。`findings` 中存在复现项时脚本返回非零；该结果应与正常场景的缓存统计分别阅读。应用修改注册信息应使用 `subscribe()` 或 handler 的 `register()`。

## 保留的历史数据

| 记录 | 内容 |
| --- | --- |
| [性能历史](./data/history.json) | 历次配对比较与测量说明，含当前已移除架构的历史结果 |
| [等待热路径](./data/latest/2026-11-08-waiter-hotpath.json) | 等待监听独立缓存前后的配对数据 |
| [热路径控制组](./data/latest/2026-11-08-waiter-hotpath-controls.json) | 混合监听及普通派发控制组 |
| [热路径采样](./data/latest/2026-11-08-waiter-hotpath-profile.json) | 当时的调用开销证据 |
| [Graph 测量](./data/2026-11-17/graph.json) | 2026-11-17 的 graph 性能原始数据 |
| [结构重构比较](./data/2026-11-17/refactor-abba.json) | 当时架构变更的配对比较，含本地控制组 |
| [等待优化比较](./data/2026-11-17/reply-optimization-comparison.json) | 当时等待优化与本地控制组的配对数据 |

![历史 Graph 测量](./data/2026-11-17/graph-performance.png)

目录 manifest 校验当前保留文件的大小和 SHA256。混合历史记录和历史补丁保持原始测量内容，不能应用到当前运行时或作为当前 API 示例。纯跨线程基准及图表已经移除，原始文件可从 Git `612d5d0` 查阅。

[测试说明](../../tests/README.md) · [文档首页](../README.md)
