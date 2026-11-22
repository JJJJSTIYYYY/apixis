# 测试目录

`core/` 保存组件契约测试，`integration/` 保存事件与图的集成测试，`performance/` 保存图性能 workload。

命令均在仓库根目录执行。安装开发依赖后运行默认测试：

```bash
uv sync --extra dev --locked
uv run --no-sync pytest
```

完整的测试筛选、背压与生命周期复现、性能参数和结果说明集中在[测试指南](../docs/testing/README.md)；历史审查与测量的源码选择见[复现指导](../docs/testing/reproduction.md)。
