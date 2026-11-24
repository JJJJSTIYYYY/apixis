<div align="center">

# APIXIS

中文 | [English](README.en.md)

面向 Python asyncio 应用的事件驱动图编排。

[![PyPI](https://img.shields.io/pypi/v/apixis.svg)](https://pypi.org/project/apixis/)
[![Python](https://img.shields.io/pypi/pyversions/apixis.svg)](https://pypi.org/project/apixis/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)

[源码](https://github.com/JJJJSTIYYYY/Apixis) ·
[问题](https://github.com/JJJJSTIYYYY/Apixis/issues) ·
[文档](docs/README.md) ·
[示例](https://github.com/JJJJSTIYYYY/apixis-example)

</div>

APIXIS 是一个轻外部依赖的 Python 库，提供两个可组合的运行时：一个负责应用内的事件通信，一个负责多步骤流程图的执行；二者共享同一个 core 和 `asyncio` 事件循环。

> 需要 Python 3.12+。运行时 API 专为 `asyncio` 设计。

## 功能概览

APIXIS 适合审批流、多步骤任务和数据处理等场景。

- **流程图编排**：按顺序或并发执行多个步骤，下一步走向由业务代码决定。
- **暂停与恢复**：流程图可以停下来等待人工审批或外部结果，之后再从暂停处继续。
- **进度保存**：自动保存执行进度，失败或中止后可以从最近的记录恢复。
- **实时输出**：执行过程中可以向调用方持续推送进度和中间结果。
- **事件发布与订阅**：应用内的组件通过发布、订阅事件解耦。订阅可以按事件名精确匹配，也可以用一个名称模式一次覆盖一组相关事件；同一个事件可以挂多个处理器，按设定的顺序执行或放到后台运行，每个处理器还能单独配置出错和取消时的回调。
- **请求与等待**：发布事件后可以等待这一次的处理结果，同名的并发请求不会拿错结果。也可以等待某个外部事件，支持条件筛选、超时，以及选择在业务处理之前或之后拿到事件；还可以先注册监听，再执行可能触发它的操作。
- **事件状态与压力控制**：处理器可以标记事件已处理，让后续处理器跳过业务逻辑，也可以查看哪些处理器执行过以及出现的错误。队列容量和并发处理量可以分别配置；队列满时发布方会等待空位，队列、监听和任务数量等运行状态可以随时查询。
- **微秒级调度**：单个空处理器的事件派发开销约为 6 微秒（macOS 27、Apple M4 实测），适合高频事件场景。
- **运行时管理**：提供完整的启动、暂停、关闭能力，暂停期间保留队列和订阅，关闭时清理在途任务和等待中的调用，适合长时间运行的服务。

## 安装

```bash
pip install apixis
```

要安装本地检出，请在仓库根目录运行：

```bash
python -m pip install .
```

如需开发和发布工具：

```bash
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest
```

使用 uv 时，请使用 `uv sync --extra dev --locked` 和 `uv run --extra dev pytest`。
运行静态代码检查：`uv run --extra dev ruff check .`。Ruff 检查常见代码错误和未使用的导入等问题，不执行静态类型检查。

项目采用 `src` 布局，测试位于仓库根目录的 `tests/`，所有开发命令均在仓库根目录执行：

```text
src/apixis/     # 可安装的 Python 包
tests/          # 单元、集成和性能测试
docs/           # 项目文档
scripts/        # 基准与安装检查脚本
pyproject.toml  # 构建、依赖和工具配置
```

## 快速开始

```python
import asyncio

from apixis import GraphManager, create_core, start_core, close_core


def increment(state: dict) -> dict:
    return {"value": state["value"] + 1}


async def master() -> None:
    create_core()
    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )
    try:
        await start_core()
        result = await graph.invoke({"value": 1})
        print(result)  # {'value': 2}
    finally:
        graph.decompose()
        await close_core()


asyncio.run(master())
```

## 文档

- [文档](docs/README.md)
- [使用指南](docs/usage/README.md)
- [事件 API](docs/usage/event/README.md)
- [图 API](docs/usage/graph/README.md)
- [状态与命令](docs/usage/graph/state.md)
- [工具与异常](docs/usage/utils/README.md)
- [开发设计](docs/development/README.md) · [测试与复现](docs/testing/README.md) · [审查与修复](docs/audits/README.md)
- **[使用示例](https://github.com/JJJJSTIYYYY/apixis-example)**

## 许可证

根据 Apache License 2.0 授权。请参阅 [LICENSE](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)。
