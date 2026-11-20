<a id="chinese"></a>

<div align="center">

# APIXIS

中文 | [English](#english)

面向 Python asyncio 应用的事件驱动图编排。

[![PyPI](https://img.shields.io/pypi/v/apixis.svg)](https://pypi.org/project/apixis/)
[![Python](https://img.shields.io/pypi/pyversions/apixis.svg)](https://pypi.org/project/apixis/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)

[源码](https://github.com/JJJJSTIYYYY/Apixis) ·
[问题](https://github.com/JJJJSTIYYYY/Apixis/issues) ·
[API 参考](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md) ·
[示例](https://github.com/JJJJSTIYYYY/apixis-example)

</div>

APIXIS 提供两个可组合的运行时：

- 一个异步事件系统，支持通配符订阅、有序处理器、背压和本地事件队列；
- 一个有状态图运行时，支持命令驱动的步骤、并行节点、快照、流式处理以及中断/恢复工作流。

> 需要 Python 3.12+。运行时 API 专为 `asyncio` 设计。

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

from apixis import GraphManager


def increment(state: dict) -> dict:
    return {"value": state["value"] + 1}


async def master() -> None:
    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )
    try:
        result = await graph.invoke({"value": 1})
        print(result)  # {'value': 2}
    finally:
        graph.decompose()


asyncio.run(master())
```

## 文档

- [API 参考](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md)
- [事件 API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/event/README.md)
- [图 API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/README.md)
- [状态与命令](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/state.md)
- [工具与异常](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/utils/README.md)
- **[使用示例](https://github.com/JJJJSTIYYYY/apixis-example)**

## 许可证

根据 Apache License 2.0 授权。请参阅 [LICENSE](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)。

---

<a id="english"></a>

<div align="center">

# APIXIS

English | [中文](#chinese)

Event-driven graph orchestration for Python asyncio applications.

[![PyPI](https://img.shields.io/pypi/v/apixis.svg)](https://pypi.org/project/apixis/)
[![Python](https://img.shields.io/pypi/pyversions/apixis.svg)](https://pypi.org/project/apixis/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)
[![Discord](https://img.shields.io/badge/Discord-Join%20Us-5865F2?style=flat&logo=discord&logoColor=white)](https://discord.gg/tUCQthwK5)
[![QQ群](https://custom-icon-badges.demolab.com/badge/QQ群-639459172-00BFFF?style=flat&logo=tencent-qq)](https://qun.qq.com/universal-share/share?ac=1&authKey=ommoQrT2zhzHU%2FUxv8pfGCJbNifW%2BJyUAFBkNdzkHTPUxdxCnlgxm5aNgGslTmdE&busi_data=eyJncm91cENvZGUiOiI2Mzk0NTkxNzIiLCJ0b2tlbiI6Im9ZZkdNUWZnSVV1Y2REeUhKNnlTbWEwc05Bb093djRzUXdXNE55dklBVnlBQk9XbGNpS0ZXSDlzK3orSW1sQ3YiLCJ1aW4iOiIzMTI5NDI0NTcyIn0%3D&data=OGTchcr80RAQg8Z8_GZTdvBb7kZDeM9B3hHcNqLaAX2ZK_KYq260C4CubblEBT1bK5fP6zgtnCk2D8fIoph1ZQ&svctype=4&tempid=h5_group_info)

[Source](https://github.com/JJJJSTIYYYY/Apixis) ·
[Issues](https://github.com/JJJJSTIYYYY/Apixis/issues) ·
[API Reference](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md) ·
[Example](https://github.com/JJJJSTIYYYY/apixis-example)

</div>

APIXIS provides two composable runtimes:

- an asynchronous event system with wildcard subscriptions, ordered handlers, backpressure, and a local event queue;
- a stateful graph runtime with Command-driven steps, parallel nodes, snapshots, streaming, and interruption/resume workflows.

> Python 3.12+ is required. Runtime APIs are designed for `asyncio`.

## Install

```bash
pip install apixis
```

To install a local checkout, run from the repository root:

```bash
python -m pip install .
```

For development and release tools:

```bash
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest
```

With uv, use `uv sync --extra dev --locked` and `uv run --extra dev pytest`.
Run static code checks with `uv run --extra dev ruff check .`. Ruff checks common code errors and unused imports; it does not perform static type checking.

The project uses a `src` layout: the installable package lives in `src/apixis/`,
and unit, integration and performance tests live in the root `tests/` directory.
Run development commands from the repository root.

## Quick start

```python
import asyncio

from apixis import GraphManager


def increment(state: dict) -> dict:
    return {"value": state["value"] + 1}


async def master() -> None:
    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )
    try:
        result = await graph.invoke({"value": 1})
        print(result)  # {'value': 2}
    finally:
        graph.decompose()


asyncio.run(master())
```

## Documentation

- [API reference](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/README.md)
- [Event API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/event/README.md)
- [Graph API](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/README.md)
- [State and commands](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/graph/state.md)
- [Utilities and exceptions](https://github.com/JJJJSTIYYYY/Apixis/blob/master/docs/core/utils/README.md)
- **[Example](https://github.com/JJJJSTIYYYY/apixis-example)**

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE).
