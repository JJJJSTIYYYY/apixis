<div align="center">

# APIXIS

English | [中文](README.md)

Event-driven graph orchestration for Python asyncio applications.

[![PyPI](https://img.shields.io/pypi/v/apixis.svg)](https://pypi.org/project/apixis/)
[![Python](https://img.shields.io/pypi/pyversions/apixis.svg)](https://pypi.org/project/apixis/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE)
[![Discord](https://img.shields.io/badge/Discord-Join%20Us-5865F2?style=flat&logo=discord&logoColor=white)](https://discord.gg/tUCQthwK5)
[![QQ群](https://custom-icon-badges.demolab.com/badge/QQ群-639459172-00BFFF?style=flat&logo=tencent-qq)](https://qun.qq.com/universal-share/share?ac=1&authKey=ommoQrT2zhzHU%2FUxv8pfGCJbNifW%2BJyUAFBkNdzkHTPUxdxCnlgxm5aNgGslTmdE&busi_data=eyJncm91cENvZGUiOiI2Mzk0NTkxNzIiLCJ0b2tlbiI6Im9ZZkdNUWZnSVV1Y2REeUhKNnlTbWEwc05Bb093djRzUXdXNE55dklBVnlBQk9XbGNpS0ZXSDlzK3orSW1sQ3YiLCJ1aW4iOiIzMTI5NDI0NTcyIn0%3D&data=OGTchcr80RAQg8Z8_GZTdvBb7kZDeM9B3hHcNqLaAX2ZK_KYq260C4CubblEBT1bK5fP6zgtnCk2D8fIoph1ZQ&svctype=4&tempid=h5_group_info)

[Source](https://github.com/JJJJSTIYYYY/Apixis) ·
[Issues](https://github.com/JJJJSTIYYYY/Apixis/issues) ·
[Documentation](docs/README.md) ·
[Example](https://github.com/JJJJSTIYYYY/apixis-example)

</div>

APIXIS is a Python library with minimal external dependencies and two composable runtimes: one for event communication within an application, and one for executing multi-step flowcharts. Both share one core and one `asyncio` event loop.

> Python 3.12+ is required. Runtime APIs are designed for `asyncio`.

## Overview

APIXIS fits approval workflows, multi-step tasks, and data processing pipelines.

- **Flowchart orchestration**: Run steps in sequence or in parallel, with the next step decided by your business code.
- **Pause and resume**: A flowchart can wait for a human approval or an external result, then continue where it left off.
- **Progress tracking**: Execution progress is saved automatically, so a failed or aborted flowchart can be resumed from the latest saved record.
- **Live output**: Progress and intermediate results can be sent to the caller while the flowchart runs.
- **Publishing and subscribing**: Components in an application communicate through published and subscribed events. Subscriptions can match an exact event name or use a name pattern to cover a group of related events at once; an event can have multiple handlers, run in a configurable order or in the background, and each handler can have its own error and cancellation callbacks.
- **Requests and waiting**: Publish an event and wait for its processing result; concurrent requests with the same event name cannot receive each other's results. You can also wait for an external event, filter by condition, apply a timeout, and choose whether to observe it before or after business handling—or register the listener first and trigger the event afterwards.
- **Event state and backpressure**: A handler can mark an event as handled so later handlers skip their business logic, and you can inspect which handlers ran and what errors occurred. Queue capacity and concurrent processing can be configured separately; when the queue is full, publishers wait for space, and queue, listener, and task counts are available for monitoring.
- **Microsecond dispatch**: Dispatching an event to a single empty handler costs about 6 microseconds (measured on macOS 27 with Apple M4), suitable for high-frequency event workloads.
- **Runtime management**: Start, pause, and shut down the runtime cleanly. A pause keeps queues and subscriptions, while shutdown cleans up in-flight work and waiting calls, making it suitable for long-running services.

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

## Documentation

- [Usage guide](docs/usage/README.md)
- [Event API](docs/usage/event/README.md)
- [Graph API](docs/usage/graph/README.md)
- [State and commands](docs/usage/graph/state.md)
- [Utilities and exceptions](docs/usage/utils/README.md)
- [Development design](docs/development/README.md) · [Tests and reproduction](docs/testing/README.md) · [Reviews and fixes](docs/audits/README.md)
- **[Example](https://github.com/JJJJSTIYYYY/apixis-example)**

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/JJJJSTIYYYY/Apixis/blob/master/LICENSE).
