"""Verify an installed distribution without importing the source checkout.

Run with the target virtual environment's Python: python -I scripts/smoke_test.py.
"""

import asyncio
from importlib.metadata import distribution
from pathlib import Path
import sys

import apixis
from apixis import (
    EventType,
    GraphManager,
    post_and_wait,
    create_core,
    start_core,
    close_core,
    subscribe,
    unsubscribe,
)


async def main() -> None:
    installed = distribution("apixis")
    assert apixis.VERSION == apixis.__version__ == installed.version
    assert Path(apixis.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    files = {str(path) for path in installed.files}
    assert "apixis/py.typed" in files
    assert any(path.endswith("/licenses/LICENSE") for path in files)
    assert installed.metadata["License-Expression"] == "Apache-2.0"
    assert not any(path.startswith(("tests/", "src/")) for path in files)
    assert "apixis/core/event/thread_bridge.py" not in files
    assert hasattr(apixis, "create_core")
    assert not hasattr(apixis, "aget_event_pipe")
    assert not hasattr(apixis, "ThreadEventWaiter")
    assert not hasattr(apixis, "post_event_threadsafe")
    for name in (
        "ApixisCore", "ApixEventHandler", "ApixEventLoop", "ApixEventPipe",
        "ApixEventRegistry", "ApixHandlerRegistry", "get_handler",
        "get_event_pipe", "get_event_loop", "get_event_registry",
        "get_handler_registry", "acquire_namespace", "release_namespace",
    ):
        assert not hasattr(apixis, name)

    create_core()
    observed = []

    @subscribe("package.*")
    async def receive(event):
        observed.append(event.context)

    def increment(state):
        return {"value": state["value"] + 1}

    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )
    await start_core()
    try:
        async with asyncio.timeout(5):
            result = await post_and_wait(
                event_type=EventType.INFO,
                event_name="package.ready",
                context={"ok": True},
            )
            assert result.context == {"ok": True}
            assert observed == [{"ok": True}]
            assert await graph.invoke({"value": 1}) == {"value": 2}
    finally:
        graph.decompose()
        unsubscribe(receive.__name__)
        await close_core()
    print(f"Installed apixis {installed.version}: metadata, events and graph passed.")


if __name__ == "__main__":
    asyncio.run(main())
