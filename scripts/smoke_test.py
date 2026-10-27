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
    get_event_loop,
    get_event_pipe,
    start_core,
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
    pipe, loop = get_event_pipe(), get_event_loop()
    try:
        async with asyncio.timeout(5):
            await pipe.post_event(
                event_type=EventType.INFO,
                event_name="package.ready",
                context={"ok": True},
            )
            await pipe.join()
            assert observed == [{"ok": True}]
            assert await graph.invoke({"value": 1}) == {"value": 2}
            await pipe.join()
    finally:
        graph.decompose()
        unsubscribe(receive.__name__)
        await loop.stop()
        await pipe.stop()
    print(f"Installed apixis {installed.version}: metadata, events and graph passed.")


if __name__ == "__main__":
    asyncio.run(main())
