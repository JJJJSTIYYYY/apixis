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
    post_and_wait,
    start_core,
    stop_core,
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
    assert not any(path.startswith("apixis/core/graph/") for path in files)

    observed = []

    @subscribe("package.*")
    async def receive(event):
        observed.append(event.context)

    await start_core()
    try:
        event = await post_and_wait(
            event_type=EventType.INFO,
            event_name="package.ready",
            context={"ok": True},
            time_out=5,
        )
        assert observed == [event.context] == [{"ok": True}]
    finally:
        unsubscribe(receive.__name__)
        await stop_core()
    print(f"Installed apixis {installed.version}: metadata and events passed.")


if __name__ == "__main__":
    asyncio.run(main())
