"""Centralized isolation and dispatch synchronization for the shared runtime."""

import asyncio
import importlib

import pytest
import pytest_asyncio

from apixis.core.event import factory
from apixis.core.graph.base import _namespace_graphs


def _clear_registries(core) -> None:
    """Reset test-owned registrations only after their tasks have stopped."""
    for graph in tuple(_namespace_graphs.values()):
        graph.decompose()
    _namespace_graphs.clear()
    core.handler_registry.clear()
    core.event_registry.clear()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def cleanup_event_runtime(event_runtime_capacity, monkeypatch):
    """Create/start explicitly and close all test-owned runtime work afterwards."""
    if event_runtime_capacity is not None:
        # Changing a module constant cannot resize an existing semaphore.
        # The preceding test has drained its core; create a fresh one here.
        loop_module = importlib.import_module("apixis.core.event.event_loop")
        monkeypatch.setattr(loop_module, "EVENT_LOOP_BACKPRESSURE", event_runtime_capacity)
        monkeypatch.setattr(factory, "_core", None)
    core = factory.create_core()
    if event_runtime_capacity is not None:
        assert core.event_loop._event_semaphore._bounded == event_runtime_capacity
    _clear_registries(core)
    await factory.start_core()
    try:
        yield core
    finally:
        for graph in tuple(_namespace_graphs.values()):
            graph.decompose()
        await core.close_core()
        if factory._core is not None and factory._core is not core:
            await factory._core.close_core()
        _namespace_graphs.clear()



@pytest.fixture
def event_runtime_capacity(request):
    """Keep the configured capacity unless a benchmark requests a fresh core."""
    return getattr(request, "param", None)


@pytest.fixture
def wait_for_dispatch():
    """Wait for foreground work explicitly; pipe.join() only acknowledges dispatch.

    Keep task inspection in this test helper instead of changing the public
    join contract or relying on arbitrary delays in individual assertions.
    Background handlers are deliberately excluded and need their own signals.
    """
    async def wait(event_loop):
        async with asyncio.timeout(2):
            while True:
                await event_loop._event_pipe.join()
                tasks = tuple(event_loop._dispatch_tasks)
                if not tasks:
                    return
                await asyncio.gather(*tasks, return_exceptions=True)
                # Finished tasks may still have queued capacity-release callbacks.
                await asyncio.sleep(0)

    return wait
