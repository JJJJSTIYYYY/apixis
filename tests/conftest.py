"""Centralized isolation and dispatch synchronization for the shared runtime."""

import asyncio
import importlib
from concurrent.futures import Future
from threading import Thread
from types import SimpleNamespace

import pytest
import pytest_asyncio

from apixis.core.event import factory


@pytest_asyncio.fixture
async def thread_cores():
    """Run named cores on real OS threads and clean up on their owning loops."""
    handles = []

    async def launch(name, setup=None):
        ready, finished = Future(), Future()

        async def serve():
            core = factory.create_core(name)
            stop = asyncio.Event()
            try:
                if setup is not None:
                    setup()
                await factory.start_core()
                ready.set_result(SimpleNamespace(name=name, loop=asyncio.get_running_loop(), stop=stop))
                await stop.wait()
            finally:
                await factory.stop_core()
                tasks = tuple(core.event_loop._dispatch_tasks | core.event_loop._background_handler_tasks)
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        def run():
            try:
                asyncio.run(serve())
            except BaseException as error:
                if not ready.done():
                    ready.set_exception(error)
                finished.set_exception(error)
            else:
                finished.set_result(None)

        thread = Thread(target=run, name=name)
        thread.start()
        handles.append((thread, ready, finished))
        try:
            return await asyncio.wait_for(asyncio.wrap_future(ready), 5)
        except BaseException:
            await asyncio.gather(asyncio.wrap_future(finished), return_exceptions=True)
            await asyncio.to_thread(thread.join, 5)
            handles.remove((thread, ready, finished))
            raise

    try:
        yield launch
    finally:
        for thread, ready, finished in handles:
            if ready.done() and not ready.cancelled() and ready.exception() is None and not finished.done():
                handle = ready.result()
                handle.loop.call_soon_threadsafe(handle.stop.set)
        for thread, ready, finished in handles:
            try:
                await asyncio.wait_for(asyncio.wrap_future(finished), 5)
            finally:
                await asyncio.to_thread(thread.join, 5)
            assert not thread.is_alive()


def _clear_registries(core) -> None:
    """Reset test-owned registrations only after their tasks have stopped."""
    core.handler_registry.clear()
    core.event_registry.clear()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def cleanup_event_runtime(event_runtime_capacity, monkeypatch):
    """Release test-owned tasks explicitly now that stop only halts consumption.

    Keep all private runtime cleanup here. Tests may pause the consumer or
    replace the factory; captured components keep teardown from waking it again.
    """
    if event_runtime_capacity is not None:
        # Changing a module constant cannot resize an existing semaphore.
        # The preceding test has drained its core; create a fresh one here.
        loop_module = importlib.import_module("apixis.core.event.event_loop")
        monkeypatch.setattr(loop_module, "EVENT_LOOP_BACKPRESSURE", event_runtime_capacity)
        monkeypatch.setattr(factory._local, "core", None)
    core = factory._get_core()
    if event_runtime_capacity is not None:
        assert core.event_loop._event_semaphore._bounded == event_runtime_capacity
    _clear_registries(core)
    await factory.start_core()
    try:
        yield core
    finally:
        if core.start_task is not None:
            await asyncio.gather(core.start_task, return_exceptions=True)
        loop, pipe = core.event_loop, core.event_pipe
        await loop.stop()
        tasks = tuple(loop._dispatch_tasks | loop._background_handler_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # stop() preserves a dequeued event; isolation must acknowledge it too.
        if loop._pending_dispatch is not None:
            loop._pending_dispatch = None
            pipe.task_done()
        await pipe.clear()
        await pipe.stop()
        if core.endpoint is not None:
            await core.endpoint.close()
        factory.get_thread_bridge().unregister(core.thread_id)
        _clear_registries(core)


@pytest.fixture
def event_runtime_capacity():
    """Keep the configured capacity unless a benchmark requests a fresh core."""
    return None


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
