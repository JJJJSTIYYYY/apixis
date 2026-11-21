"""Retry interrupted lifecycle operations without undoing completed work."""

import asyncio

import pytest
import pytest_asyncio

from apixis import (CoreClosedError, CoreState, EventWaiter, Logger, post_event)
from apixis.core.event.base.handler import (ApixEventHandler)
from apixis.core.event.core import (ApixisCore)
from apixis.core.event import factory


@pytest_asyncio.fixture(params=[None, asyncio.eager_task_factory], ids=["default", "eager"])
async def core(request, monkeypatch):
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    loop.set_task_factory(request.param)
    instance = ApixisCore._create_core()
    monkeypatch.setattr(factory, "_core", instance)
    try:
        yield instance
    finally:
        loop.set_task_factory(previous_factory)
        await instance.close_core()


@pytest.mark.parametrize("operation", ["start", "stop", "close"])
async def test_retry_internal_task_cancelled_before_its_first_step(core, operation):
    if operation != "start":
        await core.start_core()
    caller = asyncio.create_task(getattr(core, operation + "_core")())
    if asyncio.get_running_loop().get_task_factory() is None:
        await asyncio.sleep(0)
    interrupted = getattr(core, "_" + operation + "_task")
    interrupted.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    expected = {"start": CoreState.STARTING, "stop": CoreState.STOPPING, "close": CoreState.CLOSING}
    assert core.state is expected[operation]
    assert core._event_loop.is_running is (operation != "start")
    await getattr(core, operation + "_core")()
    assert core.state is {"start": CoreState.RUNNING, "stop": CoreState.STOPPED, "close": CoreState.CLOSED}[operation]
    assert core._event_loop.is_running is (operation == "start")


@pytest.mark.parametrize("operation", ["start", "stop", "close"])
@pytest.mark.parametrize("failure", ["cancel", "error"])
async def test_retry_after_component_step_completed(core, monkeypatch, operation, failure):
    if operation != "start":
        await core.start_core()
    entered = asyncio.Event()
    original = getattr(core._event_loop, operation)

    async def interrupt_after_step():
        result = await original()
        entered.set()
        if failure == "error":
            raise OSError("component completed before failure")
        await asyncio.Future()
        return result

    with monkeypatch.context() as patch:
        patch.setattr(core._event_loop, operation, interrupt_after_step)
        caller = asyncio.create_task(getattr(core, operation + "_core")())
        await entered.wait()
        interrupted = getattr(core, "_" + operation + "_task")
        if failure == "cancel":
            interrupted.cancel()
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else OSError):
            await caller
    assert core.state is {"start": CoreState.STARTING, "stop": CoreState.STOPPING, "close": CoreState.CLOSING}[operation]
    assert core._event_loop.is_running is (operation == "start")
    consumer = core._event_loop._event_consumer_task
    await asyncio.gather(*(getattr(core, operation + "_core")() for _ in range(4)))
    assert core.state is {"start": CoreState.RUNNING, "stop": CoreState.STOPPED, "close": CoreState.CLOSED}[operation]
    if operation == "start":
        assert core._event_loop._event_consumer_task is consumer
    elif operation == "stop":
        received = asyncio.Event()

        async def handle(event):
            received.set()

        ApixEventHandler(handle).register("retained")
        await post_event(event_name="retained")
        assert not received.is_set()
        await core.start_core()
        async with asyncio.timeout(1):
            await received.wait()
    else:
        assert not core._handler_registry.registry
        assert not core._event_loop._dispatch_tasks
        assert core._event_loop._event_semaphore.in_use == 0


async def test_retry_started_consumer_without_recreating_it(core, monkeypatch):
    original = core._event_loop.start

    async def interrupt_confirmation():
        consumer = await original()
        asyncio.get_running_loop().call_soon(core._start_task.cancel)
        return consumer

    with monkeypatch.context() as patch:
        patch.setattr(core._event_loop, "start", interrupt_confirmation)
        with pytest.raises(asyncio.CancelledError):
            await core.start_core()
    assert core.state is CoreState.STARTED
    consumer = core._event_loop._event_consumer_task
    await core.start_core()
    assert core.state is CoreState.RUNNING
    assert core._event_loop._event_consumer_task is consumer


@pytest.mark.parametrize("dependency", ["start", "stop"])
async def test_cancelled_close_does_not_cancel_pending_start_or_stop(core, monkeypatch, dependency):
    if dependency == "stop":
        await core.start_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = getattr(core._event_loop, dependency)

    async def blocked():
        entered.set()
        await release.wait()
        return await original()

    with monkeypatch.context() as patch:
        patch.setattr(core._event_loop, dependency, blocked)
        prerequisite = asyncio.create_task(getattr(core, dependency + "_core")())
        await entered.wait()
        dependency_task = getattr(core, "_" + dependency + "_task")
        closing = asyncio.create_task(core.close_core())
        while core._close_task is None or not core._close_task._fut_waiter:
            await asyncio.sleep(0)
        core._close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert not dependency_task.done()
        assert not dependency_task.cancelling()
        release.set()
        # Startup may now find admission sealed before creating a consumer.
        await asyncio.gather(prerequisite, return_exceptions=True)
    await core.close_core()
    assert core.state is CoreState.CLOSED
    assert not core._event_loop.is_running


async def test_retry_stop_while_startup_is_still_running(core, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    original = core._event_loop.start

    async def blocked_start():
        entered.set()
        await release.wait()
        return await original()

    with monkeypatch.context() as patch:
        patch.setattr(core._event_loop, "start", blocked_start)
        starting = asyncio.create_task(core.start_core())
        await entered.wait()
        start_task = core._start_task
        stopping = asyncio.create_task(core.stop_core())
        while core._stop_task is None or not core._stop_task._fut_waiter:
            await asyncio.sleep(0)
        core._stop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        assert not start_task.cancelling()
        retry = asyncio.create_task(core.stop_core())
        restart = asyncio.create_task(core.start_core())
        release.set()
        await asyncio.gather(starting, retry, restart)
    assert core.state is CoreState.RUNNING
    assert core._event_loop.is_running


async def test_retry_stop_retains_consumer_until_cancellation_cleanup_finishes(core, monkeypatch):
    cleaning, release = asyncio.Event(), asyncio.Event()

    async def consumer():
        try:
            await asyncio.Future()
        finally:
            cleaning.set()
            await release.wait()

    with monkeypatch.context() as patch:
        patch.setattr(core._event_loop, "_event_consumer_loop", consumer)
        await core.start_core()
        old_consumer = core._event_loop._event_consumer_task
        caller = asyncio.create_task(core.stop_core())
        await cleaning.wait()
        core._stop_task.cancel()
        try:
            with pytest.raises(asyncio.CancelledError):
                await caller
            assert core._event_loop._event_consumer_task is old_consumer
            assert not old_consumer.done()
            restart = asyncio.create_task(core.start_core())
            await asyncio.sleep(0)
            assert not restart.done()
            release.set()
            await restart
            assert old_consumer.cancelled()
            assert core._event_loop._event_consumer_task is not old_consumer
            assert core.state is CoreState.RUNNING
        finally:
            release.set()


async def test_retry_close_while_handler_cancellation_cleanup_is_running(core):
    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    cleanup_tasks = []
    callbacks = []

    async def run(event):
        entered.set()
        await asyncio.Future()

    async def cleanup(event):
        task = asyncio.current_task()
        cleanup_tasks.append((task, task.cancelling()))
        cleaning.set()
        await release.wait()

    ApixEventHandler(run, on_cancelled=cleanup).register("active")
    waiter = EventWaiter("never-produced")
    core.add_close_task_done_callback(lambda task: callbacks.append(task))
    await core.start_core()
    await post_event(event_name="active")
    await entered.wait()
    caller = asyncio.create_task(core.close_core())
    await cleaning.wait()
    interrupted = core._close_task
    interrupted.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    task, cancellation_count = cleanup_tasks[0]
    assert not task.done()
    assert task.cancelling() == cancellation_count
    assert core.state is CoreState.CLOSING
    release.set()
    await core.close_core()
    assert not waiter.active
    assert await waiter.wait() is None
    assert len(cleanup_tasks) == 1
    assert callbacks[0] is interrupted and callbacks[0].cancelled()
    assert len(callbacks) == 2 and callbacks[1].exception() is None
    assert core.state is CoreState.CLOSED
    assert not core._event_loop._dispatch_tasks
    assert not core._event_loop._background_handler_tasks
    assert core._event_loop._event_semaphore.in_use == 0


async def test_repeat_completed_stop_and_close_without_reopening_core(core, monkeypatch):
    await core.start_core()
    await core.stop_core()
    previous_stop = core._stop_task
    await core.stop_core()
    assert core._stop_task is not previous_stop
    await core.close_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = core._event_loop.close

    async def repeat_cleanup():
        entered.set()
        await release.wait()
        await original()

    with monkeypatch.context() as patch:
        patch.setattr(core._event_loop, "close", repeat_cleanup)
        caller = asyncio.create_task(core.close_core())
        await entered.wait()
        assert core.state is CoreState.CLOSING
        assert core._event_pipe.closed and core._handler_registry.closed
        with pytest.raises(CoreClosedError):
            await core.start_core()
        release.set()
        await caller
    assert core.state is CoreState.CLOSED


async def test_retry_close_after_waiter_unregister_logging_failed(core, monkeypatch):
    waiter = EventWaiter(["one", "two"])

    def fail(message):
        if message.startswith("Unregistered handler"):
            raise OSError("logging failed during waiter release")

    with monkeypatch.context() as patch:
        patch.setattr(Logger, "_debug", fail)
        with pytest.raises(OSError):
            await core.close_core()
    assert core.state is CoreState.CLOSING
    assert not waiter.active
    await core.close_core()
    assert core.state is CoreState.CLOSED
    assert not core._handler_registry._waiter_cache.waiters
    assert not core._handler_registry.registry
    waiter.close()
    assert waiter._registry is None


async def test_retry_close_after_queue_clear_logging_failed(core, monkeypatch):
    for _ in range(3):
        await post_event(event_name="queued")

    def fail(message):
        if message.startswith("Cleaned"):
            raise OSError("queue was cleared before logging failed")

    with monkeypatch.context() as patch:
        patch.setattr(Logger, "_info", fail)
        with pytest.raises(OSError):
            await core.close_core()
    assert core.state is CoreState.CLOSING
    assert core._event_pipe.empty()
    await core.close_core()
    assert core.state is CoreState.CLOSED
    async with asyncio.timeout(1):
        await core._event_pipe.join()
