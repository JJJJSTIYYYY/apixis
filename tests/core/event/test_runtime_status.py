"""Read-only runtime snapshots across lifecycle and backpressure boundaries."""

import asyncio
from dataclasses import FrozenInstanceError, asdict
from functools import partial
import importlib

import pytest

from apixis import (
    ApixEventPipe, EventLoopStatus, EventRuntimeStatus, EventThreadBridgeStatus,
    EventWaiter, get_event_runtime_status, post_event, start_core, stop_core, subscribe,
)
from apixis.core.event import factory
from apixis.core.event.base import suspend_process


@pytest.fixture
def event_runtime_capacity(request, monkeypatch):
    capacity = getattr(request, "param", None)
    loop_module = importlib.import_module("apixis.core.event.event_loop")
    monkeypatch.setattr(loop_module, "EVENT_LOOP_BACKPRESSURE", capacity)
    monkeypatch.setattr(factory, "_core", None)
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    return capacity


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.parametrize("module_name", ["apixis", "apixis.core", "apixis.core.event"])
def test_status_exports(module_name):
    module = importlib.import_module(module_name)
    assert module.get_event_runtime_status is get_event_runtime_status
    for status_type in (EventRuntimeStatus, EventLoopStatus, EventThreadBridgeStatus):
        assert getattr(module, status_type.__name__) is status_type
        assert status_type.__name__ in module.__all__


async def test_uninitialized_query_does_not_construct_or_start_core(monkeypatch):
    monkeypatch.setattr(factory, "_core", None)
    assert get_event_runtime_status() == EventRuntimeStatus()
    assert factory._core is None


async def test_initialized_query_does_not_schedule_startup(monkeypatch):
    monkeypatch.setattr(factory, "_core", None)
    core = factory._get_core()
    snapshot = get_event_runtime_status()
    assert snapshot.initialized
    assert not snapshot.running
    assert snapshot.event_queue_capacity == 1
    assert not snapshot.event_loop.running
    assert snapshot.thread_bridge is None
    assert core.start_task is None
    assert core.event_loop._event_consumer_task is None


async def test_snapshots_are_immutable_and_do_not_change_with_runtime(cleanup_event_runtime):
    before = get_event_runtime_status()
    assert before.initialized and before.running
    assert before.thread_bridge.running
    assert before.wait_listeners == 0
    with pytest.raises(FrozenInstanceError):
        before.running = False
    with pytest.raises(FrozenInstanceError):
        before.event_loop.dispatch_tasks = 999
    with pytest.raises(FrozenInstanceError):
        before.thread_bridge.queue_size = 999
    waiter = EventWaiter(["never.one", "never.two"])
    try:
        current = get_event_runtime_status()
        assert current.wait_listeners == 2
        assert before.wait_listeners == 0
        assert asdict(current)["thread_bridge"]["queue_capacity"] == 1
    finally:
        waiter.close()
    assert get_event_runtime_status().wait_listeners == 0


async def test_worker_query_is_rejected_without_bridge_submission(cleanup_event_runtime):
    with pytest.raises(RuntimeError, match="main thread"):
        await asyncio.to_thread(get_event_runtime_status)
    assert get_event_runtime_status().thread_bridge.active_requests == 0


@pytest.mark.parametrize("event_runtime_capacity", [1, None], indirect=True)
async def test_suspended_dispatch_keeps_task_but_lends_permit(cleanup_event_runtime, event_runtime_capacity):
    entered, release = asyncio.Event(), asyncio.Event()

    @subscribe("suspended")
    async def suspended(event):
        async with suspend_process():
            entered.set()
            await release.wait()

    try:
        await post_event(event_name="suspended")
        await asyncio.wait_for(entered.wait(), 1)
        snapshot = get_event_runtime_status()
        assert snapshot.event_loop.dispatch_capacity == event_runtime_capacity
        assert snapshot.event_loop.dispatch_tasks == 1
        assert snapshot.event_loop.dispatch_permits_in_use == 0
        assert not snapshot.event_loop.pending_dispatch
    finally:
        release.set()
        await until(lambda: get_event_runtime_status().event_loop.dispatch_tasks == 0)
    assert get_event_runtime_status().event_loop.dispatch_permits_in_use == 0


@pytest.mark.parametrize("event_runtime_capacity", [1], indirect=True)
async def test_queue_pending_dispatch_background_and_pause_are_distinct(
    cleanup_event_runtime, wait_for_dispatch,
):
    core = cleanup_event_runtime
    foreground_entered, background_entered = asyncio.Event(), asyncio.Event()
    release_foreground, release_background = asyncio.Event(), asyncio.Event()

    @subscribe("foreground")
    async def foreground(event):
        foreground_entered.set()
        await release_foreground.wait()

    @subscribe("background", background=True)
    async def background(event):
        background_entered.set()
        await release_background.wait()

    try:
        await post_event(event_name="background")
        await asyncio.wait_for(background_entered.wait(), 1)
        await until(lambda: get_event_runtime_status().event_loop.dispatch_tasks == 0)
        await post_event(event_name="foreground")
        await asyncio.wait_for(foreground_entered.wait(), 1)
        await post_event(event_name="pending")
        await post_event(event_name="queued")
        await until(lambda: get_event_runtime_status().event_loop.pending_dispatch)
        snapshot = get_event_runtime_status()
        assert snapshot.event_queue_size == 1
        assert snapshot.event_loop.dispatch_tasks == 1
        assert snapshot.event_loop.dispatch_permits_in_use == 1
        assert snapshot.event_loop.background_tasks == 1

        await stop_core()
        paused = get_event_runtime_status()
        assert not paused.running
        assert not paused.event_loop.running
        assert not paused.thread_bridge.running
        assert paused.event_loop.dispatch_tasks == 1
        assert paused.event_loop.background_tasks == 1
        assert paused.event_loop.pending_dispatch
        assert paused.event_queue_size == 1
    finally:
        release_foreground.set()
        release_background.set()
        await start_core()
        await wait_for_dispatch(core.event_loop)
        await until(lambda: get_event_runtime_status().event_loop.background_tasks == 0)
    drained = get_event_runtime_status()
    assert drained.running
    assert drained.event_queue_size == 0
    assert not drained.event_loop.pending_dispatch
    assert drained.event_loop.dispatch_tasks == 0
    assert drained.event_loop.dispatch_permits_in_use == 0


async def test_bridge_reports_active_queued_and_admission_waiting_requests(
    cleanup_event_runtime, monkeypatch,
):
    core = cleanup_event_runtime
    entered = asyncio.Event()

    async def blocked(event):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(core.event_pipe, "put", blocked)
    first = await core.thread_bridge.asubmit(partial(post_event, event_name="blocked"))
    await asyncio.wait_for(entered.wait(), 1)
    second = await core.thread_bridge.asubmit(partial(post_event, event_name="queued"))
    waiting = asyncio.create_task(core.thread_bridge.asubmit(partial(post_event, event_name="waiting")))
    try:
        await until(lambda: get_event_runtime_status().thread_bridge.admission_waiters == 1)
        snapshot = get_event_runtime_status()
        assert snapshot.thread_bridge.running
        assert snapshot.thread_bridge.queue_capacity == 1
        assert snapshot.thread_bridge.queue_size == 1
        assert snapshot.thread_bridge.active_requests == 1
        assert snapshot.event_queue_size == 0
    finally:
        await stop_core()
        outcomes = await asyncio.gather(
            asyncio.wrap_future(first.result), asyncio.wrap_future(second.result), waiting,
            return_exceptions=True,
        )
    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)
    stopped = get_event_runtime_status().thread_bridge
    assert not stopped.running
    assert stopped.queue_size == 0
    assert stopped.admission_waiters == 0
    assert stopped.active_requests == 0
