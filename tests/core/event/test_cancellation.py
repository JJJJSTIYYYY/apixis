"""Cancellation notifications terminate dispatch without resuming business work."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from apixis.core.event import ApixEventPipe, get_event_registry

from apixis.core.event import ApixEvent, ApixEventHandler, EventType
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.handler_registry import ApixHandlerRegistry


@pytest.fixture(autouse=True)
def isolate_registry():
    """Keep the process-wide registry independent between contract tests."""
    registry = ApixHandlerRegistry(get_event_registry())
    for name in list(registry.registry):
        registry.unregister_handler(name)
    yield
    for name in list(registry.registry):
        registry.unregister_handler(name)


def register(registry, name, core_func, **kwargs):
    """Register a handler through the public registry contract."""
    handler = ApixEventHandler(core_func, **kwargs)
    handler.name = name
    handler.subscribe = ["cancel.test"]
    handler.priority = 1
    registry.register_handler(handler)
    return handler


def make_event():
    return ApixEvent("cancel-id", EventType.WORKFLOW, "cancel.test", None, 0)


@pytest.mark.parametrize("phase", ["core_func", "on_has_error", "on_accepted", "on_error"])
@pytest.mark.parametrize("stop_when_error", [False, True])
async def test_cancelled_phase_notifies_entire_foreground_chain(phase, stop_when_error):
    """Completed, current and unreached subscribers all receive cleanup once."""
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    event = make_event()
    notified = []

    def cleanup(name):
        async def on_cancelled(event):
            notified.append(name)
        return on_cancelled

    async def prepare(event):
        if phase == "on_accepted":
            event.accept()
        elif phase == "on_has_error":
            raise ValueError("upstream failure")

    async def cancel(*args):
        raise asyncio.CancelledError("original cancellation")

    register(registry, "before", prepare, on_cancelled=cleanup("before"))
    callbacks = {"core_func": AsyncMock(), phase: cancel}
    if phase == "on_error":
        callbacks["core_func"] = AsyncMock(side_effect=ValueError("core failure"))
    register(
        registry, "current", on_cancelled=cleanup("current"),
        stop_when_error=stop_when_error, **callbacks,
    )
    tail_core = AsyncMock()
    register(registry, "after", tail_core, on_cancelled=cleanup("after"),
             stop_when_error=False)
    background_cleanup = AsyncMock()
    register(registry, "background", AsyncMock(), background=True,
             on_cancelled=background_cleanup)

    task = asyncio.create_task(runtime._dispatch_event(
        event, registry.get_handlers_for_event(event.event_name),
    ))
    with pytest.raises(asyncio.CancelledError, match="original cancellation"):
        await asyncio.wait_for(task, 1)

    assert task.cancelled()
    assert sorted(notified) == ["after", "before", "current"]
    tail_core.assert_not_awaited()
    background_cleanup.assert_not_awaited()
    assert [error.exception_type for error in event.error_stack] == (
        ["ValueError", "CancelledError"] if phase in ("on_error", "on_has_error") else ["CancelledError"]
    )
    error = event.error_stack[-1]
    assert error.handler_name == "current"
    assert error.phase == phase
    assert error.message == "original cancellation"
    assert event.seen == (["before", "current"] if phase in ("core_func", "on_error") else ["before"])


@pytest.mark.parametrize("failure", ["error", "timeout", "cancel"])
async def test_cleanup_failure_cannot_hide_cancellation_or_skip_other_subscribers(failure):
    """A failing cleanup preserves the original cancellation and other hooks."""
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    event = make_event()
    original = asyncio.CancelledError("original cancellation")

    async def cancel(event):
        raise original

    async def broken_cleanup(event):
        if failure == "error":
            raise ValueError("cleanup failure")
        if failure == "cancel":
            raise asyncio.CancelledError("cleanup cancelled")
        await asyncio.Future()

    on_error = AsyncMock()
    register(registry, "broken", cancel, on_cancelled=broken_cleanup,
             on_error=on_error, time_out=0.01 if failure == "timeout" else None)
    tail_cleanup = AsyncMock()
    register(registry, "tail", AsyncMock(), on_cancelled=tail_cleanup)
    with patch("apixis.core.event.base.logger") as logger:
        task = asyncio.create_task(runtime._dispatch_event(event, [registry.registry["broken"], registry.registry["tail"]]))
        with pytest.raises(asyncio.CancelledError, match="original cancellation"):
            # Dispatch overrides the handler timeout with a five-second cleanup limit.
            await asyncio.wait_for(task, 6 if failure == "timeout" else 1)
        logger.error.assert_called_once()
    tail_cleanup.assert_awaited_once_with(event)
    on_error.assert_not_awaited()
    [error] = event.error_stack
    assert error.exception_type == "CancelledError"
    assert error.message == "original cancellation"
    assert error.phase == "core_func"
    assert event.seen == ["broken"]


async def test_cancel_notifications_run_serially_and_wait_for_every_hook():
    """Each cleanup finishes before the next starts, and dispatch waits for all."""
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    event = make_event()
    entered = [asyncio.Event() for _ in range(3)]
    release = [asyncio.Event() for _ in range(3)]
    completed = []
    tail_core = AsyncMock()

    async def cancel(event):
        raise asyncio.CancelledError("original cancellation")

    def cleanup(index):
        async def on_cancelled(event):
            entered[index].set()
            await release[index].wait()
            completed.append(index)
        return on_cancelled

    for index, core in enumerate((AsyncMock(), cancel, tail_core)):
        register(registry, f"handler.{index}", core,
                 on_cancelled=cleanup(index), time_out=None)

    task = asyncio.create_task(runtime._dispatch_event(
        event, registry.get_handlers_for_event(event.event_name),
    ))
    try:
        for index in range(3):
            await asyncio.wait_for(entered[index].wait(), 1)
            assert completed == list(range(index))
            assert not any(signal.is_set() for signal in entered[index + 1:])
            assert not task.done()
            release[index].set()
        with pytest.raises(asyncio.CancelledError, match="original cancellation"):
            await asyncio.wait_for(task, 1)
        assert completed == [0, 1, 2]
        tail_core.assert_not_awaited()
    finally:
        for signal in release:
            signal.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_repeated_dispatch_cancellation_preserves_later_notifications():
    """A second cancellation interrupts the current cleanup, then later hooks run."""
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    event = make_event()
    entered = [asyncio.Event(), asyncio.Event()]
    release_tail = asyncio.Event()
    interrupted = []
    tail_core = AsyncMock()
    on_error = AsyncMock()

    async def cancel(event):
        raise asyncio.CancelledError("original cancellation")

    async def first_cleanup(event):
        entered[0].set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            interrupted.append(0)
            raise

    async def tail_cleanup(event):
        entered[1].set()
        await release_tail.wait()

    register(registry, "first", cancel, on_cancelled=first_cleanup,
             on_error=on_error, time_out=None)
    register(registry, "tail", tail_core, on_cancelled=tail_cleanup,
             on_error=on_error, time_out=None)
    with patch("apixis.core.event.base.logger") as logger:
        task = asyncio.create_task(runtime._dispatch_event(event, [registry.registry["first"], registry.registry["tail"]]))
        try:
            await asyncio.wait_for(entered[0].wait(), 1)
            assert not entered[1].is_set()
            task.cancel("second cancellation")
            await asyncio.wait_for(entered[1].wait(), 1)
            assert interrupted == [0]
            assert not task.done()
            release_tail.set()
            with pytest.raises(asyncio.CancelledError, match="original cancellation"):
                await asyncio.wait_for(task, 1)
            assert task.cancelled()
            logger.error.assert_called_once()
            tail_core.assert_not_awaited()
            on_error.assert_not_awaited()
            assert len(event.error_stack) == 1
            assert event.error_stack[0].exception_type == "CancelledError"
        finally:
            release_tail.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_external_dispatch_cancellation_notifies_after_core_cleanup():
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    entered = asyncio.Event()
    calls = []

    async def core(event):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            calls.append("core cleanup")

    async def cleanup(event):
        calls.append("cancel notification")

    register(registry, "waiting", core, on_cancelled=cleanup)
    task = asyncio.create_task(runtime._dispatch_event(make_event(), [registry.registry["waiting"]]))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert calls == ["core cleanup", "cancel notification"]


async def test_cancelling_one_background_task_does_not_cancel_other_handlers():
    """Concurrent background calls are independent of each other and foreground work."""
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    entered = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()
    calls = 0

    async def background(event):
        nonlocal calls
        index = calls
        calls += 1
        entered[index].set()
        await release.wait()

    background_cleanup = AsyncMock()
    register(registry, "background", background, background=True,
             on_cancelled=background_cleanup)
    foreground_core, foreground_cleanup = AsyncMock(), AsyncMock()
    register(registry, "foreground", foreground_core, on_cancelled=foreground_cleanup)
    event = make_event()
    runtime._create_background_handler_task(runtime._registry.registry["background"], event)
    first_background, = runtime._background_handler_tasks
    try:
        await asyncio.wait_for(entered[0].wait(), 1)
        await runtime._dispatch_event(event, [registry.registry["background"], registry.registry["foreground"]])
        await asyncio.wait_for(entered[1].wait(), 1)
        foreground_core.assert_awaited_once_with(event)
        second_background, = runtime._background_handler_tasks - {first_background}
        second_background.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second_background
        assert not first_background.done()
        foreground_cleanup.assert_not_awaited()
        background_cleanup.assert_awaited_once_with(event)
    finally:
        release.set()
        await asyncio.gather(*runtime._background_handler_tasks, return_exceptions=True)


@pytest.mark.parametrize("cause", ["raise", "external"])
async def test_background_cancellation_only_notifies_its_own_handler(cause):
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    entered = asyncio.Event()

    async def core(event):
        entered.set()
        if cause == "raise":
            raise asyncio.CancelledError()
        await asyncio.Future()

    background_cleanup = AsyncMock()
    foreground_cleanup = AsyncMock()
    register(registry, "background", core, background=True, on_cancelled=background_cleanup)
    register(registry, "foreground", AsyncMock(), on_cancelled=foreground_cleanup)
    event = make_event()
    task = asyncio.create_task(runtime._run_background_handler(runtime._registry.registry["background"], event))
    await asyncio.wait_for(entered.wait(), 1)
    if cause == "external":
        task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    background_cleanup.assert_awaited_once_with(event)
    foreground_cleanup.assert_not_awaited()
    assert len(event.error_stack) == 0


@pytest.mark.parametrize("action", ["success", "accept", "error", "timeout"])
async def test_non_cancellation_does_not_notify_cancelled(action):
    registry = ApixHandlerRegistry(get_event_registry())
    runtime = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

    async def core(event):
        if action == "accept":
            event.accept()
        elif action == "error":
            raise ValueError("ordinary failure")
        elif action == "timeout":
            await asyncio.Future()

    cleanup = AsyncMock()
    register(registry, "work", core, on_cancelled=cleanup, time_out=0.01)
    await runtime._dispatch_event(make_event(), [registry.registry["work"]])
    cleanup.assert_not_awaited()


def test_on_cancelled_registration_validation_and_replacement():
    with pytest.raises(TypeError):
        ApixEventHandler(AsyncMock(), on_cancelled=1)
    registry = ApixHandlerRegistry(get_event_registry())
    cleanup = AsyncMock()
    handler = register(registry, "handler", AsyncMock(), on_cancelled=cleanup)
    assert registry.get_handler("handler").on_cancelled is cleanup
    with pytest.raises(ValueError, match="on_cancelled"):
        handler.add_on_cancelled_callback(AsyncMock(), exist_ok=False)
    with pytest.raises(TypeError):
        handler.add_on_cancelled_callback(1)
    replacement = AsyncMock()
    handler.add_on_cancelled_callback(replacement)
    assert handler.on_cancelled is replacement
    handler.on_cancelled = 1
    with pytest.raises(TypeError, match="on_cancelled"):
        registry.register_handler(handler, exist_ok=True)
