"""Terminal admission, cancellation delivery and waiter completion contracts."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from apixis import (
    CoreClosedError,
    CoreState,
    EventWaiter,
    close_core,
    create_core,
    post_and_wait,
    post_event,
    start_core,
    stop_core,
    subscribe,
)
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.factory import get_handler_registry
from apixis.core.event.base.event import EventType, create_event


async def test_failed_consumer_is_logged_and_does_not_interrupt_closure(monkeypatch):
    from apixis.core.event.core import ApixisCore
    from apixis.core.event import factory

    core = ApixisCore._create_core()
    entered, cleaned, finish_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    fail_consumer = asyncio.Event()
    error = RuntimeError("consumer failed")
    errors = Mock()

    async def running(event):
        entered.set()
        await asyncio.Future()

    async def cleanup(event):
        cleaned.set()
        await finish_cleanup.wait()

    async def failing_get():
        await fail_consumer.wait()
        raise error

    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    loop.set_task_factory(None)
    with monkeypatch.context() as patch:
        patch.setattr(factory, "_core", core)
        patch.setattr("apixis.core.event.event_loop.Logger.error", errors)
        ApixEventHandler(running, on_cancelled=cleanup).register("close.active")
        await core.start_core()
        await post_event(event_name="close.active")
        await entered.wait()
        await core.stop_core()
        patch.setattr(core._event_pipe, "get", failing_get)
        await core.start_core()
        waiter = EventWaiter("never.produced")
        pending = create_event(event_type=EventType.INFO, event_name="pending")
        core._event_pipe.put_nowait(pending)
        core._event_loop._pending_dispatch = core._event_pipe.get_nowait(), []
        await post_event(event_name="queued")
        consumer = core._event_loop._event_consumer_task
        fail_consumer.set()
        await asyncio.sleep(0)
        assert consumer.done()
        assert core._event_loop._event_consumer_task is consumer
        loop.set_task_factory(asyncio.eager_task_factory)
        closing = asyncio.create_task(core.close_core())
        try:
            async with asyncio.timeout(1):
                await cleaned.wait()
                # Waiter completion precedes joining the blocked cleanup hook.
                assert await waiter.wait() is None
            assert core.state is CoreState.CLOSING
            assert core._event_pipe.empty()
            assert core._event_loop._pending_dispatch is None
            assert not closing.done()
            finish_cleanup.set()
            await closing
            errors.assert_called_once()
            assert "consumer failed" in errors.call_args.args[0]
            assert core.state is CoreState.CLOSED
            assert not core._event_loop._dispatch_tasks
            assert not core._event_loop._background_handler_tasks
            assert not core._handler_registry.registry
            await asyncio.wait_for(core._event_pipe.join(), 1)
        finally:
            loop.set_task_factory(original_factory)
            finish_cleanup.set()
            await asyncio.gather(closing, return_exceptions=True)


async def test_cleanup_failure_does_not_report_closed(monkeypatch):
    from apixis.core.event.core import ApixisCore

    core = ApixisCore._create_core()
    original = core._handler_registry.close
    error = RuntimeError("registration cleanup failed")
    monkeypatch.setattr(core._handler_registry, "close", AsyncMock(side_effect=error))
    with pytest.raises(RuntimeError) as raised:
        await core.close_core()
    assert raised.value is error
    assert core.state is CoreState.CLOSING
    assert core._event_pipe.closed
    await original()


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.parametrize("phase", ["created", "running", "stopped"])
@pytest.mark.parametrize("point", ["received", "processed"])
@pytest.mark.parametrize("patterns", ["never.one", ["never.one", "never.*"], {"never.one", "never.*"}])
async def test_close_resolves_never_produced_waits_without_cancelling_future(
    cleanup_event_runtime, phase, point, patterns,
):
    core = cleanup_event_runtime
    if phase != "running":
        await stop_core()
    if phase == "created":
        await close_core()
        core = create_core()

    def reject(event):
        raise AssertionError(
            "Direct core closure must not invoke payload predicates"
        )

    waiter = EventWaiter(patterns, point=point, predicate=reject)
    callbacks = []
    for handler in core._handler_registry._waiter_cache.waiters[waiter].values():
        callback = AsyncMock(wraps=handler._on_cancelled)
        handler._on_cancelled = callback
        callbacks.append(callback)
    readers = [asyncio.create_task(waiter.wait()) for _ in range(2)]
    await asyncio.sleep(0)
    await asyncio.wait_for(close_core(), 1)
    assert not waiter.active
    for callback in callbacks:
        callback.assert_not_awaited()
    for reader in readers:
        assert await reader is None
    assert await waiter.get_result() is None
    assert not waiter._future.cancelled()
    assert core.state is CoreState.CLOSED
    assert not core._handler_registry._waiter_cache.positions
    assert not core._handler_registry._waiter_cache.waiters
    assert not core._handler_registry.registry


async def test_partial_collection_returns_none_on_cancellation(
    cleanup_event_runtime,
):
    waiter = EventWaiter(["received", "missing"])
    reader = asyncio.create_task(waiter.wait())
    await post_event(event_name="received", context=42)
    await until(lambda: waiter._events["received"] is not None)
    await close_core()
    assert await reader is None
    assert await waiter.get_result() is None
    assert not waiter._future.cancelled()


@pytest.mark.parametrize("background", [False, True])
@pytest.mark.parametrize("failure", ["error", "cancel"])
async def test_close_notifies_all_handlers_despite_cleanup_failures(
    cleanup_event_runtime, background, failure
):
    core = cleanup_event_runtime
    entered = asyncio.Event()
    notified = []
    after = AsyncMock()

    async def running(event):
        entered.set()
        await asyncio.Future()

    async def broken(event):
        notified.append("broken")
        assert event.cancelled is (not background)
        if failure == "error":
            raise ValueError("cleanup failed")
        raise asyncio.CancelledError("cleanup cancelled")

    handler = ApixEventHandler(running, on_cancelled=broken, background=background)
    handler.register("active")
    ApixEventHandler(
        after,
        on_cancelled=lambda_event_cleanup(notified),
        background=background,
        name="after",
    ).register("active")
    await post_event(event_name="active")
    await entered.wait()
    await asyncio.wait_for(close_core(), 1)
    assert "broken" in notified
    if not background:
        assert "after" in notified
        after.assert_not_awaited()
    assert not core._event_loop._dispatch_tasks
    assert not core._event_loop._background_handler_tasks
    assert core._event_loop._event_semaphore.in_use == 0


def lambda_event_cleanup(notified):
    async def cleanup(event):
        assert event.cancelled
        notified.append("after")

    return cleanup


async def test_close_completes_same_name_requests_with_none(cleanup_event_runtime):
    entered = set()

    @subscribe("request")
    async def block(event):
        entered.add(event.context)
        await asyncio.Future()

    requests = [
        asyncio.create_task(post_and_wait(event_name="request", context=index))
        for index in range(4)
    ]
    await until(lambda: len(entered) == 4)
    await close_core()
    results = await asyncio.gather(*requests)
    assert results == [None] * 4


async def test_one_cancelled_request_does_not_cancel_other_same_name_requests(
    cleanup_event_runtime,
):
    entered = set()
    release = asyncio.Event()

    @subscribe("request")
    async def handle(event):
        entered.add(event.context)
        await release.wait()
        if event.context == 0:
            raise asyncio.CancelledError

    requests = [
        asyncio.create_task(post_and_wait(event_name="request", context=index))
        for index in range(4)
    ]
    await until(lambda: len(entered) == 4)
    release.set()
    results = await asyncio.gather(*requests)
    assert results[0] is None
    assert [event.context for event in results[1:]] == [1, 2, 3]
    assert not get_handler_registry()._waiter_cache.positions


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_direct_completion_finishes_pending_wait_before_expired_timer_runs(
    cleanup_event_runtime, point,
):
    registry = cleanup_event_runtime._handler_registry
    waiter = EventWaiter("request", point=point, time_out=0)
    # Direct completion wins while the expired timer callback is still pending.
    await registry.cancel_waiters()
    assert await waiter.wait() is None
    assert not waiter._future.cancelled()
    assert not registry._waiter_cache.positions
    assert not registry._waiter_cache.waiters


async def test_managed_waiter_keeps_normal_asyncio_task_cancellation(
    cleanup_event_runtime,
):
    waiter = EventWaiter("never.produced")
    entered = asyncio.Event()

    @subscribe("parent")
    async def parent(event):
        async with waiter:
            entered.set()
            await waiter.wait()

    reader = asyncio.create_task(waiter.wait())
    await post_event(event_name="parent")
    await entered.wait()
    await close_core()
    with pytest.raises(asyncio.CancelledError):
        await reader
    assert waiter._future.cancelled()


async def test_business_close_still_cancels_the_shared_future(cleanup_event_runtime):
    waiter = EventWaiter("never")
    reader = asyncio.create_task(waiter.wait())
    await asyncio.sleep(0)
    waiter.close()
    with pytest.raises(asyncio.CancelledError):
        await reader
    assert waiter._future.cancelled()
    assert not get_handler_registry()._waiter_cache.positions


async def test_close_discards_queued_events_without_resolving_or_notifying_handlers(
    cleanup_event_runtime, monkeypatch,
):
    core = cleanup_event_runtime
    await stop_core()
    business = AsyncMock()
    cleanup = AsyncMock()
    ApixEventHandler(business, on_cancelled=cleanup).register("queued")
    request = asyncio.create_task(post_and_wait(event_name="queued", context=7))
    await until(lambda: core._event_pipe.qsize() == 1)
    queued = create_event(event_type=EventType.DEFAULT, event_name="queued", context=8)
    core._event_pipe.put_nowait(queued)
    resolve = Mock(side_effect=AssertionError("Discarding queued events must not resolve handlers"))
    monkeypatch.setattr(core._handler_registry, "get_handlers_for_event", resolve)
    await close_core()
    assert await request is None
    business.assert_not_awaited()
    cleanup.assert_not_awaited()
    resolve.assert_not_called()
    assert not queued.cancelled
    assert core._event_pipe.empty()
    await asyncio.wait_for(core._event_pipe.join(), 1)


async def test_close_preserves_completed_event_before_task_cleanup_callback(
    cleanup_event_runtime,
):
    result = await post_and_wait(event_name="completed", context=42)
    assert result is not None
    await close_core()
    assert result.context == 42
    assert not result.cancelled


async def test_close_caller_cancellation_does_not_abort_shared_cleanup(
    cleanup_event_runtime,
):
    entered, cleanup_entered, release = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )

    async def run(event):
        entered.set()
        await asyncio.Future()

    async def cleanup(event):
        cleanup_entered.set()
        await release.wait()

    ApixEventHandler(run, on_cancelled=cleanup).register("active")
    await post_event(event_name="active")
    await entered.wait()
    closing = asyncio.create_task(close_core())
    await cleanup_entered.wait()
    assert cleanup_event_runtime.state is CoreState.CLOSING
    with pytest.raises(RuntimeError, match="close_core"):
        create_core()
    with pytest.raises(CoreClosedError):
        await start_core()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    release.set()
    await asyncio.wait_for(close_core(), 1)
    assert cleanup_event_runtime.state is CoreState.CLOSED


@pytest.mark.parametrize("eager", [False, True], ids=["default", "eager"])
async def test_close_from_handler_and_its_cancel_hook_does_not_wait_for_itself(
    cleanup_event_runtime,
    eager,
):
    entered = asyncio.Event()
    notified = asyncio.Event()

    async def cleanup(event):
        await close_core()
        notified.set()

    async def run(event):
        entered.set()
        await close_core()

    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        ApixEventHandler(run, on_cancelled=cleanup).register("self.close")
        await post_event(event_name="self.close")
        await entered.wait()
        await asyncio.wait_for(close_core(), 1)
        assert notified.is_set()
        assert cleanup_event_runtime.state is CoreState.CLOSED
    finally:
        loop.set_task_factory(previous)


@pytest.mark.parametrize("eager", [False, True], ids=["default", "eager"])
async def test_first_consumer_turn_can_request_close_during_startup(
    cleanup_event_runtime, eager,
):
    await close_core()
    core = create_core()
    notified = asyncio.Event()

    async def cleanup(event):
        await close_core()
        notified.set()

    async def run(event):
        await close_core()

    ApixEventHandler(run, on_cancelled=cleanup).register("startup.close")
    await post_event(event_name="startup.close")
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        async with asyncio.timeout(1):
            await start_core()
            await close_core()
        assert notified.is_set()
        assert core.state is CoreState.CLOSED
        assert core._start_task is None
        assert not core._event_loop._dispatch_tasks
    finally:
        loop.set_task_factory(previous)


async def test_closed_component_references_cannot_admit_or_register_work(
    cleanup_event_runtime,
):
    core = cleanup_event_runtime
    await close_core()
    with pytest.raises(CoreClosedError):
        await core._event_pipe.post_event(event_name="late")
    with pytest.raises(CoreClosedError):
        core._event_pipe.put_nowait(object())
    with pytest.raises(CoreClosedError):
        EventWaiter("late")
    with pytest.raises(CoreClosedError):
        ApixEventHandler(AsyncMock()).register("late")
    replacement = create_core()
    assert replacement is not core
    assert not replacement._event_pipe.closed


@pytest.mark.parametrize("phase", ["finally", "on_cancelled"])
async def test_close_breaks_event_waits_inside_cancellation_cleanup(
    cleanup_event_runtime, phase
):
    core = cleanup_event_runtime
    entered, cleaned = asyncio.Event(), asyncio.Event()
    waiter = EventWaiter("never.cleanup")
    request_event = None

    async def complete_cleanup():
        assert await waiter.wait() is None
        cleaned.set()

    async def run(event):
        nonlocal request_event
        request_event = event
        entered.set()
        try:
            await asyncio.Future()
        finally:
            if phase == "finally":
                await complete_cleanup()

    async def cancelled(event):
        if phase == "on_cancelled":
            await complete_cleanup()

    ApixEventHandler(run, on_cancelled=cancelled).register("cleanup.request")
    request = asyncio.create_task(
        post_and_wait(event_name="cleanup.request", context=99)
    )
    await entered.wait()
    await asyncio.wait_for(close_core(), 1)
    assert await request is None
    assert request_event.context == 99 and request_event.cancelled
    assert cleaned.is_set()
    assert not waiter._future.cancelled()
    assert core.state is CoreState.CLOSED


async def test_close_cancels_running_work_and_discards_pending_and_queued_events(
    cleanup_event_runtime, monkeypatch
):
    import importlib
    from apixis.core.event.event_pipe import ApixEventPipe

    await close_core()
    core_module = importlib.import_module("apixis.core.event.core")
    loop_module = importlib.import_module("apixis.core.event.event_loop")
    monkeypatch.setattr(core_module, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    monkeypatch.setattr(loop_module, "EVENT_LOOP_BACKPRESSURE", 1)
    core = create_core()
    await start_core()
    entered = asyncio.Event()
    executed, cancelled = [], []

    async def run(event):
        executed.append(event.event_name)
        entered.set()
        await asyncio.Future()

    async def cleanup(event):
        cancelled.append(event.event_name)
        assert event.cancelled

    ApixEventHandler(run, on_cancelled=cleanup).register("work.*")
    waiter = EventWaiter(
        ["work.pending", "work.queued", "never.produced"], point="processed"
    )
    reader = asyncio.create_task(waiter.wait())
    await post_event(event_name="work.active")
    await entered.wait()
    await post_event(event_name="work.pending", context=1)
    await until(lambda: core._event_loop._pending_dispatch is not None)
    await post_event(event_name="work.queued", context=2)
    blocked = asyncio.create_task(post_event(event_name="work.blocked"))
    await until(lambda: bool(core._event_pipe._blocked_puts))
    await asyncio.wait_for(close_core(), 1)
    with pytest.raises(CoreClosedError):
        await blocked
    assert await reader is None
    assert executed == ["work.active"]
    assert cancelled == ["work.active"]
    assert core._event_pipe.empty()
    assert core._event_loop._pending_dispatch is None
    assert not core._event_pipe._blocked_puts
    assert core._event_loop._event_semaphore.in_use == 0
    await asyncio.wait_for(core._event_pipe.join(), 1)


async def test_application_cancellation_still_cancels_future_during_core_close(
    cleanup_event_runtime, monkeypatch
):
    core = cleanup_event_runtime
    waiter = EventWaiter("never")
    reader = asyncio.create_task(waiter.wait())
    entered, release = asyncio.Event(), asyncio.Event()
    original = core._handler_registry.cancel_waiters

    async def pause_notifications():
        entered.set()
        await release.wait()
        await original()

    monkeypatch.setattr(core._handler_registry, "cancel_waiters", pause_notifications)
    closing = asyncio.create_task(close_core())
    await entered.wait()
    reader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reader
    assert waiter._future.cancelled()
    release.set()
    await asyncio.wait_for(closing, 1)


@pytest.mark.parametrize("background", [False, True])
@pytest.mark.parametrize("outcome", ["raise", "suppress"])
@pytest.mark.parametrize("with_tail", [False, True])
async def test_close_cannot_resume_business_after_cancellation_is_transformed(
    cleanup_event_runtime, background, outcome, with_tail,
):
    entered = asyncio.Event()
    cleanup = AsyncMock()
    error_handler = AsyncMock()
    tail = AsyncMock()

    async def run(event):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            if outcome == "raise":
                raise ValueError("failure while cancelling")

    ApixEventHandler(
        run, on_error=error_handler, on_cancelled=cleanup, background=background
    ).register("cancel.transform")
    if with_tail and not background:
        ApixEventHandler(tail, stop_when_error=False, name="tail").register(
            "cancel.transform"
        )
    await post_event(event_name="cancel.transform")
    await entered.wait()
    await asyncio.wait_for(close_core(), 1)
    cleanup.assert_awaited_once()
    error_handler.assert_not_awaited()
    tail.assert_not_awaited()
    assert cleanup_event_runtime.state is CoreState.CLOSED


@pytest.mark.parametrize("background", [False, True])
@pytest.mark.parametrize("accepted", [False, True])
async def test_close_stops_handler_phases_after_upstream_hook_swallows_cancellation(
    cleanup_event_runtime, background, accepted,
):
    entered = asyncio.Event()
    core_callback = AsyncMock()
    accepted_callback = AsyncMock()
    error_callback = AsyncMock()
    cleanup = AsyncMock()

    @subscribe("cancel.upstream", priority=2)
    async def upstream(event):
        raise ValueError("upstream failure")

    async def on_has_error(event):
        if accepted:
            event.accept()
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass

    subscribe(
        "cancel.upstream", name="downstream", priority=1,
        background=background, stop_when_error=False,
        on_has_error=on_has_error, on_accepted=accepted_callback,
        on_error=error_callback, on_cancelled=cleanup,
    )(core_callback)

    await post_event(event_name="cancel.upstream")
    await asyncio.wait_for(entered.wait(), 1)
    await asyncio.wait_for(close_core(), 1)

    core_callback.assert_not_awaited()
    accepted_callback.assert_not_awaited()
    error_callback.assert_not_awaited()
    cleanup.assert_awaited_once()
    event = cleanup.call_args.args[0]
    assert "downstream" not in event.seen
    assert cleanup_event_runtime.state is CoreState.CLOSED
