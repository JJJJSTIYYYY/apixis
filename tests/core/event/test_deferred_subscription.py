"""Deferred event collection and waiter-owned subscription lifecycles."""

import asyncio
import gc
import weakref

import pytest

from apixis.core.event import EventType
from apixis.core.event import subscription
from apixis.core.event.base import ApixEvent, ApixEventHandler
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.factory import get_event_loop, get_event_pipe, get_handler_registry
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event.subscription import EventWaiter, await_for, wait_for_event


@pytest.mark.parametrize("event_names", ["reply", ["reply"], {"reply"}])
async def test_await_for_deferred_timeout_preserves_listeners_and_result_shape(
    event_names, wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = await await_for(event_names, time_out=0, keep_listening=True)
    assert isinstance(waiter, EventWaiter)
    assert len(set(registry.registry) - registered) == 1
    try:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="late result",
        )
        await wait_for_dispatch(get_event_loop())
        # Completion releases listeners before the eventual result is read.
        assert set(registry.registry) == registered
        result = await waiter.get_result()
        event = result if isinstance(event_names, str) else result["reply"]
        assert isinstance(event, ApixEvent)
        assert event.context == "late result"
        assert await waiter.get_result() is result
    finally:
        waiter.close()


async def test_explicit_wait_timeout_detaches_on_normal_context_exit(wait_for_dispatch):
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event("reply") as waiter:
        assert await waiter.wait(time_out=0, keep_listening=True) is waiter
    assert len(set(registry.registry) - registered) == 1
    try:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="after exit",
        )
        result = await asyncio.wait_for(waiter.get_result(), 1)
        assert result.context == "after exit"
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
    finally:
        waiter.close()


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_deferred_multi_event_collection_retains_first_wildcard_match(
    point, wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event(["job.*", "saved"], point=point) as waiter:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.first", context="first",
        )
        await wait_for_dispatch(get_event_loop())
        assert await waiter.wait(time_out=0, keep_listening=True) is waiter
    pending = asyncio.create_task(waiter.get_result())
    try:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.second", context="duplicate",
        )
        await wait_for_dispatch(get_event_loop())
        assert not pending.done()
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="saved", context="last",
        )
        result = await asyncio.wait_for(pending, 1)
        assert set(result) == {"job.*", "saved"}
        assert result["job.*"].event_name == "job.first"
        assert result["job.*"].context == "first"
        assert result["saved"].context == "last"
        assert set(registry.registry) == registered
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        waiter.close()
        await wait_for_dispatch(get_event_loop())


async def test_hard_timeout_closes_before_context_exit():
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event("never") as waiter:
        assert len(set(registry.registry) - registered) == 1
        with pytest.raises(TimeoutError):
            await waiter.wait(time_out=0)
        assert set(registry.registry) == registered


async def test_wait_call_can_disable_configured_keep_listening():
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event("never", keep_listening=True) as waiter:
        with pytest.raises(TimeoutError):
            await waiter.wait(time_out=0, keep_listening=False)
        assert set(registry.registry) == registered


async def test_configured_hard_deadline_closes_without_waiting():
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event("never", time_out=0) as waiter:
        # Let the configured deadline expire without invoking wait().
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert set(registry.registry) == registered
        with pytest.raises(TimeoutError):
            await waiter.wait()


@pytest.mark.parametrize("keep_listening", [False, True])
async def test_success_closes_listeners_and_retains_result_in_context(
    keep_listening, wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event(
        "reply", time_out=60, keep_listening=keep_listening,
    ) as waiter:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="ready",
        )
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
        result = await waiter.wait()
        assert result.context == "ready"
        assert await waiter.wait() is result
        assert await waiter.get_result() is result


async def test_keep_listening_configuration_alone_does_not_detach():
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event("never", time_out=60, keep_listening=True):
        assert len(set(registry.registry) - registered) == 1
    assert set(registry.registry) == registered


async def test_context_error_after_deferred_timeout_closes_listeners():
    registry = get_handler_registry()
    registered = set(registry.registry)
    with pytest.raises(ValueError, match="publication failed"):
        async with wait_for_event("never", time_out=0, keep_listening=True) as waiter:
            assert await waiter.wait(time_out=0) is waiter
            raise ValueError("publication failed")
    assert set(registry.registry) == registered


@pytest.mark.parametrize("operation", ["wait", "get_result"])
async def test_cancellation_closes_listeners_even_when_keep_listening(operation):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("never", keep_listening=True)
    if operation == "get_result":
        assert await waiter.wait(time_out=0) is waiter
        pending = asyncio.create_task(waiter.get_result())
    else:
        pending = asyncio.create_task(waiter.wait())
    try:
        await asyncio.sleep(0)
        assert not pending.done()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert set(registry.registry) == registered
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        waiter.close()


async def test_abandoned_deferred_waiter_can_be_closed_idempotently():
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = await await_for(["first", "second"], time_out=0, keep_listening=True)
    assert len(set(registry.registry) - registered) == 2
    waiter.close()
    waiter.close()
    assert set(registry.registry) == registered


async def test_waiter_closes_the_registry_that_registered_its_handlers(monkeypatch):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = await await_for("never", time_out=0, keep_listening=True)
    installed = set(registry.registry) - registered
    assert len(installed) == 1
    replacement = ApixHandlerRegistry(ApixEventRegistry())

    async def unrelated(event):
        pass

    handler = ApixEventHandler(unrelated, name=next(iter(installed)))
    handler.subscribe = ["unrelated"]
    handler.priority = 1
    replacement.register_handler(handler)
    monkeypatch.setattr(subscription, "get_handler_registry", lambda: replacement)
    waiter.close()
    assert set(registry.registry) == registered
    assert replacement.get_handler(handler.name) is handler


async def test_concurrent_wait_and_get_result_share_collection_after_budget_override(
    wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("reply", time_out=60)
    waiting = asyncio.create_task(waiter.wait())
    result_waiting = asyncio.create_task(waiter.get_result())
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        assert not result_waiting.done()
        # Resetting the shared budget wakes existing waits without cancelling
        # the result retrieval or the underlying event collection.
        assert await waiter.wait(time_out=0, keep_listening=True) is waiter
        assert await asyncio.wait_for(waiting, 1) is waiter
        assert not result_waiting.done()
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="shared",
        )
        result = await asyncio.wait_for(result_waiting, 1)
        assert result.context == "shared"
        assert await waiter.get_result() is result
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
    finally:
        waiting.cancel()
        result_waiting.cancel()
        await asyncio.gather(waiting, result_waiting, return_exceptions=True)
        waiter.close()


async def test_numeric_wait_budget_can_resume_waiting_after_deferred_timeout(
    wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = await await_for("reply", time_out=0, keep_listening=True)
    waiting = asyncio.create_task(waiter.wait(time_out=60))
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="new budget",
        )
        result = await asyncio.wait_for(waiting, 1)
        assert isinstance(result, ApixEvent)
        assert result.context == "new budget"
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        waiter.close()


async def test_expired_soft_deadline_without_wait_does_not_detach():
    registry = get_handler_registry()
    registered = set(registry.registry)
    async with wait_for_event("never", time_out=0, keep_listening=True):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert len(set(registry.registry) - registered) == 1
    assert set(registry.registry) == registered


async def test_get_result_reports_hard_timeout_to_concurrent_waits():
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("never")
    waiting = asyncio.create_task(waiter.wait())
    result_waiting = asyncio.create_task(waiter.get_result())
    try:
        await asyncio.sleep(0)
        with pytest.raises(TimeoutError):
            await waiter.wait(time_out=0)
        outcomes = await asyncio.gather(waiting, result_waiting, return_exceptions=True)
        assert all(isinstance(outcome, TimeoutError) for outcome in outcomes)
        assert set(registry.registry) == registered
    finally:
        waiting.cancel()
        result_waiting.cancel()
        await asyncio.gather(waiting, result_waiting, return_exceptions=True)
        waiter.close()


@pytest.mark.parametrize("outcome", ["close", "success"])
async def test_closed_waiter_is_not_retained_by_its_long_deadline(outcome, wait_for_dispatch):
    waiter = EventWaiter("reply", time_out=3600, keep_listening=True)
    reference = weakref.ref(waiter)
    if outcome == "success":
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply",
        )
        await wait_for_dispatch(get_event_loop())
        assert isinstance(await waiter.get_result(), ApixEvent)
    else:
        waiter.close()
    del waiter
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize("explicit_none", [False, True])
async def test_no_argument_wait_waits_indefinitely_after_deferred_return(
    explicit_none, wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("reply", time_out=60, keep_listening=True)
    installed = set(registry.registry) - registered
    assert await asyncio.wait_for(waiter.wait(time_out=0), 1) is waiter
    waiting = asyncio.create_task(waiter.wait(time_out=None) if explicit_none else waiter.wait())
    try:
        await asyncio.sleep(0)
        # A later no-argument call starts another wait instead of immediately
        # returning the waiter for the already consumed timeout.
        assert not waiting.done()
        assert waiter._deadline is None
        assert waiter._timeout_handle is None
        assert set(registry.registry) - registered == installed
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="resumed",
        )
        result = await asyncio.wait_for(waiting, 1)
        assert isinstance(result, ApixEvent)
        assert result.context == "resumed"
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        waiter.close()


async def test_repeated_deferred_waits_keep_partial_results_and_handler_identity(
    wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter(["job.*", "saved"], time_out=60, keep_listening=True)
    installed = set(registry.registry) - registered
    try:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.first", context="first",
        )
        await wait_for_dispatch(get_event_loop())
        assert await asyncio.wait_for(waiter.wait(time_out=0), 1) is waiter
        for event_name in ("job.second", "job.third"):
            waiting = asyncio.create_task(waiter.wait(time_out=60))
            try:
                await asyncio.sleep(0)
                assert not waiting.done()
                assert set(registry.registry) - registered == installed
                await get_event_pipe().post_event(
                    event_type=EventType.INFO, event_name=event_name, context="duplicate",
                )
                await wait_for_dispatch(get_event_loop())
                assert not waiting.done()
                waiter._expire()
                assert await asyncio.wait_for(waiting, 1) is waiter
            finally:
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="saved", context="last",
        )
        result = await waiter.get_result()
        assert result["job.*"].event_name == "job.first"
        assert result["job.*"].context == "first"
        assert result["saved"].context == "last"
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
    finally:
        waiter.close()


async def test_resumed_wait_can_change_soft_timeout_to_hard_timeout():
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("never", time_out=60, keep_listening=True)
    assert await asyncio.wait_for(waiter.wait(time_out=0), 1) is waiter
    waiting = asyncio.create_task(waiter.wait(time_out=60, keep_listening=False))
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        waiter._expire()
        with pytest.raises(TimeoutError):
            await waiting
        assert set(registry.registry) == registered
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        waiter.close()


async def test_cancelling_resumed_wait_closes_existing_listeners():
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("never", time_out=60, keep_listening=True)
    assert await asyncio.wait_for(waiter.wait(time_out=0), 1) is waiter
    waiting = asyncio.create_task(waiter.wait())
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert set(registry.registry) == registered
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        waiter.close()


@pytest.mark.parametrize("explicit_none", [False, True])
@pytest.mark.parametrize("keep_listening", [False, True])
async def test_first_wait_without_budget_cancels_configured_deadline(
    explicit_none, keep_listening, wait_for_dispatch,
):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("reply", time_out=60, keep_listening=keep_listening)
    installed = set(registry.registry) - registered
    waiting = asyncio.create_task(waiter.wait(time_out=None) if explicit_none else waiter.wait())
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        assert waiter._deadline is None
        assert waiter._timeout_handle is None
        assert set(registry.registry) - registered == installed
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="first infinite wait",
        )
        result = await asyncio.wait_for(waiting, 1)
        assert result.context == "first infinite wait"
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        waiter.close()


async def test_first_wait_without_budget_replaces_elapsed_soft_deadline(wait_for_dispatch):
    waiter = EventWaiter("reply", time_out=60, keep_listening=True)
    waiter._expire()
    waiting = asyncio.create_task(waiter.wait())
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        assert waiter._deadline is None
        assert waiter._timeout_handle is None
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="after elapsed budget",
        )
        result = await asyncio.wait_for(waiting, 1)
        assert isinstance(result, ApixEvent)
        assert result.context == "after elapsed budget"
        await wait_for_dispatch(get_event_loop())
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        waiter.close()


async def test_none_budget_replaces_active_concurrent_finite_wait(wait_for_dispatch):
    registry = get_handler_registry()
    registered = set(registry.registry)
    waiter = EventWaiter("reply")
    finite = asyncio.create_task(waiter.wait(time_out=60))
    indefinite = None
    try:
        await asyncio.sleep(0)
        assert waiter._deadline is not None
        indefinite = asyncio.create_task(waiter.wait(time_out=None))
        await asyncio.sleep(0)
        assert not finite.done()
        assert not indefinite.done()
        assert waiter._deadline is None
        assert waiter._timeout_handle is None
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="reply", context="shared unlimited",
        )
        first, second = await asyncio.wait_for(asyncio.gather(finite, indefinite), 1)
        assert first is second
        assert first.context == "shared unlimited"
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == registered
    finally:
        tasks = [finite] if indefinite is None else [finite, indefinite]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        waiter.close()
