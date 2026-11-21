"""One collection Future owns the waiter's result, deadline and listeners."""

import asyncio
import gc
import weakref

import pytest

from apixis import EventWaiter, expect_event, post_event, subscribe
from apixis.core.event.factory import get_event_loop, get_handler_registry
from apixis.core.event.subscription import waiter as waiter_module


@pytest.mark.parametrize("time_out", [0, -1])
async def test_event_cancellation_checks_deadline_before_timer_runs(sample_event, time_out):
    predicate_calls = []
    with pytest.raises(TimeoutError):
        async with expect_event(
            sample_event.event_name,
            point="processed",
            time_out=time_out,
            predicate=lambda event: predicate_calls.append(event) or True,
        ) as waiter:
            # No scheduling point: the timeout callback has not run yet.
            await waiter._cancel(sample_event.event_name, sample_event)
            await waiter.wait()
    assert predicate_calls == []
    assert waiter._registry is None
    assert waiter._timeout_handle is None


async def test_processed_cancellation_cannot_win_over_zero_timeout():
    @subscribe("deadline.cancel")
    async def cancel(event):
        raise asyncio.CancelledError

    async with expect_event("deadline.cancel", point="processed", time_out=0) as waiter:
        await post_event(event_name="deadline.cancel")
        with pytest.raises(TimeoutError):
            await waiter.wait()


async def test_core_finish_keeps_none_result_before_timeout_callback():
    async with expect_event("deadline.close", time_out=0) as waiter:
        waiter.finish()
        assert await waiter.wait() is None


@pytest.mark.parametrize("outcome", ["result", "timeout", "cancel", "close", "finish"])
async def test_terminal_paths_release_registration_once(monkeypatch, outcome):
    registry = get_handler_registry()
    original = registry.unregister_waiter
    removed = []

    def unregister(waiter):
        removed.append(waiter)
        original(waiter)

    monkeypatch.setattr(registry, "unregister_waiter", unregister)
    async with expect_event("cleanup.once", time_out=60) as waiter:
        reader = asyncio.create_task(waiter.wait())
        await asyncio.sleep(0)
        if outcome == "result":
            await post_event(event_name="cleanup.once")
        elif outcome == "timeout":
            waiter._expire()
        elif outcome == "cancel":
            reader.cancel()
        elif outcome == "finish":
            waiter.finish()
        else:
            waiter.close()
        result = await asyncio.gather(reader, return_exceptions=True)
        if outcome == "timeout":
            assert isinstance(result[0], TimeoutError)
        elif outcome in ("cancel", "close"):
            assert isinstance(result[0], asyncio.CancelledError)
        elif outcome == "finish":
            assert result == [None]
        else:
            assert result[0].event_name == "cleanup.once"
        waiter.close()
    await asyncio.sleep(0)
    assert removed == [waiter]
    assert waiter not in registry._waiter_cache.waiters
    assert waiter._timeout_handle is None


@pytest.mark.parametrize("operation", ["wait", "get_result"])
async def test_wait_preserves_registration_deadline(operation):
    registry = get_handler_registry()
    baseline = set(registry.registry)
    async with expect_event("never", time_out=0.01) as waiter:
        future = waiter._future
        deadline = waiter._deadline
        with pytest.raises(TimeoutError):
            await getattr(waiter, operation)()
        assert waiter._future is future
        assert waiter._deadline == deadline
        assert future.done()
        assert waiter._timeout_handle is None
        assert set(registry.registry) == baseline
        with pytest.raises(TimeoutError):
            await waiter.wait()


@pytest.mark.parametrize("outcome", ["cancel", "error", "success"])
async def test_future_completion_cleans_up_without_a_wait_call(outcome, wait_for_dispatch):
    registry = get_handler_registry()
    baseline = set(registry.registry)
    waiter = EventWaiter("reply", time_out=60)
    future = waiter._future
    assert waiter.active
    assert waiter in registry._waiter_cache.waiters
    try:
        if outcome == "success":
            await post_event(event_name="reply")
            await wait_for_dispatch(get_event_loop())
        elif outcome == "error":
            future.set_exception(ValueError("collection failed"))
        else:
            future.cancel()
        # Future state is authoritative even before its cleanup callback runs.
        assert not waiter.active
        await registry.cancel_waiters()
        await asyncio.sleep(0)
        assert future.done()
        assert waiter._timeout_handle is None
        assert set(registry.registry) == baseline
        assert waiter not in registry._waiter_cache.waiters
        if outcome == "success":
            result = await waiter.wait()
            assert result.event_name == "reply"
            assert await waiter.get_result() is result
        else:
            error = ValueError if outcome == "error" else asyncio.CancelledError
            for _ in range(2):
                with pytest.raises(error):
                    await waiter.wait()
        assert waiter._future is future
    finally:
        waiter.close()


async def test_deadline_expires_without_a_reader():
    registry = get_handler_registry()
    baseline = set(registry.registry)
    async with expect_event("never", time_out=0) as waiter:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert waiter._future.done()
        assert set(registry.registry) == baseline
        with pytest.raises(TimeoutError):
            await waiter.wait()


async def test_explicit_close_cancels_partial_collection(wait_for_dispatch):
    registry = get_handler_registry()
    baseline = set(registry.registry)
    waiter = EventWaiter(["first", "last"], time_out=60)
    await post_event(event_name="first")
    await wait_for_dispatch(get_event_loop())
    waiter.close()
    waiter.close()
    assert waiter._future.cancelled()
    assert waiter._timeout_handle is None
    assert set(registry.registry) == baseline
    await post_event(event_name="last")
    await wait_for_dispatch(get_event_loop())
    with pytest.raises(asyncio.CancelledError):
        await waiter.wait()


async def test_context_exit_cancels_shared_future_and_prevents_reentry():
    registry = get_handler_registry()
    baseline = set(registry.registry)
    waiter = EventWaiter("never", time_out=60)
    async with waiter:
        first = asyncio.create_task(waiter.wait())
        second = asyncio.create_task(waiter.get_result())
        await asyncio.sleep(0)
    outcomes = await asyncio.gather(first, second, return_exceptions=True)
    assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
    assert set(registry.registry) == baseline
    with pytest.raises(RuntimeError, match="already been entered"):
        async with waiter:
            pass


async def test_cleanup_uses_the_registration_registry(monkeypatch):
    registry = get_handler_registry()
    baseline = set(registry.registry)
    waiter = EventWaiter("never", time_out=60)
    monkeypatch.setattr(waiter_module, "get_handler_registry", lambda: pytest.fail("registry accessed"))
    waiter.close()
    assert set(registry.registry) == baseline


@pytest.mark.parametrize("outcome", ["close", "success"])
async def test_completed_waiter_releases_its_timer_reference(outcome, wait_for_dispatch):
    waiter = EventWaiter("reply", time_out=3600)
    reference = weakref.ref(waiter)
    if outcome == "success":
        await post_event(event_name="reply")
        await wait_for_dispatch(get_event_loop())
    else:
        waiter.close()
    await asyncio.sleep(0)
    del waiter
    gc.collect()
    assert reference() is None
