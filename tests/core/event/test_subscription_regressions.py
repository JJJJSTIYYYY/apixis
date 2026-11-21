"""Metadata isolation and complete-collection waiter cleanup regressions."""

import asyncio

import pytest

from apixis import (
    EventType, EventWaiter, get_event_loop, get_event_pipe,
    get_handler_meta, get_handler_registry, subscribe,
)
from apixis.core.event.subscription import waiter as waiter_module


async def test_metadata_edits_do_not_change_routing_or_cache(wait_for_dispatch):
    seen = []

    @subscribe("job.*", filter_event=["job.hidden"])
    async def receive(event):
        seen.append(event.event_name)

    registry = get_handler_registry()
    chain = registry.get_handlers_chain_for_event("job.visible")
    meta = get_handler_meta(receive.__name__)
    meta["subscribe"].clear()
    meta["filter_event"].append("job.visible")
    meta["priority"] = 99
    assert get_handler_meta(receive.__name__)["subscribe"] == ["job.*"]
    assert get_handler_meta(receive.__name__)["filter_event"] == ["job.hidden"]
    assert registry.get_handlers_chain_for_event("job.visible") is chain
    for name in ("job.visible", "job.hidden", "job.fresh", "other"):
        await get_event_pipe().post_event(event_type=EventType.INFO, event_name=name)
    await wait_for_dispatch(get_event_loop())
    assert seen == ["job.visible", "job.fresh"]


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_waiter_keeps_partial_listeners_and_cache_until_complete(
    point, wait_for_dispatch, monkeypatch,
):
    registry = get_handler_registry()
    before = set(registry.registry)
    waiter = EventWaiter(["job.*", "job.done", "saved"], point=point)
    listeners = set(registry.registry) - before
    chain = registry.get_subscription_chain_for_event("job.done")
    removed = []
    unregister = registry.unregister_handler

    def record_removal(name, **kwargs):
        removed.append(name)
        unregister(name, **kwargs)

    monkeypatch.setattr(registry, "unregister_handler", record_removal)
    try:
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.done", context="first",
        )
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == before | listeners
        assert not removed
        assert registry.get_subscription_chain_for_event("job.done") is chain
        # Repeated matches neither overwrite the first event nor unregister early.
        await get_event_pipe().post_event(
            event_type=EventType.INFO, event_name="job.done", context="second",
        )
        await wait_for_dispatch(get_event_loop())
        assert not removed
        await get_event_pipe().post_event(event_type=EventType.INFO, event_name="saved")
        await wait_for_dispatch(get_event_loop())
        # Cleanup occurs on collection, before the application reads the result.
        assert set(registry.registry) == before
        assert set(removed) == listeners
        result = await asyncio.wait_for(waiter.get_result(), 1)
        assert result["job.*"] is result["job.done"]
        assert result["job.done"].context == "first"
        assert await waiter.get_result() is result
    finally:
        waiter.close()


@pytest.mark.parametrize("options, error", [
    ({"point": "invalid"}, ValueError),
    ({"time_out": True}, TypeError),
    ({"time_out": float("nan")}, ValueError),
    ({"time_out": float("inf")}, ValueError),
    ({"filter": [1]}, ValueError),
    ({"filter": {}}, TypeError),
])
async def test_invalid_waiter_options_fail_before_registry_access(monkeypatch, options, error):
    def unexpected_registry():
        pytest.fail("Invalid arguments must fail before fetching the shared runtime")

    monkeypatch.setattr(waiter_module, "get_handler_registry", unexpected_registry)
    with pytest.raises(error):
        EventWaiter("reply", **options)


@pytest.mark.parametrize("names, error", [
    ([], ValueError), ([[]], ValueError), ([""], ValueError),
    (["reply", 1], ValueError), (None, TypeError), (1, TypeError),
])
async def test_invalid_waiter_patterns_fail_before_registry_access(monkeypatch, names, error):
    monkeypatch.setattr(waiter_module, "get_handler_registry", lambda: pytest.fail("registry accessed"))
    with pytest.raises(error):
        EventWaiter(names)


async def test_cancelling_one_result_wait_closes_shared_partial_collection(wait_for_dispatch):
    registry = get_handler_registry()
    baseline = set(registry.registry)
    waiter = EventWaiter(["first", "last"])
    independent = EventWaiter("last")
    getters = [asyncio.create_task(waiter.get_result()) for _ in range(2)]
    waiting = asyncio.create_task(waiter.wait())
    try:
        await asyncio.sleep(0)
        await get_event_pipe().post_event(event_type=EventType.INFO, event_name="first")
        await wait_for_dispatch(get_event_loop())
        assert all(not getter.done() for getter in getters)
        getters[0].cancel()
        outcomes = await asyncio.gather(*getters, waiting, return_exceptions=True)
        assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
        assert set(registry.registry) == baseline | set(registry._waiter_cache.waiters[independent])
        with pytest.raises(asyncio.CancelledError):
            await waiter.get_result()
        await get_event_pipe().post_event(event_type=EventType.INFO, event_name="last")
        assert (await independent.get_result()).event_name == "last"
        await wait_for_dispatch(get_event_loop())
        assert set(registry.registry) == baseline
    finally:
        waiter.close()
        independent.close()
        for task in (*getters, waiting):
            task.cancel()
        await asyncio.gather(*getters, waiting, return_exceptions=True)
