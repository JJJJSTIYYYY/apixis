"""Waiter cache isolation, indexed matching and dispatch snapshot contracts."""

import asyncio
from collections import Counter, OrderedDict
from unittest.mock import patch

import pytest

from apixis import (
    ApixEventHandler, ApixEventRegistry, ApixHandlerRegistry, EventType,
    EventWaiter, get_event_loop, get_event_pipe, get_handler_registry, subscribe,
)
from apixis.core.utils.exception import EventHandlerAlreadyRegisteredError


@pytest.mark.parametrize("pattern", ["reply.fixed", "reply.*"])
@pytest.mark.parametrize("point", ["received", "processed"])
async def test_repeated_waits_preserve_ordinary_cache(pattern, point, wait_for_dispatch):
    registry = get_handler_registry()

    @subscribe("reply.*")
    async def ordinary(event):
        pass

    normal = registry.get_subscription_chain_for_event("reply.fixed")
    builds = Counter()
    resolve = registry._resolve_handler_chain

    def count_builds(name):
        builds[name] += 1
        return resolve(name)

    with patch.object(registry, "_resolve_handler_chain", side_effect=count_builds):
        for _ in range(128):
            async with EventWaiter(pattern, point=point) as waiter:
                assert registry.get_subscription_chain_for_event("reply.fixed") is normal
                await get_event_pipe().post_event(
                    event_type=EventType.INFO, event_name="reply.fixed",
                )
                await asyncio.wait_for(waiter.get_result(), 1)
            await wait_for_dispatch(get_event_loop())
            assert registry.cached_chain["reply.fixed"] is normal
    assert not builds
    assert normal == [ordinary.__name__]
    assert not registry._waiter_cache.positions
    assert not registry._waiter_cache._chains


@pytest.mark.parametrize("outcome", ["close", "timeout", "cancel"])
async def test_wait_cleanup_never_invalidates_normal_cache(outcome):
    registry = get_handler_registry()
    normal = registry.get_subscription_chain_for_event("reply.fixed")
    waiter = EventWaiter("reply.*", time_out=0 if outcome == "timeout" else None)
    try:
        assert registry.get_handlers_for_event("reply.fixed") == list(registry._waiter_cache.waiters[waiter].values())
        if outcome == "timeout":
            with pytest.raises(TimeoutError):
                await waiter.wait()
        elif outcome == "cancel":
            task = asyncio.create_task(waiter.wait())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        waiter.close()
    assert registry.get_subscription_chain_for_event("reply.fixed") is normal
    assert registry.get_handlers_for_event("reply.fixed") == []
    assert not registry._waiter_cache._chains


async def test_mixed_waits_keep_registration_order_and_snapshot_references():
    registry = get_handler_registry()

    @subscribe("reply.fixed", priority=1e100)
    async def first(event):
        pass

    @subscribe("reply.fixed", priority=-1e100)
    async def last(event):
        pass

    waiters = [
        EventWaiter("reply.*", point="processed"),
        EventWaiter("reply.fixed", point="received"),
        EventWaiter("reply.*", point="received"),
        EventWaiter("reply.fixed", point="processed"),
    ]
    handlers = [next(iter(registry._waiter_cache.waiters[waiter].values())) for waiter in waiters]
    expected = [handlers[1], handlers[2], registry.get_handler("first"),
                registry.get_handler("last"), handlers[0], handlers[3]]
    snapshot = registry.get_handlers_for_event("reply.fixed")
    assert snapshot == expected
    assert registry.get_handlers_chain_for_event("reply.fixed") == [
        handler.name for handler in expected
    ]
    assert registry.get_handlers_for_event("reply.fixed") == expected
    assert registry.priority_buckets == {1e100: ["first"], -1e100: ["last"]}
    for waiter in waiters:
        waiter.close()
    # Cleanup must neither alter captured membership nor clone handler objects.
    assert snapshot == expected
    assert all(actual is original for actual, original in zip(snapshot, expected))
    assert registry.get_handlers_for_event("reply.fixed") == expected[2:4]


async def test_exact_wait_does_not_traverse_unrelated_caches_or_listeners(monkeypatch):
    registry = get_handler_registry()
    waiters = [EventWaiter(f"unrelated.{index}") for index in range(128)]
    for index in range(128):
        registry.get_handlers_for_event(f"unrelated.{index}")

    class NoTraversal(OrderedDict):
        def __iter__(self):
            pytest.fail("Exact waiting must not traverse cached event names")

    monkeypatch.setattr(registry, "cached_chain", NoTraversal(registry.cached_chain))
    cache = registry._waiter_cache
    monkeypatch.setattr(cache, "_chains", NoTraversal(cache._chains))
    try:
        with (
            patch("apixis.core.event.handler_registry.registry.fnmatchcase", side_effect=AssertionError),
            patch("apixis.core.event.handler_registry.waiter_cache.fnmatchcase", side_effect=AssertionError),
            patch("apixis.core.event.handler_registry.matching.fnmatchcase", side_effect=AssertionError),
        ):
            waiter = EventWaiter("reply.fixed")
            assert registry.get_handlers_for_event("reply.fixed") == list(registry._waiter_cache.waiters[waiter].values())
            waiter.close()
            assert registry.get_handlers_for_event("reply.fixed") == []
    finally:
        for waiter in waiters:
            waiter.close()


async def test_glob_wait_cache_targets_matches_and_snapshots_filter_input():
    registry = get_handler_registry()
    keeper = EventWaiter("unrelated")
    for name in ("reply.fixed", "reply.excluded", "unrelated"):
        registry.get_handlers_for_event(name)
    cache = registry._waiter_cache
    normal = dict(registry.cached_chain)
    excluded = cache._chains["reply.excluded"]
    filters = ["reply.excluded"]
    waiter = EventWaiter("reply.*", filter=filters)
    filters.clear()
    try:
        assert cache._chains["reply.excluded"] is excluded
        assert registry.get_handlers_for_event("reply.fixed") == list(cache.waiters[waiter].values())
        assert registry.get_handlers_for_event("reply.excluded") == []
        assert registry.get_handlers_for_event("Reply.fixed") == []
        waiter.close()
        assert registry.get_handlers_for_event("reply.fixed") == []
        assert cache._chains["reply.excluded"] is excluded
        assert all(registry.cached_chain[name] is chain for name, chain in normal.items())
    finally:
        waiter.close()
        keeper.close()


@pytest.mark.parametrize("removal", ["group", "clear"])
async def test_wait_cache_is_bounded_and_removal_preserves_captured_handlers(monkeypatch, removal):
    registry = ApixHandlerRegistry(ApixEventRegistry(), cache_size=2)
    monkeypatch.setattr("apixis.core.event.subscription.waiter.get_handler_registry", lambda: registry)
    waiter = EventWaiter("reply.*")
    try:
        captured = registry.get_handlers_for_event("reply.first")
        owned = list(registry._waiter_cache.waiters[waiter].values())
        registry.get_handlers_for_event("reply.second")
        registry.get_handlers_for_event("reply.first")
        registry.get_handlers_for_event("reply.third")
        assert list(registry._waiter_cache._chains) == ["reply.first", "reply.third"]
        assert len(registry.cached_chain) == 2
        if removal == "group":
            registry.unregister_group("global")
        else:
            registry.clear()
        assert not registry.registry
        assert not registry._waiter_cache.positions
        assert not registry._waiter_cache._chains
        assert captured == owned
        assert registry._waiter_cache.waiters[waiter] == {}
        assert waiter.active
        await registry.cancel_waiters()
        assert await waiter.wait() is None
        assert waiter not in registry._waiter_cache.waiters
        registry.clear()
        assert not registry._waiter_cache._exact
        assert not registry._waiter_cache._glob
        assert registry.get_handlers_for_event("reply.first") == []
    finally:
        waiter.close()




async def test_waiter_cleanup_preserves_same_name_replacement():
    registry = get_handler_registry()
    waiter = EventWaiter("reply")
    original = next(iter(registry._waiter_cache.waiters[waiter].values()))

    async def noop(event):
        pass

    replacement = ApixEventHandler(noop, name=original.name)
    replacement.priority = 1
    replacement.subscribe = ["reply"]
    try:
        registry.register_handler(replacement, exist_ok=True)
        assert registry._waiter_cache.waiters[waiter] == {}
        waiter.close()
        assert registry.get_handler(replacement.name) is replacement
        assert registry.get_handlers_for_event("reply") == [replacement]
        assert not registry._waiter_cache.waiters
        assert not registry._waiter_cache._owners
    finally:
        waiter.close()


async def test_batch_waiter_registration_collision_rolls_back_only_its_own_entries(monkeypatch):
    registry = get_handler_registry()

    async def noop(event):
        pass

    existing = ApixEventHandler(noop, name="existing")
    existing.priority = 1
    existing.subscribe = ["reply"]
    registry.register_handler(existing)
    normal = registry.get_subscription_chain_for_event("reply")
    keeper = EventWaiter("keeper")
    retained = dict(registry.registry)
    partial = ApixEventHandler(noop, name="partial")
    partial.subscribe = ["first"]
    collision = ApixEventHandler(noop, name="existing")
    collision.subscribe = ["second"]
    monkeypatch.setattr(EventWaiter, "_build_handlers", lambda self, filter: [partial, collision])
    try:
        with pytest.raises(EventHandlerAlreadyRegisteredError):
            EventWaiter(["first", "second"])
        assert registry.registry == retained
        assert list(registry._waiter_cache.waiters) == [keeper]
        assert registry.get_subscription_chain_for_event("reply") is normal
        assert registry.get_handlers_for_event("first") == []
        assert set(registry._waiter_cache._owners) == set(registry._waiter_cache.waiters[keeper])
    finally:
        keeper.close()
