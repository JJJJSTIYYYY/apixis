"""Group membership, indexed removal and captured dispatch contracts."""

from functools import partial
from unittest.mock import AsyncMock

import pytest

from apixis import (
    ApixEvent, ApixEventHandler, EventHandlerAlreadyRegisteredError,
    EventHandlerNotRegisteredError, EventType, get_handler_meta,
    get_handler_registry, subscribe, unsubscribe,
)
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.factory import get_event_registry


@pytest.fixture
def registry():
    return get_handler_registry()


def handler(name, *, group="global"):
    return ApixEventHandler(AsyncMock(), name=name, group=group)


def test_default_group_and_metadata(registry):
    instance = handler("instance").register("event")

    @subscribe("event")
    async def decorated(event):
        pass

    assert instance.group == "global"
    assert get_handler_meta("decorated")["group"] == "global"
    meta = get_handler_meta("instance")
    meta["group"] = "other"
    assert instance.group == "global"
    unsubscribe(group="global")
    assert not registry.registry
    assert not registry._group_handlers
    assert not registry._handler_groups


def test_instance_group_is_preserved_and_can_be_overridden(registry):
    instance = handler("worker", group="original")
    assert subscribe("old")(instance) is instance
    assert instance.group == "original"
    instance.register("event")
    assert instance.group == "original"
    subscribe("event", group="decorator")(instance)
    assert instance.group == "decorator"
    instance.register("event", group="updated")
    assert instance.group == "updated"
    assert get_handler_meta("worker")["group"] == "updated"
    for old_group in ("original", "decorator"):
        registry.unregister_group(old_group, missing_ok=True)
        assert registry.get_handler("worker") is instance
    registry.unregister_group("updated")
    assert registry.get_handler("worker") is None


@pytest.mark.parametrize("entrypoint", ["function", "instance", "registry"])
def test_group_removal_is_exact_and_preserves_other_groups(registry, entrypoint):
    first = handler("first", group="plugin.*").register("event", priority=10)
    handler("second", group="plugin.*").register("event", priority=1)
    handler("case", group="Plugin.*").register("event", priority=5)
    handler("literal", group="plugin.worker").register("event", priority=5)
    handler("default").register("event", priority=5)
    if entrypoint == "function":
        unsubscribe(group="plugin.*")
    elif entrypoint == "instance":
        first.unregister(same_group=True)
    else:
        registry.unregister_group("plugin.*")
    assert registry.get_handlers_chain_for_event("event") == ["case", "literal", "default"]
    assert "plugin.*" not in registry._group_handlers


def test_subscribe_function_sets_group():
    @subscribe("event", group="plugin")
    async def decorated(event):
        pass

    assert get_handler_meta(decorated.__name__)["group"] == "plugin"
    unsubscribe(group="plugin")
    assert get_handler_meta(decorated.__name__) is None


@pytest.mark.parametrize("same_instance", [False, True])
def test_replacement_moves_group_without_leaving_stale_members(registry, same_instance):
    original = handler("worker", group="old").register("event")
    handler("peer", group="old").register("event")
    if same_instance:
        # Direct metadata changes take effect when the handler is re-registered.
        original.group = "new"
        original.register("event")
        replacement = original
    else:
        replacement = handler("worker", group="new").register("event")
    unsubscribe(group="old")
    assert registry.get_handler("worker") is replacement
    assert registry.get_handlers_chain_for_event("event") == ["worker"]
    assert registry._handler_groups == {"worker": "new"}
    assert registry._group_handlers == {"new": {"worker"}}
    unsubscribe(group="new")
    assert not registry.registry


@pytest.mark.parametrize("options, error", [
    ({"group": ""}, ValueError),
    ({"group": 1}, TypeError),
    ({"group": "new", "between_handlers": ("missing", None)}, EventHandlerNotRegisteredError),
    ({"group": "new", "exist_ok": False}, EventHandlerAlreadyRegisteredError),
])
def test_failed_replacement_preserves_group_and_cached_chain(registry, options, error):
    instance = handler("worker", group="old").register("event")
    chain = registry.get_handlers_chain_for_event("event")
    with pytest.raises(error):
        instance.register("other", **options)
    assert instance.group == "old"
    assert registry.get_handler("worker") is instance
    assert registry.get_handlers_chain_for_event("event") is chain
    assert registry._group_handlers == {"old": {"worker"}}
    assert registry._handler_groups == {"worker": "old"}
    unsubscribe(group="old")
    assert not registry.registry


def test_single_unregister_and_clear_clean_group_indexes(registry):
    first = handler("first", group="plugin").register("event")
    second = handler("second", group="plugin").register("event")
    first.unregister()
    assert registry._group_handlers == {"plugin": {"second"}}
    second.unregister()
    assert not registry._group_handlers
    assert not registry._handler_groups
    handler("third").register("event")
    registry.clear()
    assert not registry._group_handlers
    assert not registry._handler_groups
    handler("fourth").register("event")
    unsubscribe(group="global")
    assert not registry.registry


@pytest.mark.parametrize("entrypoint", ["function", "instance", "registry"])
def test_missing_group_follows_missing_ok_without_invalidating_cache(registry, entrypoint):
    handler("existing").register("event")
    chain = registry.get_handlers_chain_for_event("event")
    if entrypoint == "function":
        remove = partial(unsubscribe, group="missing")
    elif entrypoint == "instance":
        instance = handler("absent", group="missing")
        remove = partial(instance.unregister, same_group=True)
    else:
        remove = partial(registry.unregister_group, "missing")
    remove(missing_ok=True)
    with pytest.raises(EventHandlerNotRegisteredError, match="missing"):
        remove(missing_ok=False)
    assert registry.get_handlers_chain_for_event("event") is chain


@pytest.mark.parametrize("group, error", [
    ("", ValueError), (None, TypeError), (1, TypeError), ([], TypeError),
])
def test_invalid_group_is_rejected_before_registration(registry, group, error):
    with pytest.raises(error, match="group"):
        handler("invalid", group=group)
    instance = handler("invalid")
    instance.group = group
    instance.subscribe = ["event"]
    instance.priority = 1
    with pytest.raises(error, match="group"):
        registry.register_handler(instance)
    assert not registry.registry
    assert not registry._group_handlers
    with pytest.raises(error, match="group"):
        registry.unregister_group(group, missing_ok=True)


@pytest.mark.parametrize("args, options", [
    ((), {}), (("worker",), {"group": "plugin"}),
])
def test_unsubscribe_requires_exactly_one_selector(args, options):
    with pytest.raises(ValueError, match="exactly one"):
        unsubscribe(*args, **options)


def test_group_removal_never_scans_registry_or_other_groups(registry):
    class NoScanDict(dict):
        def __iter__(self):
            pytest.fail("Unexpected full index scan")

        def items(self):
            pytest.fail("Unexpected full index scan")

        def values(self):
            pytest.fail("Unexpected full index scan")

    for index in range(200):
        handler(f"unrelated.{index}", group=f"other.{index}").register(f"event.{index}")
    handler("target.first", group="target").register("reply")
    handler("target.second", group="target").register("reply")
    unrelated_chain = registry.get_handlers_chain_for_event("event.100")
    registry.get_handlers_chain_for_event("reply")
    attributes = ("registry", "_group_handlers", "_handler_groups")
    for attribute in attributes:
        setattr(registry, attribute, NoScanDict(getattr(registry, attribute)))
    try:
        unsubscribe(group="target")
        assert len(registry.registry) == 200
        assert registry.get_handlers_chain_for_event("reply") == []
        assert registry.get_handlers_chain_for_event("event.100") is unrelated_chain
    finally:
        for attribute in attributes:
            setattr(registry, attribute, getattr(registry, attribute).copy())


async def test_group_removal_preserves_captured_dispatch_and_expires_matching_caches(registry):
    exact = handler("exact", group="plugin").register("event", priority=10)
    glob = handler("glob", group="plugin").register("event*", priority=5)
    other = handler("other").register("event", priority=1)
    chain = registry.get_handlers_chain_for_event("event")
    captured = registry.get_handlers_for_event("event")
    registry.get_handlers_chain_for_event("event.more")
    unsubscribe(group="plugin")
    assert chain == ["exact", "glob", "other"]
    assert captured == [exact, glob, other]
    assert registry.get_handlers_chain_for_event("event") == ["other"]
    assert registry.get_handlers_chain_for_event("event.more") == []
    event = ApixEvent("id", EventType.INFO, "event", None, 0)
    loop = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
    await loop._dispatch_event(event, captured)
    for instance in captured:
        instance.core_func.assert_awaited_once_with(event)
