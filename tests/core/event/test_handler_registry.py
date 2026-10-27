"""Tests for the current, glob-aware event handler registry."""

import time
import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock, patch

import pytest

from apixis.core.event.base import (
    ApixEvent, ApixEventHandler, EventType, handler_chain_context,
)
from apixis.core.event.factory import get_event_registry
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event.factory import get_handler_registry
from apixis.core.event.subscription import (
    unsubscribe,
    get_unmatched_subscriptions,
    subscribe,
)
from apixis.core.utils.exception import (
    EventHandlerAlreadyRegisteredError,
    EventHandlerNotRegisteredError,
)
from apixis.core.config.core_config import EVENT_LOOP_BACKPRESSURE


def make_entry(
    name: str,
    *,
    subscribe_patterns: list[str] | None = None,
    filter_event: list[str] | None = None,
    priority: float | None = 1,
    between_handlers: tuple[str | None, str | None] | None = None,
    callback=None,
) -> ApixEventHandler:
    """Create one valid handler registry entry."""
    entry = ApixEventHandler(AsyncMock() if callback is None else callback)
    entry.name = name
    entry._register_order = 0
    entry.subscribe = ["event.*"] if subscribe_patterns is None else subscribe_patterns
    entry.filter_event = [] if filter_event is None else filter_event
    entry.priority = priority
    entry.between_handlers = between_handlers
    return entry


def observe_events(*event_names: str) -> None:
    """Record exact event names without publishing queue items."""
    for event_name in event_names:
        get_event_registry().record_event(
            ApixEvent(
                event_id=f"event-{event_name}",
                event_type=EventType.WORKFLOW,
                event_name=event_name,
                context=None,
                timestamp=time.time(),
            )
        )


def test_factory_reuses_registry_but_constructor_is_independent():
    assert get_handler_registry() is get_handler_registry()
    assert ApixHandlerRegistry(get_event_registry()) is not get_handler_registry()


async def test_instance_registration_routes_patterns_and_unregisters_all_subscriptions(runtime, wait_for_dispatch):
    """Instance methods participate in the same public event dispatch lifecycle."""
    pipe, loop = runtime
    core = AsyncMock()
    handler = make_entry("instance", callback=core)
    assert handler.register("Build.*", "ready", "Build.*", filter_event=["Build.private.*"]) is handler
    assert get_handler_registry().get_handler(handler.name) is handler
    assert handler.subscribe == ["Build.*", "ready"]

    for name in ("Build.done", "ready", "Build.private.done", "build.done"):
        await pipe.post_event(event_type=EventType.INFO, event_name=name)
    await wait_for_dispatch(loop)
    assert sorted(call.args[0].event_name for call in core.await_args_list) == ["Build.done", "ready"]

    assert handler.unregister() is None
    assert get_handler_registry().get_handler(handler.name) is None
    for name in ("Build.done", "ready"):
        await pipe.post_event(event_type=EventType.INFO, event_name=name)
    await wait_for_dispatch(loop)
    assert core.await_count == 2
    handler.unregister()
    with pytest.raises(EventHandlerNotRegisteredError):
        handler.unregister(missing_ok=False)


def test_instance_reregistration_replaces_patterns_filters_and_ordering():
    first = make_entry("first").register("event.*", priority=10)
    last = make_entry("last").register("event.*", priority=1)
    middle = make_entry("middle").register(
        "event.*", between_handlers=(first.name, last.name), filter_event=["event.skip"],
    )
    assert get_handler_registry().get_handlers_chain_for_event("event.one") == ["first", "middle", "last"]
    assert get_handler_registry().get_handlers_chain_for_event("event.skip") == ["first", "last"]
    middle.register("other.*")
    assert middle.priority == 1
    assert middle.between_handlers is None
    assert middle.filter_event == []
    assert get_handler_registry().get_handlers_chain_for_event("event.one") == ["first", "last"]
    assert get_handler_registry().get_handlers_chain_for_event("other.one") == ["middle"]


@pytest.mark.parametrize("patterns, options, error", [
    ((), {}, ValueError),
    (("new.*",), {"priority": float("nan")}, ValueError),
    (("new.*",), {"between_handlers": ("missing", None)}, EventHandlerNotRegisteredError),
    (("new.*",), {"exist_ok": False}, EventHandlerAlreadyRegisteredError),
])
def test_failed_instance_registration_preserves_existing_settings(patterns, options, error):
    handler = ApixEventHandler(
        AsyncMock(),
        name="instance",
        time_out=5,
    ).register("old.*", priority=10)
    with pytest.raises(error):
        handler.register(*patterns, **options)
    assert get_handler_registry().get_handler(handler.name) is handler
    assert handler.subscribe == ["old.*"]
    assert (handler.priority, handler.time_out, handler.background) == (10, 5, False)
    assert get_handler_registry().get_handlers_chain_for_event("old.one") == ["instance"]
    assert get_handler_registry().get_handlers_chain_for_event("new.one") == []


def test_instance_reregistration_preserves_execution_options():
    """register() changes subscription metadata, not execution behaviour."""
    handler = ApixEventHandler(
        AsyncMock(),
        name="instance",
        stop_when_error=False,
        time_out=2,
        background=True,
    )

    handler.register("old.*", priority=10)
    handler.register("new.*", filter_event=["new.skip"])

    assert handler.stop_when_error is False
    assert handler.time_out == 2
    assert handler.background is True
    assert handler.subscribe == ["new.*"]
    assert handler.filter_event == ["new.skip"]
    assert handler.priority == 1


def test_instance_replacement_and_unregistration_use_handler_name():
    original = make_entry("shared").register("old.*")
    replacement = make_entry("shared")
    with pytest.raises(EventHandlerAlreadyRegisteredError):
        replacement.register("new.*", exist_ok=False)
    assert get_handler_registry().get_handler("shared") is original
    replacement.register("new.*")
    assert get_handler_registry().get_handler("shared") is replacement
    assert get_handler_registry().get_handlers_chain_for_event("old.one") == []
    assert get_handler_registry().get_handlers_chain_for_event("new.one") == ["shared"]
    # Like unsubscribe(), removal targets the name even after replacement.
    original.unregister(missing_ok=False)
    assert get_handler_registry().get_handler("shared") is None
    assert get_handler_registry().get_handlers_chain_for_event("new.one") == []


def test_pattern_normalisation_accepts_one_string():
    assert ApixHandlerRegistry._normalise_patterns(
        "event.one",
        argument_name="events",
    ) == ["event.one"]


def test_empty_chain_is_cached_for_exact_event_name():
    chain = get_handler_registry().get_handlers_chain_for_event("event.one")

    assert chain == []
    assert get_handler_registry().cached_chain == {"event.one": []}
    assert get_handler_registry().get_handlers_chain_for_event("event.one") is chain


def test_glob_matching_is_case_sensitive_and_filters_are_exclusions():
    get_handler_registry().register_handler(
        make_entry(
            "handler",
            subscribe_patterns=["Build.[A-C]*"],
            filter_event=["Build.Bad*"],
        )
    )

    assert get_handler_registry().get_handlers_chain_for_event("Build.App") == [
        "handler"
    ]
    assert get_handler_registry().get_handlers_chain_for_event("Build.BadJob") == []
    assert get_handler_registry().get_handlers_chain_for_event("build.App") == []
    assert set(get_handler_registry().cached_chain) == {
        "Build.App",
        "Build.BadJob",
        "build.App",
    }


def test_unmatched_subscription_query_respects_filters_and_case():
    observe_events("event.one", "event.skip", "Event.Case")
    get_handler_registry().register_handler(
        make_entry(
            "handler",
            subscribe_patterns=["event.*", "other.*", "Event.*", "EVENT.*"],
            filter_event=["event.skip"],
        )
    )

    assert get_handler_registry().get_unmatched_subscriptions("handler") == [
        "other.*",
        "EVENT.*",
    ]
    assert get_unmatched_subscriptions("handler") == [
        "other.*",
        "EVENT.*",
    ]

    with pytest.raises(EventHandlerNotRegisteredError):
        get_unmatched_subscriptions("missing")


def test_wildcard_registration_leaves_observed_event_chains_lazy():
    observe_events("known.one", "known.skip", "other.one")
    get_handler_registry().register_handler(
        make_entry(
            "exact",
            subscribe_patterns=["known.one"],
            priority=10,
        )
    )
    assert get_handler_registry().cached_chain == {}

    get_handler_registry().register_handler(
        make_entry(
            "wildcard",
            subscribe_patterns=["known.*"],
            filter_event=["known.skip"],
            priority=1,
        )
    )

    assert get_handler_registry().cached_chain == {}
    assert get_handler_registry().get_handlers_chain_for_event("known.one") == [
        "exact",
        "wildcard",
    ]
    assert get_handler_registry().cached_chain == {
        "known.one": ["exact", "wildcard"]
    }


def test_priority_buckets_dispatch_higher_first_and_preserve_registration_order():
    for entry in (
        make_entry("low", priority=1),
        make_entry("high_first", priority=10),
        make_entry("high_second", priority=10),
    ):
        get_handler_registry().register_handler(entry)

    assert get_handler_registry().priority_buckets == {
        1: ["low"],
        10: ["high_first", "high_second"],
    }
    assert get_handler_registry().get_handlers_chain_for_event("event.one") == [
        "high_first",
        "high_second",
        "low",
    ]


@pytest.mark.parametrize(
    ("between_handlers", "expected"),
    [
        (("left", None), ["left", "middle", "right"]),
        ((None, "right"), ["left", "middle", "right"]),
        (("left", "right"), ["left", "middle", "right"]),
    ],
)
def test_between_handlers_inserts_at_requested_boundary(
    between_handlers,
    expected,
):
    get_handler_registry().register_handler(make_entry("left", priority=5))
    get_handler_registry().register_handler(make_entry("right", priority=5))
    get_handler_registry().register_handler(
        make_entry(
            "middle",
            priority=None,
            between_handlers=between_handlers,
        )
    )

    assert get_handler_registry().priority_buckets[5] == expected


def test_right_boundary_controls_cross_priority_insertion():
    get_handler_registry().register_handler(make_entry("left", priority=10))
    get_handler_registry().register_handler(make_entry("right", priority=1))
    get_handler_registry().register_handler(
        make_entry(
            "middle",
            priority=None,
            between_handlers=("left", "right"),
        )
    )

    assert get_handler_registry().priority_buckets[1] == ["middle", "right"]
    assert get_handler_registry().get_handlers_chain_for_event("event.one") == [
        "left",
        "middle",
        "right",
    ]


def test_between_handlers_rejects_missing_or_reversed_boundaries():
    get_handler_registry().register_handler(make_entry("left", priority=1))
    get_handler_registry().register_handler(make_entry("right", priority=10))

    with pytest.raises(EventHandlerNotRegisteredError, match="missing"):
        get_handler_registry().register_handler(
            make_entry(
                "unknown_left",
                priority=None,
                between_handlers=("missing", "right"),
            )
        )
    with pytest.raises(EventHandlerNotRegisteredError, match="missing"):
        get_handler_registry().register_handler(
            make_entry(
                "unknown_right",
                priority=None,
                between_handlers=("left", "missing"),
            )
        )
    with pytest.raises(ValueError, match="must be before"):
        get_handler_registry().register_handler(
            make_entry(
                "reversed",
                priority=None,
                between_handlers=("left", "right"),
            )
        )


def test_between_handlers_rejects_reversed_names_in_same_bucket():
    get_handler_registry().register_handler(make_entry("first", priority=1))
    get_handler_registry().register_handler(make_entry("second", priority=1))

    with pytest.raises(ValueError, match="must be before"):
        get_handler_registry().register_handler(
            make_entry(
                "middle",
                priority=None,
                between_handlers=("second", "first"),
            )
        )


def test_register_invalidates_only_matching_exact_event_caches():
    assert get_handler_registry().get_handlers_chain_for_event("event.one") == []
    assert get_handler_registry().get_handlers_chain_for_event("other.one") == []

    get_handler_registry().register_handler(
        make_entry(
            "handler",
            subscribe_patterns=["event.*"],
            filter_event=["event.skip"],
        )
    )

    assert get_handler_registry().cached_chain["other.one"] == []
    assert get_handler_registry().get_handlers_chain_for_event("event.one") == [
        "handler"
    ]


def test_register_rejects_invalid_entries_without_partial_mutation():
    with pytest.raises(TypeError, match="ApixEventHandler"):
        get_handler_registry().register_handler(object())
    with pytest.raises(ValueError, match="name"):
        get_handler_registry().register_handler(make_entry(""))
    with pytest.raises(TypeError, match="callable"):
        get_handler_registry().register_handler(
            make_entry("no_callback", callback=False)
        )
    with pytest.raises(ValueError, match="subscribe"):
        get_handler_registry().register_handler(
            make_entry("no_subscriptions", subscribe_patterns=[])
        )
    with pytest.raises(TypeError, match="priority"):
        get_handler_registry().register_handler(
            make_entry("no_priority", priority=None)
        )

    assert get_handler_registry().registry == {}
    assert get_handler_registry().priority_buckets == {}


def test_register_rejects_duplicate_name_and_priority_with_between():
    entry = make_entry("handler")
    get_handler_registry().register_handler(entry)

    with pytest.raises(EventHandlerAlreadyRegisteredError):
        get_handler_registry().register_handler(make_entry("handler"))
    with pytest.raises(ValueError, match="cannot be set together"):
        get_handler_registry().register_handler(
            make_entry(
                "invalid_between",
                priority=1,
                between_handlers=("handler", None),
            )
        )


@pytest.mark.parametrize(
    "between_handlers",
    [
        (None, None),
        ("same", "same"),
        ("left",),
        ("", None),
    ],
)
def test_direct_registration_rejects_invalid_between_handlers(
    between_handlers,
):
    with pytest.raises(ValueError):
        get_handler_registry().register_handler(
            make_entry(
                "invalid",
                priority=None,
                between_handlers=between_handlers,
            )
        )


def test_direct_registration_rejects_non_finite_priority():
    with pytest.raises(ValueError, match="finite"):
        get_handler_registry().register_handler(
            make_entry("invalid", priority=float("nan"))
        )


@pytest.mark.parametrize("name", ["", None, 1])
def test_get_chain_validates_name(name):
    with pytest.raises(ValueError, match="event_name"):
        get_handler_registry().get_handlers_chain_for_event(name)


def test_unregister_removes_entry_and_preserves_already_resolved_list():
    get_handler_registry().register_handler(make_entry("handler"))
    chain = get_handler_registry().get_handlers_chain_for_event("event.one")
    get_handler_registry().unregister_handler("handler")
    assert get_handler_registry().get_handler("handler") is None
    assert get_handler_registry().priority_buckets == {}
    assert chain == ["handler"]
    assert get_handler_registry().get_handlers_chain_for_event("event.one") == []


def test_unregister_unknown_handler_raises():
    with pytest.raises(EventHandlerNotRegisteredError):
        get_handler_registry().unregister_handler("missing")


def test_global_subscribe_builds_full_handler_metadata():
    @subscribe(
        "event.*",
        "event.*",
        filter_event=["event.skip"],
        priority=2.5,
        stop_when_error=False,
        time_out=0,
        background=True,
    )
    async def handler(event):
        return None

    entry = get_handler_registry().get_handler("handler")
    assert entry is not None
    assert entry.subscribe == ["event.*"]
    assert entry.filter_event == ["event.skip"]
    assert entry.priority == 2.5
    assert entry.stop_when_error is False
    assert entry.time_out is None
    assert entry.background is True


def test_global_subscribe_defaults_priority_and_preserves_decorated_function():
    async def handler(event):
        return None

    decorated = subscribe("event.one")(handler)

    assert decorated is handler
    assert get_handler_registry().get_handler("handler").priority == 1


def test_global_subscribe_replaces_by_handler_name():
    @subscribe("event.one")
    async def handler(event):
        return None

    async def replacement(event):
        return None

    replacement.__name__ = "handler"
    assert subscribe("event.two")(replacement) is replacement
    assert get_handler_registry().get_handler("handler").core_func is replacement

    with pytest.raises(EventHandlerAlreadyRegisteredError):
        subscribe("event.two", exist_ok=False)(replacement)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"between_handlers": (None, None)},
        {"between_handlers": ("same", "same")},
        {"between_handlers": ("left",)},
        {"between_handlers": ["left", "right"]},
        {"between_handlers": ("left", None), "priority": 1},
    ],
)
def test_global_subscribe_rejects_invalid_between_handlers(kwargs):
    with pytest.raises(ValueError):
        subscribe("event.one", **kwargs)(make_entry("handler"))


def test_global_subscribe_rejects_empty_boundary_name():
    with pytest.raises(ValueError, match="Boundary"):
        subscribe("event.one", between_handlers=("", None))(make_entry("handler"))


@pytest.mark.parametrize("event_names", [(), ("",), (None,)])
def test_global_subscribe_rejects_invalid_event_names(event_names):
    with pytest.raises(ValueError):
        subscribe(*event_names)(make_entry("handler"))


def test_global_unsubscribe_supports_missing_ok():
    unsubscribe("missing")
    with pytest.raises(EventHandlerNotRegisteredError):
        unsubscribe("missing", missing_ok=False)


def test_global_unsubscribe_removes_entry():
    @subscribe("event.one")
    async def handler(event):
        return None

    unsubscribe("handler")
    assert get_handler_registry().get_handler("handler") is None
    unsubscribe("handler")


def test_builtin_put_nowait_does_not_resolve_chain():
    get_handler_registry().register_handler(make_entry("handler"))
    pipe = ApixEventPipe()
    event = ApixEvent("event-id", EventType.WORKFLOW, "event.one", None, 0)
    with patch.object(get_handler_registry(), "get_handlers_chain_for_event") as resolve:
        pipe.put_nowait(event)
        resolve.assert_not_called()
    assert get_handler_registry().cached_chain == {}
    assert pipe.get_nowait() is event
    pipe.task_done()


@pytest.mark.asyncio
async def test_dispatch_keeps_captured_handler_after_unsubscribe():
    callback = AsyncMock()
    get_handler_registry().register_handler(make_entry("handler", callback=callback))
    chain = get_handler_registry().get_handlers_for_event("event.one")
    unsubscribe("handler")
    event = ApixEvent("event-id", EventType.WORKFLOW, "event.one", None, 0)
    event_loop = ApixEventLoop(get_handler_registry(), ApixEventPipe(), get_event_registry())
    with patch("apixis.core.event.event_loop.logger") as logger:
        result = await event_loop._dispatch_event(
            event,
            chain,
        )
    assert result is event
    callback.assert_awaited_once_with(event)
    assert not event.has_error
    logger.warning.assert_not_called()
    logger.error.assert_not_called()


@pytest.mark.parametrize("initial", ["missing", "expired", "empty", "populated"])
def test_four_cache_states_rebuild_only_when_required(initial):
    registry = get_handler_registry()
    if initial != "missing":
        registry.cached_chain["event.one"] = {
            "expired": None, "empty": [], "populated": ["cached"],
        }[initial]
    saved = registry.cached_chain.get("event.one")
    with patch.object(registry, "_resolve_handler_chain", return_value=[]) as build:
        chain = registry.get_handlers_chain_for_event("event.one")
        if initial in ("missing", "expired"):
            build.assert_called_once()
            assert chain == []
        else:
            build.assert_not_called()
            assert chain is saved
        assert registry.cached_chain["event.one"] is chain


def test_replacement_invalidates_union_of_old_and_new_matches_with_filters():
    registry = get_handler_registry()
    old = make_entry("handler", subscribe_patterns=["old.*", "shared.*"],
                     filter_event=["shared.new", "shared.neither"])
    registry.register_handler(old)
    names = ["old.one", "new.one", "shared.old", "shared.new", "shared.neither", "unrelated"]
    saved = {name: registry.get_handlers_chain_for_event(name) for name in names}
    new = make_entry("handler", subscribe_patterns=["new.*", "shared.*"],
                     filter_event=["shared.old", "shared.neither"], priority=10)
    registry.register_handler(new, exist_ok=True)
    assert registry.get_handler("handler") is new
    assert registry.priority_buckets == {10: ["handler"]}
    for name in names[4:]:
        assert registry.cached_chain[name] is saved[name]
    assert saved["old.one"] == ["handler"]
    # Check rebuilt routing instead of the internal representation of expiry.
    for name, expected in zip(names[:4], [[], ["handler"], [], ["handler"]]):
        assert registry.get_handlers_chain_for_event(name) == expected


@pytest.mark.parametrize("options", [
    {"priority": float("nan")},
    {"priority": "bad"},
    {"between_handlers": (None, "missing")},
    {"between_handlers": (None, "handler")},
    {"filter_event": [""]},
    {"exist_ok": False},
])
def test_failed_instance_replacement_preserves_registration_and_cache(options):
    registry = get_handler_registry()
    async def core(event):
        pass
    entry = ApixEventHandler(core, on_error=AsyncMock(), background=True)
    entry.name = "handler"
    subscribe("event.*", priority=5)(entry)
    metadata_fields = (
        "subscribe", "filter_event", "priority", "between_handlers",
        "background", "stop_when_error", "time_out",
    )
    old_metadata = {name: deepcopy(getattr(entry, name)) for name in metadata_fields}
    old_on_error = entry.on_error
    chain = registry.get_handlers_chain_for_event("event.one")
    with pytest.raises((ValueError, TypeError, EventHandlerNotRegisteredError,
                        EventHandlerAlreadyRegisteredError)):
        subscribe("other.*", background=False, **options)(entry)
    assert registry.get_handler("handler") is entry
    assert {name: getattr(entry, name) for name in old_metadata} == old_metadata
    assert entry.on_error is old_on_error
    assert registry.priority_buckets == {5: ["handler"]}
    assert registry.cached_chain["event.one"] is chain


@pytest.mark.parametrize(("name", "between", "expected"), [
    ("first", (None, "last"), ["middle", "first", "last"]),
    ("last", ("first", None), ["first", "last", "middle"]),
    ("middle", ("first", "last"), ["first", "middle", "last"]),
    ("first", None, ["middle", "last", "first"]),
])
def test_replacement_repositions_without_duplicate_bucket_records(name, between, expected):
    registry = get_handler_registry()
    for existing in ["first", "middle", "last"]:
        registry.register_handler(make_entry(existing))
    registry.register_handler(make_entry(name, priority=None if between else 1,
                                        between_handlers=between), exist_ok=True)
    assert registry.priority_buckets == {1: expected}
    assert registry.get_handlers_chain_for_event("event.one") == expected


@pytest.fixture
async def runtime(monkeypatch):
    """Use real consumer scheduling with a private pipe and registry state."""
    pipe = ApixEventPipe()
    loop = ApixEventLoop(get_handler_registry(), pipe, get_event_registry())
    await pipe.start()
    await loop.start()
    yield pipe, loop
    await loop.stop()
    tasks = list(loop._dispatch_tasks | loop._background_handler_tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("publish", ["post", "put", "nowait"])
@pytest.mark.parametrize("cached", [False, True])
async def test_publish_does_not_resolve_but_dequeue_uses_latest_registration(runtime, publish, cached, wait_for_dispatch):
    pipe, loop = runtime
    registry = get_handler_registry()
    calls = []
    @subscribe("event.*")
    async def first(event):
        calls.append("first")
    if cached:
        registry.get_handlers_chain_for_event("event.one")
        # Expire an existing populated cache before publishing.
        subscribe("event.*", priority=1)(first)
    await loop.stop()
    with patch.object(registry, "get_handlers_chain_for_event",
                      wraps=registry.get_handlers_chain_for_event) as resolve:
        event = ApixEvent("id", EventType.INFO, "event.one", None, 0)
        if publish == "post":
            await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
        elif publish == "put":
            await pipe.put(event)
        else:
            pipe.put_nowait(event)
        resolve.assert_not_called()
        @subscribe("event.*", priority=10)
        async def late(event):
            calls.append("late")
        await loop.start()
        await wait_for_dispatch(loop)
        resolve.assert_called_once_with("event.one")
    assert calls == ["late", "first"]
    assert registry.cached_chain["event.one"] == ["late", "first"]


async def test_dequeue_resolves_chain_before_dispatch_task_starts(runtime, monkeypatch, wait_for_dispatch):
    pipe, loop = runtime
    registry = get_handler_registry()
    called = []
    @subscribe("event.*")
    async def first(event):
        called.append("first")
    dispatch_started, release = asyncio.Event(), asyncio.Event()
    dispatch = loop._dispatch_event
    async def delayed_dispatch(event, chain):
        assert [handler.name for handler in chain] == registry.cached_chain[event.event_name]
        assert all(handler is registry.registry[handler.name] for handler in chain)
        dispatch_started.set()
        await release.wait()
        return await dispatch(event, chain)
    monkeypatch.setattr(loop, "_dispatch_event", delayed_dispatch)
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await asyncio.wait_for(dispatch_started.wait(), 1)
    @subscribe("event.*", priority=10)
    async def late(event):
        called.append("late")
    release.set()
    await wait_for_dispatch(loop)
    assert called == ["first"]
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await wait_for_dispatch(loop)
    assert called == ["first", "late", "first"]


@pytest.mark.parametrize("change", ["remove", "replace", "reregister"])
@pytest.mark.parametrize(("patterns", "filters", "matches"), [
    (["other.*"], [], False),
    (["event.*"], ["event.[ot]*"], False),
    (["Event.*"], [], False),
    (["event.?ne"], ["event.two"], True),
])
async def test_foreground_dispatch_keeps_captured_target_after_await(runtime, change, patterns, filters, matches, wait_for_dispatch):
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    @subscribe("event.*", priority=10)
    async def first(event):
        entered.set()
        await release.wait()
        calls.append("first-done")
    @subscribe("event.*")
    async def target(event):
        calls.append("old")
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await asyncio.wait_for(entered.wait(), 1)
    if change in ("remove", "reregister"):
        unsubscribe("target")
    if change in ("replace", "reregister"):
        async def replacement(event):
            calls.append("new")
        replacement.__name__ = "target"
        # Registration changes only affect events dequeued after this point.
        subscribe(*patterns, filter_event=filters, priority=100)(replacement)
    unsubscribe("first")
    release.set()
    await wait_for_dispatch(loop)
    assert calls == ["first-done", "old"]
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await wait_for_dispatch(loop)
    assert calls == ["first-done", "old"] + (["new"] if change != "remove" and matches else [])


@pytest.mark.parametrize("change", ["remove", "replace"])
@pytest.mark.parametrize(("patterns", "filters", "matches"), [
    (["other.*"], [], False),
    (["event.*"], ["event.[ot]*"], False),
    (["Event.*"], [], False),
    (["event.?ne"], ["event.two"], True),
])
async def test_background_keeps_captured_target_when_task_starts(runtime, change, patterns, filters, matches, wait_for_dispatch):
    """Registration changes before the new task runs leave its target intact."""
    pipe, loop = runtime
    old, new = AsyncMock(), AsyncMock()
    entry = ApixEventHandler(old, background=True)
    entry.name = "target"
    subscribe("event.*")(entry)
    event = ApixEvent("id", EventType.INFO, "event.one", None, 0)
    # Creation is synchronous; mutate registration before yielding to the task.
    loop._create_background_handler_task(entry, event)
    tasks = tuple(loop._background_handler_tasks)
    old.assert_not_awaited()
    if change == "remove":
        unsubscribe("target")
    else:
        replacement = ApixEventHandler(new, background=True)
        replacement.name = "target"
        subscribe(*patterns, filter_event=filters)(replacement)
    await asyncio.wait_for(asyncio.gather(*tasks), 1)
    old.assert_awaited_once_with(event)
    new.assert_not_awaited()
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await wait_for_dispatch(loop)
    await asyncio.wait_for(asyncio.gather(*loop._background_handler_tasks), 1)
    old.assert_awaited_once_with(event)
    assert new.await_count == (1 if change == "replace" and matches else 0)


@pytest.mark.parametrize("background", [False, True])
async def test_replacing_handler_execution_mode_only_affects_later_events(
    runtime, wait_for_dispatch, background,
):
    """A queued background task must not resolve a new foreground replacement."""
    pipe, loop = runtime
    calls = []

    async def old(event):
        calls.append((event.event_name, "old", handler_chain_context.get() is None))

    async def new(event):
        calls.append((event.event_name, "new", handler_chain_context.get() is None))

    ApixEventHandler(old, name="target", background=background).register("event.*", priority=10)

    # Replace after scheduling background work, or before calling foreground work.
    @subscribe("event.first", priority=5 if background else 20)
    async def replace(event):
        ApixEventHandler(new, name="target", background=not background).register("event.*", priority=10)

    await pipe.post_event(event_type=EventType.INFO, event_name="event.first")
    await wait_for_dispatch(loop)
    await asyncio.wait_for(asyncio.gather(*loop._background_handler_tasks), 1)
    assert calls == [("event.first", "old", background)]

    await pipe.post_event(event_type=EventType.INFO, event_name="event.next")
    await wait_for_dispatch(loop)
    await asyncio.wait_for(asyncio.gather(*loop._background_handler_tasks), 1)
    assert calls == [("event.first", "old", background), ("event.next", "new", not background)]


@pytest.mark.parametrize("change", ["remove", "replace"])
async def test_cancellation_notifies_captured_handlers_after_registration_changes(runtime, change):
    """Cancellation cleanup belongs to the original chain, including its tail."""
    pipe, loop = runtime
    entered = asyncio.Event()
    old_cleanup, new_cleanup = AsyncMock(), AsyncMock()
    old_core, new_core = AsyncMock(), AsyncMock()

    @subscribe("event.*", priority=10)
    async def blocker(event):
        entered.set()
        await asyncio.Event().wait()

    ApixEventHandler(old_core, name="target", on_cancelled=old_cleanup).register("event.*")
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await asyncio.wait_for(entered.wait(), 1)
    if change == "remove":
        unsubscribe("target")
    else:
        ApixEventHandler(new_core, name="target", on_cancelled=new_cleanup).register("event.*")

    task, = loop._dispatch_tasks
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    old_cleanup.assert_awaited_once()
    new_cleanup.assert_not_awaited()
    old_core.assert_not_awaited()
    new_core.assert_not_awaited()


@pytest.mark.parametrize("background", [False, True])
async def test_stop_preserves_started_calls_and_explicit_start_resumes(runtime, background, wait_for_dispatch):
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []
    @subscribe("event.*", background=background)
    async def target(event):
        entered.set()
        await release.wait()
        completed.append(event.event_name)
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await asyncio.wait_for(entered.wait(), 1)
    # Leave an event queued while the consumer is stopped.
    await loop.stop()
    assert not loop._started
    pipe.get_channel().put_nowait(
        ApixEvent("queued", EventType.INFO, "event.two", None, 0)
    )
    unsubscribe("target")
    release.set()
    await asyncio.wait_for(asyncio.gather(*loop._dispatch_tasks,
                                         *loop._background_handler_tasks), 1)
    assert completed == ["event.one"]
    assert pipe.qsize() == 1
    @subscribe("event.*")
    async def current(event):
        completed.append(event.event_name)
    await pipe.post_event(event_type=EventType.INFO, event_name="event.three")
    assert not loop._started
    await loop.start()
    assert loop._started
    await wait_for_dispatch(loop)
    assert completed == ["event.one", "event.two", "event.three"]


@pytest.mark.parametrize("timeout", [0, -1])
def test_replacement_can_disable_existing_instance_timeout(timeout):
    entry = ApixEventHandler(AsyncMock(), time_out=30)
    entry.name = "handler"
    subscribe("event.*")(entry)
    subscribe("event.*", time_out=timeout)(entry)
    assert get_handler_registry().get_handler("handler").time_out is None


async def test_explicit_start_consumes_existing_ready_events(runtime):
    pipe, loop = runtime
    await loop.stop()
    pipe.get_channel().put_nowait(
        ApixEvent("first", EventType.INFO, "event.one", None, 0)
    )
    assert not loop._started
    await asyncio.wait_for(pipe.post_event(event_type=EventType.INFO,
                                          event_name="event.two"), 1)
    assert not loop._started
    await loop.start()
    await asyncio.wait_for(pipe.join(), 1)
    assert loop._started


async def test_reordering_existing_name_only_changes_subsequent_dequeued_order(runtime, wait_for_dispatch):
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    @subscribe("event.*", priority=10)
    async def first(event):
        entered.set()
        await release.wait()
        calls.append("first")
    @subscribe("event.*", priority=5)
    async def second(event):
        calls.append("second")
    @subscribe("event.*", priority=1)
    async def third(event):
        calls.append("third")
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await asyncio.wait_for(entered.wait(), 1)
    subscribe("event.*", priority=20)(third)
    release.set()
    await wait_for_dispatch(loop)
    assert calls == ["first", "second", "third"]
    calls.clear()
    await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
    await wait_for_dispatch(loop)
    assert calls == ["third", "first", "second"]


async def test_chain_resolution_failure_acknowledges_event_and_keeps_consuming(runtime, wait_for_dispatch):
    pipe, loop = runtime
    with patch.object(get_handler_registry(), "get_handlers_chain_for_event",
                      side_effect=[RuntimeError("resolution failed"), []]) as resolve:
        with patch("apixis.core.event.event_loop.logger") as logger:
            await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
            await pipe.post_event(event_type=EventType.INFO, event_name="event.two")
            await wait_for_dispatch(loop)
            assert resolve.call_count == 2
            logger.error.assert_called_once()
    await loop.stop()
    assert loop._event_semaphore._value == EVENT_LOOP_BACKPRESSURE


def test_unregister_missing_ok_preserves_existing_cache():
    registry = get_handler_registry()
    registry.register_handler(make_entry("existing"))
    chain = registry.get_handlers_chain_for_event("event.one")
    registry.unregister_handler("missing", missing_ok=True)
    assert registry.cached_chain["event.one"] is chain
    assert registry.priority_buckets == {1: ["existing"]}


async def test_dispatch_cancelled_before_start_releases_ack_and_capacity(runtime, wait_for_dispatch):
    pipe, loop = runtime
    cancelled = []
    class CancelFirstTask(set):
        def add(self, task):
            super().add(task)
            if not cancelled:
                cancelled.append(task)
                task.cancel()  # Cancel before create_task can enter the coroutine.
    loop._dispatch_tasks = CancelFirstTask()
    await loop.stop()
    loop._event_semaphore = asyncio.BoundedSemaphore(1)
    await loop.start()
    calls = []
    @subscribe("event.*")
    async def handler(event):
        calls.append(event.event_name)
    with patch.object(pipe, "task_done", wraps=pipe.task_done) as acknowledge:
        await pipe.post_event(event_type=EventType.INFO, event_name="event.first")
        await pipe.post_event(event_type=EventType.INFO, event_name="event.second")
        await wait_for_dispatch(loop)
        await loop.stop()
        assert acknowledge.call_count == 2
    assert cancelled[0].cancelled()
    assert calls == ["event.second"]
    assert not loop._dispatch_tasks
    assert loop._event_semaphore._value == 1


@pytest.mark.parametrize("outcome", ["return", "raise", "cancel"])
async def test_started_dispatch_completes_queue_and_capacity_exactly_once(runtime, outcome, wait_for_dispatch):
    pipe, loop = runtime
    entered, release = asyncio.Event(), asyncio.Event()
    await loop.stop()
    loop._event_semaphore = asyncio.BoundedSemaphore(1)
    await loop.start()
    @subscribe("event.*")
    async def handler(event):
        entered.set()
        await release.wait()
        if outcome == "raise":
            raise ValueError("handler failed")
    with patch.object(pipe, "task_done", wraps=pipe.task_done) as acknowledge:
        await pipe.post_event(event_type=EventType.INFO, event_name="event.one")
        await asyncio.wait_for(entered.wait(), 1)
        if outcome == "cancel":
            next(iter(loop._dispatch_tasks)).cancel()
        else:
            release.set()
        await wait_for_dispatch(loop)
        await loop.stop()
        acknowledge.assert_called_once_with()
    assert not loop._dispatch_tasks
    assert loop._event_semaphore._value == 1


@pytest.mark.parametrize("phase", ["before_start", "running"])
async def test_background_cancellation_removes_task_without_using_event_capacity(runtime, phase):
    """Cancelling background work preserves event capacity and removes its task."""
    pipe, loop = runtime
    entered = asyncio.Event()
    cleanup = AsyncMock()
    event = ApixEvent("id", EventType.INFO, "event.one", None, 0)

    async def handler(event):
        entered.set()
        await asyncio.Event().wait()

    ApixEventHandler(handler, background=True, on_cancelled=cleanup).register("event.*")
    initial_capacity = loop._event_semaphore._value
    loop._create_background_handler_task(loop._registry.registry["handler"], event)
    task, = loop._background_handler_tasks
    if phase == "running":
        await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    assert entered.is_set() == (phase == "running")
    assert cleanup.await_count == (1 if phase == "running" else 0)
    assert not loop._background_handler_tasks
    assert loop._event_semaphore._value == initial_capacity
