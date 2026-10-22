"""Indexed resolution agrees with complete glob matching through mutations."""

from fnmatch import fnmatchcase
from random import Random
from unittest.mock import patch

import pytest

from apixis.core.event.base import ApixEventHandler
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry import ApixHandlerRegistry


async def noop(event):
    pass


def entry(name, patterns, *, filters=(), priority=1, between=None):
    handler = ApixEventHandler(noop, name=name)
    handler.subscribe = list(patterns)
    handler.filter_event = list(filters)
    handler.priority = priority
    handler.between_handlers = between
    return handler


@pytest.mark.parametrize("cache_size", [0, -1, True, False, 1.5, "2", None])
def test_cache_size_requires_positive_integer(cache_size):
    with pytest.raises(ValueError, match="positive integer"):
        ApixHandlerRegistry(ApixEventRegistry(), cache_size=cache_size)


def test_exact_mutations_do_not_match_unrelated_cached_names():
    registry = ApixHandlerRegistry(ApixEventRegistry())
    for name in ["reply.one", "reply.excluded", "reply.other", "unrelated"]:
        registry.get_handlers_chain_for_event(name)
    unaffected = registry.cached_chain["reply.excluded"]
    handler = entry("waiter", ["reply.one", "reply.excluded"], filters=["*.excluded"])
    with patch.object(registry, "_matches_handler", wraps=registry._matches_handler) as match:
        registry.register_handler(handler)
        assert [call.args[1] for call in match.call_args_list] == ["reply.one", "reply.excluded"]
        assert registry.cached_chain["reply.excluded"] is unaffected
        registry.get_handlers_chain_for_event("reply.one")
        match.reset_mock()
        registry.unregister_handler("waiter")
        assert [call.args[1] for call in match.call_args_list] == ["reply.one", "reply.excluded"]
    assert registry.cached_chain["reply.excluded"] is unaffected
    assert registry.get_handlers_chain_for_event("reply.one") == []


def test_exact_resolution_does_not_visit_unrelated_handlers():
    registry = ApixHandlerRegistry(ApixEventRegistry())
    with patch("apixis.core.event.handler_registry.fnmatchcase", side_effect=AssertionError("unexpected glob match")):
        for index in range(100):
            registry.register_handler(entry(f"handler.{index}", [f"event.{index}"]))
        assert registry.get_handlers_chain_for_event("event.99") == ["handler.99"]
        assert registry.get_handlers_chain_for_event("unknown") == []


def test_mixed_exact_and_glob_subscriptions_deduplicate_and_apply_filters():
    registry = ApixHandlerRegistry(ApixEventRegistry())
    registry.register_handler(entry("mixed", ["topic.a", "topic.*", "topic.?"],
                                    filters=["*.skip"], priority=5))
    registry.register_handler(entry("exact", ["topic.a"], priority=10))
    registry.register_handler(entry("glob", ["topic.[ab]"], priority=5))
    registry.register_handler(entry("filtered", ["topic.a"], filters=["topic.a"], priority=20))
    assert registry.get_handlers_chain_for_event("topic.a") == ["exact", "mixed", "glob"]
    assert registry.get_handlers_chain_for_event("topic.b") == ["mixed", "glob"]
    assert registry.get_handlers_chain_for_event("topic.skip") == []
    assert registry.get_handlers_chain_for_event("Topic.a") == []


def test_relative_boundaries_can_themselves_have_no_explicit_priority():
    registry = ApixHandlerRegistry(ApixEventRegistry())
    registry.register_handler(entry("left", ["topic.*"], priority=10))
    registry.register_handler(entry("right", ["topic.a"], priority=2))
    registry.register_handler(entry("middle", ["topic.a"], priority=None, between=("left", "right")))
    registry.register_handler(entry("after_middle", ["topic.*"], priority=None, between=("middle", None)))
    registry.register_handler(entry("before_middle", ["topic.a"], priority=None, between=(None, "middle")))
    assert registry.get_handlers_chain_for_event("topic.a") == [
        "left", "before_middle", "middle", "after_middle", "right",
    ]
    registry.unregister_handler("middle")
    registry.register_handler(entry("appended", ["topic.a"], priority=2))
    assert registry.get_handlers_chain_for_event("topic.a") == [
        "left", "before_middle", "after_middle", "right", "appended",
    ]


def test_lru_eviction_refreshes_hits_including_empty_chains():
    registry = ApixHandlerRegistry(ApixEventRegistry(), cache_size=2)
    handler = entry("handler", ["matched"])
    registry.register_handler(handler)
    original = registry.get_handlers_chain_for_event("matched")
    captured = registry.get_handlers_for_event("matched")
    empty = registry.get_handlers_chain_for_event("empty")
    assert registry.get_handlers_chain_for_event("matched") is original
    registry.get_handlers_chain_for_event("new")
    assert list(registry.cached_chain) == ["matched", "new"]
    assert registry.get_handlers_chain_for_event("empty") == []
    assert registry.cached_chain["empty"] is not empty
    registry.unregister_handler("handler")
    assert original == ["handler"]
    assert captured == [handler]
    assert registry.get_handlers_chain_for_event("matched") == []
    assert len(registry.cached_chain) == 2


def test_clear_releases_indexes_and_preserves_captured_chains():
    observations = ApixEventRegistry()
    registry = ApixHandlerRegistry(observations, cache_size=2)
    handler = entry("handler", ["topic.a", "topic.*"])
    registry.register_handler(handler)
    chain = registry.get_handlers_chain_for_event("topic.a")
    captured = registry.get_handlers_for_event("topic.a")
    registry.clear()
    assert registry.registry == registry.priority_buckets == registry.cached_chain == {}
    assert registry._exact_subscriptions == registry._glob_subscriptions == {}
    assert registry._handler_order == registry._bucket_next_order == {}
    assert registry._handler_exact_names == {}
    assert registry._register_order == 0
    assert chain == ["handler"]
    assert captured == [handler]
    registry.register_handler(entry("new", ["topic.a"]))
    assert registry.get_handlers_chain_for_event("topic.a") == ["new"]


def test_replacement_changes_exact_and_glob_indexes_in_both_directions():
    registry = ApixHandlerRegistry(ApixEventRegistry(), cache_size=2)
    for patterns, filters, expected in [
        (["topic.a"], [], [["handler"], [], []]),
        (["topic.*"], ["topic.a"], [[], ["handler"], []]),
        (["other", "topic.a"], [], [["handler"], [], ["handler"]]),
        (["*"], ["topic.*"], [[], [], ["handler"]]),
    ]:
        registry.register_handler(entry("handler", patterns, filters=filters), exist_ok=True)
        assert [registry.get_handlers_chain_for_event(name) for name in ["topic.a", "topic.b", "other"]] == expected
    registry.unregister_handler("handler")
    assert registry._exact_subscriptions == registry._glob_subscriptions == {}


def test_indexed_chains_match_reference_through_ordering_replacements_and_eviction():
    """Use an independent ordered-list model rather than private index contents."""
    rng = Random(20261022)
    registry = ApixHandlerRegistry(ApixEventRegistry(), cache_size=5)
    model = {}
    buckets = {}
    patterns = ["topic.a", "topic.b", "Topic.a", "topic.*", "topic.?", "topic.[ab]", "*", "literal[", "close]"]
    filters = [[], ["topic.a"], ["*.b"], ["Topic.*"], ["topic.[!a]"]]
    events = ["topic.a", "topic.b", "topic.c", "Topic.a", "other", "literal[", "close]", "topic.a.more"]
    retained = []

    for step in range(300):
        ordered = [name for priority in sorted(buckets, reverse=True) for name in buckets[priority]]
        if ordered and rng.random() < 0.25:
            name = rng.choice(ordered)
            registry.unregister_handler(name)
            del model[name]
            for bucket in buckets.values():
                if name in bucket:
                    bucket.remove(name)
        else:
            name = rng.choice(ordered) if ordered and rng.random() < 0.4 else f"handler.{step}"
            boundaries = [candidate for candidate in ordered if candidate != name]
            between = None
            priority = rng.choice([1, 2, 5])
            if boundaries and rng.random() < 0.5:
                boundary = rng.choice(boundaries)
                between = (None, boundary) if rng.random() < 0.5 else (boundary, None)
                bucket_priority = next(value for value, names in buckets.items() if boundary in names)
            else:
                bucket_priority = priority
            chosen_patterns = rng.sample(patterns, rng.randint(1, 3))
            chosen_filters = rng.choice(filters)
            registry.register_handler(entry(name, chosen_patterns, filters=chosen_filters,
                                            priority=None if between else priority, between=between), exist_ok=True)
            for bucket in buckets.values():
                if name in bucket:
                    bucket.remove(name)
            bucket = buckets.setdefault(bucket_priority, [])
            if between is None:
                bucket.append(name)
            elif between[0] is None:
                bucket.insert(bucket.index(between[1]), name)
            else:
                bucket.insert(bucket.index(between[0]) + 1, name)
            model[name] = (chosen_patterns, chosen_filters)

        for event_name in rng.sample(events, 6):
            expected = [
                name for priority in sorted(buckets, reverse=True) for name in buckets[priority]
                if any(fnmatchcase(event_name, pattern) for pattern in model[name][0])
                and not any(fnmatchcase(event_name, pattern) for pattern in model[name][1])
            ]
            actual = registry.get_handlers_chain_for_event(event_name)
            assert actual == expected, (step, event_name)
            if step % 50 == 0:
                retained.append((actual, expected.copy()))
        assert len(registry.cached_chain) <= 5
        assert all(chain == saved for chain, saved in retained)
