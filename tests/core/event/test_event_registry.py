"""Tests for the registry of exact event names observed at runtime."""

import asyncio
import time
from unittest.mock import Mock

import pytest

import apixis.core.event as event_package
from apixis.core.event import ApixEventLoop, ApixHandlerRegistry, ApixEventHandler
from apixis.core.event.base import ApixEvent, EventType
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.factory import get_event_registry


def make_event(event_name: str) -> ApixEvent:
    """Create one event for registry tests."""
    return ApixEvent(
        event_id=f"event-{event_name}",
        event_type=EventType.WORKFLOW,
        event_name=event_name,
        context=None,
        timestamp=time.time(),
    )


def test_event_registry_class_and_factory_getter_are_exported():
    assert event_package.ApixEventRegistry is ApixEventRegistry
    assert event_package.get_event_registry() is get_event_registry()
    assert ApixEventRegistry() is not get_event_registry()


def test_record_event_stores_exact_case_sensitive_names_once():
    get_event_registry().record_event(make_event("Graph.Start"))
    get_event_registry().record_event(make_event("Graph.Start"))
    get_event_registry().record_event(make_event("graph.start"))

    assert get_event_registry().get_registered_events() == frozenset(
        {"Graph.Start", "graph.start"}
    )


def test_get_registered_events_returns_immutable_snapshot():
    get_event_registry().record_event(make_event("event.one"))
    snapshot = get_event_registry().get_registered_events()
    get_event_registry().record_event(make_event("event.two"))

    assert snapshot == frozenset({"event.one"})
    assert get_event_registry().get_registered_events() == frozenset(
        {"event.one", "event.two"}
    )


def test_clear_forgets_observed_names():
    get_event_registry().record_event(make_event("event.one"))

    get_event_registry().clear()

    assert get_event_registry().get_registered_events() == frozenset()


def test_record_event_validates_input():
    with pytest.raises(TypeError, match="ApixEvent"):
        get_event_registry().record_event(object())
    with pytest.raises(ValueError, match="event_name"):
        get_event_registry().record_event(make_event(""))


@pytest.mark.asyncio
async def test_post_event_only_buffers_before_dispatch():
    pipe = ApixEventPipe()

    await pipe.post_event(
        event_type=EventType.INFO,
        event_name="runtime.posted",
    )

    assert get_event_registry().get_registered_events() == frozenset()
    await pipe.get()
    pipe.task_done()


def test_put_nowait_only_buffers_events_and_other_queue_values():
    pipe = ApixEventPipe()

    pipe.put_nowait(make_event("runtime.nowait"))
    pipe.put_nowait("raw-value")

    assert get_event_registry().get_registered_events() == frozenset()
    assert pipe.get_nowait().event_name == "runtime.nowait"
    pipe.task_done()
    assert pipe.get_nowait() == "raw-value"
    pipe.task_done()


@pytest.mark.parametrize("publication", ["post_event", "put", "put_nowait", "builtin"])
@pytest.mark.parametrize("event_type, name", [
    (EventType.INFO, "runtime.observed"),
    (EventType.TRANSIENT, "runtime.transient"),
    (EventType.INFO, ""),
])
async def test_recording_occurs_once_after_dispatch_capacity_is_acquired(
    publication, event_type, name, monkeypatch, wait_for_dispatch,
):
    """A pending event is observed only after it can enter dispatch."""
    registry = ApixEventRegistry()
    handlers = ApixHandlerRegistry(registry)
    pipe = ApixEventPipe()
    loop = ApixEventLoop(handlers, pipe, registry)
    capacity = asyncio.Semaphore(0)
    loop._event_semaphore = capacity
    record = Mock(wraps=registry.record_event)
    monkeypatch.setattr(registry, "record_event", record)
    observed_in_handler = []

    async def receive(event):
        observed_in_handler.append(registry.get_registered_events())

    handler = ApixEventHandler(receive)
    handler.subscribe = ["runtime.*"]
    handler.priority = 1
    handlers.register_handler(handler)
    event = make_event(name)
    event.event_type = event_type
    await pipe.start()
    try:
        if publication == "post_event":
            await pipe.post_event(event_type=event_type, event_name=name)
        elif publication == "put":
            await pipe.put(event)
        elif publication == "put_nowait":
            pipe.put_nowait(event)
        elif publication == "builtin":
            await pipe.get_channel().put(event)
        assert registry.get_registered_events() == frozenset()
        record.assert_not_called()
        await loop.start()
        # The consumer may dequeue one event before dispatch capacity is available.
        await asyncio.sleep(0)
        assert pipe.empty()
        expected = frozenset({name}) if name and event_type != EventType.TRANSIENT else frozenset()
        assert registry.get_registered_events() == frozenset()
        record.assert_not_called()
        assert observed_in_handler == []
        capacity.release()
        await wait_for_dispatch(loop)
        assert registry.get_registered_events() == expected
        assert record.call_count == (1 if name else 0)
        assert observed_in_handler == ([expected] if name else [])
    finally:
        await loop.stop()
        await pipe.stop()


