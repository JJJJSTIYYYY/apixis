"""Shared fixtures and event test utilities for Python 3.12+."""

from uuid import uuid4

from apixis.core.event.base import ApixEvent, EventType, ApixEventHandler

import time

import pytest


# ============================
# Fixtures: Event & Handler
# ============================


@pytest.fixture
def sample_event():
    """Create a basic sample event."""
    return ApixEvent(
        event_id="event-" + uuid4().hex,
        event_type=EventType.WORKFLOW,
        event_name="test.event",
        context={"key": "value"},
        timestamp=time.time(),
        accepted=False,
    )


@pytest.fixture
def sample_event_no_name():
    """Create event with empty name."""
    return ApixEvent(
        event_id="event-" + uuid4().hex,
        event_type=EventType.WORKFLOW,
        event_name="",
        context=None,
        timestamp=time.time(),
        accepted=False,
    )


@pytest.fixture
def make_event():
    """Factory fixture: create an event with custom parameters."""

    def _make(event_name="test.event", event_type=None, context=None):
        return ApixEvent(
            event_id="event-"+uuid4().hex,
            event_type=event_type or EventType.WORKFLOW,
            event_name=event_name,
            context=context,
            timestamp=time.time(),
            accepted=False,
        )

    return _make


@pytest.fixture
def make_handler():
    """Factory fixture: create an async handler callback."""

    def _make(side_effect=None, accept_event=False, return_value=None):
        async def handler(event: ApixEvent):
            if side_effect:
                raise side_effect
            if accept_event:
                event.accept()
            return return_value

        return handler

    return _make


@pytest.fixture
def handler_entry_factory():
    """Factory fixture: create a ApixEventHandler."""

    def _make(
        name="test_handler",
        subscribe=None,
        callback=None,
        priority=1.0,
        register_order=0,
        stop_when_error=True,
        time_out=30.0,
        background=False,
    ):
        if callback is None:

            async def default_handler(event: ApixEvent):
                pass

            callback = default_handler

        entry = ApixEventHandler(
            callback,
            stop_when_error=stop_when_error,
            time_out=time_out,
            background=background,
        )
        entry.id = "handler_id_001"
        entry.name = name
        entry.subscribe = list(subscribe or ["test.event"])
        entry.priority = priority
        entry._register_order = register_order
        return entry


    return _make
