"""Uniform numeric contracts at public runtime boundaries."""

import importlib

import pytest

from apixis import (
    ApixEventHandler, ApixEventLoop, ApixEventPipe, ApixEventRegistry,
    ApixHandlerRegistry, BuiltinChannel, Node, subscribe,
)
from apixis.core.utils.validation import validate_int, validate_number, validate_timeout


@pytest.mark.parametrize("value", [True, False, 1.5, "1", None, float("nan"), float("inf")])
def test_integer_validation_never_coerces(value):
    with pytest.raises(TypeError):
        validate_int(value, "capacity", minimum=1)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), 10**400])
def test_finite_validation_rejects_nonfinite_and_overflow(value):
    with pytest.raises(ValueError, match="finite"):
        validate_number(value, "budget")


@pytest.mark.parametrize("value", [True, "1", object()])
def test_number_validation_rejects_non_numeric_values(value):
    with pytest.raises(TypeError):
        validate_number(value, "budget")


def test_integer_bound_and_execution_timeout_semantics():
    assert validate_int(1, "capacity", minimum=1) == 1
    with pytest.raises(ValueError):
        validate_int(0, "capacity", minimum=1)
    assert validate_timeout(None) is None
    assert validate_timeout(0) is None
    assert validate_timeout(-1) is None
    assert validate_timeout(0.1) == 0.1


@pytest.mark.parametrize("value", [True, 1.5, "2", float("nan"), float("inf"), -1])
def test_channel_validates_capacity(value):
    with pytest.raises(ValueError if type(value) is int else TypeError):
        BuiltinChannel(maxsize=value)


def test_independent_channel_can_be_unbounded_but_pipe_cannot():
    channel = BuiltinChannel(maxsize=0)
    assert channel.maxsize == 0
    with pytest.raises(ValueError):
        ApixEventPipe(builtin=channel)


@pytest.mark.parametrize("value", [True, 1.5, "2", float("nan"), float("inf"), 0, -1])
def test_dispatch_capacity_is_validated_even_for_direct_construction(monkeypatch, value):
    module = importlib.import_module("apixis.core.event.event_loop")
    monkeypatch.setattr(module, "EVENT_LOOP_BACKPRESSURE", value)
    registry = ApixEventRegistry()
    with pytest.raises(ValueError if type(value) is int else TypeError):
        ApixEventLoop(ApixHandlerRegistry(registry), ApixEventPipe(), registry)


@pytest.mark.parametrize("value, error", [
    (True, TypeError), ("1", TypeError), (float("nan"), ValueError),
    (float("inf"), ValueError), (10**400, ValueError),
])
def test_execution_and_priority_apis_reject_invalid_numbers(value, error):
    async def receive(event):
        pass

    for operation in (
        lambda: ApixEventHandler(receive, time_out=value),
        lambda: Node(lambda state: {}, timeout=value),
        lambda: subscribe("reply", priority=value),
    ):
        with pytest.raises(error):
            operation()


def test_failed_timeout_assignment_preserves_previous_value():
    async def receive(event):
        pass

    handler = ApixEventHandler(receive, time_out=2)
    with pytest.raises(ValueError):
        handler.time_out = float("nan")
    assert handler.time_out == 2
