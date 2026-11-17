"""Application logger injection and independent event dispatch output."""

import importlib
import logging
import sys
from unittest.mock import Mock

import pytest

from apixis import Logger


LEVELS = ("debug", "info", "success", "warning", "error", "exception")


@pytest.fixture(autouse=True)
def reset_callbacks():
    for level in LEVELS:
        getattr(Logger, f"set_{level}")(None)
    yield
    for level in LEVELS:
        getattr(Logger, f"set_{level}")(None)


def test_public_logger_export():
    module = importlib.import_module("apixis.core.utils.logger")
    assert Logger is module.Logger
    assert module.__all__ == ["Logger"]


def test_unconfigured_levels_skip_output_and_formatting(capsys):
    class Unformattable:
        def __str__(self):
            raise AssertionError("Disabled logs must not be formatted")

    for level in LEVELS:
        assert getattr(Logger, f"_{level}") is None
        getattr(Logger, level)(Unformattable(), "unused", extra={"job": 1})
    captured = capsys.readouterr()
    assert not captured.out
    assert not captured.err


@pytest.mark.parametrize("level", LEVELS)
def test_callbacks_forward_original_arguments_and_are_independent(level):
    callback = Mock()
    message, argument, metadata = object(), object(), {"job": object()}
    getattr(Logger, f"set_{level}")(callback)

    for name in LEVELS:
        getattr(Logger, name)(message, argument, extra=metadata)

    callback.assert_called_once_with(message, argument, extra=metadata)
    assert callback.call_args.args[0] is message
    assert callback.call_args.kwargs["extra"] is metadata


@pytest.mark.parametrize("level", LEVELS)
def test_callbacks_can_be_replaced_and_disabled(level, capsys):
    records = []

    def first(message):
        records.append(("first", message))

    def second(message):
        records.append(("second", message))

    setter = getattr(Logger, f"set_{level}")
    emit = getattr(Logger, level)
    setter(first)
    emit("one")
    setter(second)
    emit("two")
    setter(None)
    emit("three")
    assert records == [("first", "one"), ("second", "two")]
    assert capsys.readouterr() == ("", "")


def test_bound_callback_and_callback_error_propagation():
    records = []
    Logger.set_info(records.append)
    Logger.info("message")
    assert records == ["message"]

    def fail(message):
        raise ValueError(message)

    Logger.set_info(fail)
    with pytest.raises(ValueError, match="logger failed"):
        Logger.info("logger failed")


def test_application_logger_owns_formatting_and_exception_details(caplog):
    application_logger = logging.getLogger("apixis.tests.application")
    Logger.set_exception(application_logger.exception)
    error = ValueError("invalid node")
    try:
        raise error
    except ValueError:
        Logger.exception("Cannot run %s", "graph", extra={"job": 1})
        assert sys.exception() is error

    record, = caplog.records
    assert record.getMessage() == "Cannot run graph"
    assert record.job == 1
    assert record.exc_info[1] is error


@pytest.mark.parametrize("show_dispatch", [False, True])
async def test_dispatch_print_is_independent_of_logger_callbacks(
    monkeypatch, capsys, cleanup_event_runtime, wait_for_dispatch, show_dispatch,
):
    from apixis import EventType
    from apixis.core.event import event_loop

    monkeypatch.setattr(event_loop, "SHOW_EVENT_DISPATCH", show_dispatch)
    await cleanup_event_runtime.event_pipe.post_event(
        event_type=EventType.INFO, event_name="logging.dispatch",
    )
    await wait_for_dispatch(cleanup_event_runtime.event_loop)
    captured = capsys.readouterr()
    assert ("[EVENT LOOP]" in captured.out) is show_dispatch
    assert ("logging.dispatch" in captured.out) is show_dispatch
    assert not captured.err


async def test_runtime_logs_use_the_configured_callback(cleanup_event_runtime):
    callback = Mock()
    Logger.set_info(callback)
    await cleanup_event_runtime.event_pipe.clear()
    callback.assert_called_once_with("Cleaned 0 in event pipe.")
