"""Console logging contracts: module functions, streams and exception details."""

import asyncio
import importlib
import re
from types import ModuleType

import pytest

import apixis
from apixis import logger
from apixis.core.config import core_config


@pytest.fixture(autouse=True)
def enable_logging(monkeypatch):
    monkeypatch.setattr(core_config, "LOG_LEVEL", "DEBUG")


def test_public_logger_is_the_module_with_no_legacy_class_or_lifecycle():
    assert isinstance(logger, ModuleType)
    assert logger is importlib.import_module("apixis.core.utils.logger")
    assert not hasattr(apixis, "Logger")
    for name in ("Logger", "LOG_LEVELS", "register_log_level", "start", "stop", "flush", "write_log"):
        assert not hasattr(logger, name)


@pytest.mark.parametrize("name, level, color, stream", [
    ("debug", "DEBUG", "96", "out"),
    ("info", "INFO", "37", "out"),
    ("success", "SUCCESS", "92", "out"),
    ("warning", "WARNING", "93", "out"),
    ("error", "ERROR", "91", "err"),
])
def test_colored_output_includes_timestamp_level_and_formatted_message(
    capsys, name, level, color, stream,
):
    getattr(logger, name)("job %s took %d ms", "ready", 3)
    captured = capsys.readouterr()
    output = getattr(captured, stream)
    assert re.fullmatch(
        rf"\x1b\[{color}m\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}"
        rf" \[APIXIS\] \[{level}\] job ready took 3 ms\x1b\[0m\n",
        output,
    )
    assert not getattr(captured, "err" if stream == "out" else "out")


def test_message_can_be_any_object_without_percent_interpolation(capsys):
    logger.info({"count": 1})
    logger.info("100% complete")
    output = capsys.readouterr().out
    assert "{'count': 1}" in output
    assert "100% complete" in output


def test_exception_prints_active_traceback_to_stderr(capsys):
    try:
        raise ValueError("invalid node")
    except ValueError:
        logger.exception("Cannot run %s", "graph")
    captured = capsys.readouterr()
    assert not captured.out
    assert "[ERROR] Cannot run graph" in captured.err
    assert "Traceback (most recent call last)" in captured.err
    assert "ValueError: invalid node" in captured.err


def test_exception_without_active_error_only_prints_message(capsys):
    logger.exception("Failed")
    captured = capsys.readouterr()
    assert "[ERROR] Failed" in captured.err
    assert "NoneType: None" not in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.parametrize("threshold, expected", [
    ("DEBUG", ["DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR"]),
    ("INFO", ["INFO", "SUCCESS", "WARNING", "ERROR"]),
    ("SUCCESS", ["SUCCESS", "WARNING", "ERROR"]),
    ("WARNING", ["WARNING", "ERROR"]),
    ("ERROR", ["ERROR"]),
    ("NONE", []),
])
def test_log_level_filters_all_message_functions(monkeypatch, capsys, threshold, expected):
    monkeypatch.setattr(core_config, "LOG_LEVEL", threshold)
    for name in ("debug", "info", "success", "warning", "error"):
        getattr(logger, name)("message")
    captured = capsys.readouterr()
    assert re.findall(r"\[APIXIS\] \[(\w+)\]", captured.out + captured.err) == expected


def test_none_suppresses_errors_tracebacks_and_message_formatting(monkeypatch, capsys):
    monkeypatch.setattr(core_config, "LOG_LEVEL", "NONE")

    class Unformattable:
        def __str__(self):
            raise AssertionError("Filtered messages must not be formatted")

    try:
        raise ValueError("hidden error")
    except ValueError:
        for name in ("debug", "info", "success", "warning", "error", "exception"):
            getattr(logger, name)(Unformattable(), "unused argument")
    captured = capsys.readouterr()
    assert not captured.out
    assert not captured.err


def test_filtered_messages_skip_percent_interpolation(monkeypatch, capsys):
    monkeypatch.setattr(core_config, "LOG_LEVEL", "ERROR")
    logger.debug("%d", "not a number")
    assert not capsys.readouterr().out


@pytest.mark.parametrize("show_dispatch", [False, True])
async def test_dispatch_output_is_controlled_separately_from_log_level(
    monkeypatch, capsys, cleanup_event_runtime, wait_for_dispatch, show_dispatch,
):
    from apixis import EventType
    from apixis.core.event import event_loop

    monkeypatch.setattr(core_config, "LOG_LEVEL", "NONE")
    monkeypatch.setattr(event_loop, "SHOW_EVENT_DISPATCH", show_dispatch)
    await cleanup_event_runtime.event_pipe.post_event(
        event_type=EventType.INFO, event_name="logging.dispatch",
    )
    await wait_for_dispatch(cleanup_event_runtime.event_loop)
    captured = capsys.readouterr()
    assert ("[EVENT LOOP]" in captured.out) is show_dispatch
    assert ("logging.dispatch" in captured.out) is show_dispatch
    assert "[APIXIS]" not in captured.out
    assert not captured.err


async def test_repeated_logging_needs_no_startup_and_creates_no_tasks_or_files(
    monkeypatch, tmp_path, capsys,
):
    monkeypatch.chdir(tmp_path)
    tasks = asyncio.all_tasks()
    for index in range(3):
        logger.info("record %d", index)
    assert asyncio.all_tasks() == tasks
    assert not list(tmp_path.iterdir())
    output = capsys.readouterr().out
    assert output.count("[INFO]") == 3
    assert output.index("record 0") < output.index("record 1") < output.index("record 2")
