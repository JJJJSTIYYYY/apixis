"""Console logging contracts: module functions, streams and exception details."""

import asyncio
import importlib
import re
from types import ModuleType

import pytest

import apixis
from apixis import logger


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
        rf" \[APIX\] \[{level}\] job ready took 3 ms\x1b\[0m\n",
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
