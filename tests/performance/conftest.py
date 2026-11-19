"""Quiet runtime logging during performance tests."""

import importlib
import logging

import pytest

from apixis import Logger


@pytest.fixture(autouse=True)
def quiet_runtime_logs(monkeypatch):
    """Keep console output out of measurements while preserving error logging."""
    loop_module = importlib.import_module("apixis.core.event.event_loop")
    monkeypatch.setattr(loop_module, "SHOW_EVENT_DISPATCH", False)
    application_logger = logging.getLogger("apixis.performance")
    Logger.set_error(application_logger.error)
    Logger.set_exception(application_logger.exception)
    try:
        yield
    finally:
        Logger.set_error(None)
        Logger.set_exception(None)
