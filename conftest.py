"""Repository-wide runtime requirements, pytest selection and benchmark options."""

import sys
import sysconfig

import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    """Validate the interpreter before collection."""
    if session.config.getoption("--require-free-threading"):
        if sysconfig.get_config_var("Py_GIL_DISABLED") != 1 or sys._is_gil_enabled():
            raise pytest.UsageError(
                "--require-free-threading needs a free-threaded Python with the GIL disabled."
            )


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session):
    """Fail if an extension enabled the GIL after the startup check."""
    if session.config.getoption("--require-free-threading") and sys._is_gil_enabled():
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_sep(
                "=", "GIL enabled during a required free-threaded test run", red=True,
            )


def pytest_addoption(parser):
    threads = parser.getgroup("event threads")
    threads.addoption(
        "--require-free-threading", action="store_true", default=False,
        help="Require a free-threaded build and verify the GIL remains disabled.",
    )
    threads.addoption("--thread-workers", type=_positive_int, default=None, help="Override stress worker threads.")
    threads.addoption("--thread-iterations", type=_positive_int, default=None, help="Override operations per stress worker.")
    threads.addoption("--thread-queue-size", type=_positive_int, default=64, help="Set both thread stress queue capacities.")
    threads.addoption(
        "--thread-report", default=None,
        help="Write cross-thread stress metrics to a JSON file.",
    )
    threads.addoption(
        "--run-performance", action="store_true", default=False,
        help="Collect the opt-in event performance suite.",
    )


def _positive_int(value):
    number = int(value)
    if number <= 0:
        raise ValueError("must be a positive integer")
    return number


def pytest_collection_modifyitems(config, items):
    """Classify existing tests without repeating markers in every module."""
    for item in items:
        parts = item.path.parts
        if "integration" in parts:
            item.add_marker(pytest.mark.integration)
        elif "core" in parts:
            item.add_marker(pytest.mark.unit)
        if "performance" in parts and not config.getoption("--run-performance"):
            item.add_marker(pytest.mark.skip(reason="use --run-performance to run benchmarks"))


def pytest_ignore_collect(collection_path, config):
    if collection_path.name == "performance":
        return not config.getoption("--run-performance")
