"""Repository-wide runtime requirements, pytest selection and benchmark options."""

import math
import sys
import sysconfig
from time import perf_counter

import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    """Validate the interpreter and start benchmark accounting before collection."""
    if session.config.getoption("--require-free-threading"):
        if sysconfig.get_config_var("Py_GIL_DISABLED") != 1 or sys._is_gil_enabled():
            raise pytest.UsageError(
                "--require-free-threading needs a free-threaded Python with the GIL disabled."
            )
    if session.config.getoption("--run-performance"):
        session.config._graph_session_started = perf_counter()
        session.config._graph_test_durations = {}


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


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    """Use pytest's phase durations, including setup and final cleanup."""
    report = yield
    if item.config.getoption("--run-performance"):
        phases = item.config._graph_test_durations.setdefault(item.nodeid, {})
        phases[report.when] = report.duration
    return report


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
    group = parser.getgroup("graph performance")
    group.addoption(
        "--run-performance", action="store_true", default=False,
        help="Collect the opt-in graph performance suite.",
    )
    for option, default, help_text in (
        ("--graph-count", None, "Override concurrent invocations in all graph workloads, including the capacity sweep."),
        ("--graph-nodes", None, "Override nodes per invocation in all graph workloads, including the capacity sweep."),
        ("--graph-width", 4, "Maximum nodes per concurrent graph step."),
        ("--graph-repeats", 3, "Measured rounds; every round includes fresh graph construction."),
    ):
        group.addoption(option, type=_positive_int, default=default, help=help_text)
    group.addoption(
        "--graph-timeout", type=_positive_float, default=120.0,
        help="Deadline in seconds for each entire build-and-run round.",
    )
    group.addoption("--graph-report", default=None, help="Write benchmark metrics to a JSON file.")


def _positive_int(value):
    number = int(value)
    if number <= 0:
        raise ValueError("must be a positive integer")
    return number


def _positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("must be a finite positive number")
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
