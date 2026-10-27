"""Performance settings, quiet logging, and reproducible metric output."""

import importlib
import json
import os
import platform
from datetime import datetime, timezone
from math import fsum
from pathlib import Path
from statistics import median
from time import perf_counter

import pytest

from apixis.core.config.core_config import EVENT_LOOP_BACKPRESSURE, EVENT_PIPE_MAX_LEN


@pytest.fixture(params=[
    pytest.param((1000, 16), id="1000-invocations-16-nodes"),
    pytest.param((3000, 32), id="3000-invocations-32-nodes"),
])
def graph_settings(request):
    """Run both standard and stress workloads; allow small CI overrides."""
    count, nodes = request.param
    settings = {
        key: request.config.getoption(f"--graph-{key}")
        for key in ("count", "nodes", "width", "repeats", "timeout")
    }
    settings["count"] = settings["count"] if settings["count"] is not None else count
    settings["nodes"] = settings["nodes"] if settings["nodes"] is not None else nodes
    return settings


@pytest.fixture
def event_runtime_capacity(request):
    """Only the sweep replaces the runtime; other workloads use configuration."""
    return getattr(request, "param", None)


@pytest.fixture
def backpressure_settings(request):
    """Use many short chains to emphasize waiting rather than snapshot growth."""
    settings = {
        key: request.config.getoption(f"--graph-{key}")
        for key in ("count", "nodes", "width", "repeats", "timeout")
    }
    settings["count"] = settings["count"] if settings["count"] is not None else 8192
    settings["nodes"] = settings["nodes"] if settings["nodes"] is not None else 4
    return settings


@pytest.fixture(autouse=True)
def quiet_runtime_logs(monkeypatch):
    """Keep console output out of measurements while preserving error logging."""
    logger_module = importlib.import_module("apixis.core.utils.logger")
    loop_module = importlib.import_module("apixis.core.event.event_loop")
    for name in ("debug", "info", "success", "warning"):
        monkeypatch.setattr(logger_module, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(loop_module, "SHOW_EVENT_DISPATCH", False)


@pytest.fixture
def report_graph_metrics(request, cleanup_event_runtime):
    reports = getattr(request.config, "_graph_metrics", None)
    if reports is None:
        reports = request.config._graph_metrics = []

    def record(metrics):
        reports.append({
            "node_id": request.node.nodeid,
            "event_loop_backpressure": cleanup_event_runtime.event_loop._event_semaphore._bound_value,
            **metrics,
        })

    return record


def _scenario_name(report):
    name = f"{report['ownership']}/{report['topology']}"
    workload = report.get("workload", "immediate")
    return f"{name}/{workload}" if workload != "immediate" else name


def _performance_readme(reports, session_timing):
    """Render cumulative durations, round benchmarks, and scenario definitions."""
    lines = [
        "| 场景 | 图实例 | 并发调用 | 每次节点数 | 事件背压数 | 等待节点/次 | 节点 sleep（毫秒） | 预热轮数 | 测量轮数 | 总耗时（秒） | 单轮耗时中位数（秒） | 单轮建图中位数（秒） | 单轮吞吐量中位数（节点/秒） |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for report in reports:
        rounds = report["rounds"]
        lines.append(
            f"| {_scenario_name(report)} "
            f"| {report['graph_instances']} | {report['invocations']} "
            f"| {report['nodes_per_invocation']} | {report['event_loop_backpressure']} "
            f"| {report.get('sleep_nodes_per_invocation', 0)} "
            f"| {report.get('sleep_seconds', 0.0) * 1000:g} "
            f"| {report['warmup_rounds']} | {report['measured_rounds']} "
            f"| {report['test_seconds']:.3f} "
            f"| {median(r['total_seconds'] for r in rounds):.3f} "
            f"| {median(r['build_seconds'] for r in rounds):.3f} "
            f"| {median(r['nodes_per_second'] for r in rounds):.1f} |"
        )
    for label, seconds in (
        ("其他用例", session_timing["other_test_seconds"]),
        ("会话开销（收集、调度等）", session_timing["overhead_seconds"]),
        ("会话合计", session_timing["total_seconds"]),
    ):
        if label == "其他用例" and not seconds:
            continue
        cells = [label, *(["—"] * 8), f"{seconds:.3f}", *(["—"] * 3)]
        lines.append("| " + " | ".join(cells) + " |")
    lines.extend([
        "",
        "## 场景说明",
        "",
        "场景名称由图实例使用方式和图内节点调度方式组成：",
        "",
        "| 场景 | 含义 |",
        "| --- | --- |",
        "| `independent/chain` | 多个独立图实例同时运行，每张图内部的节点依次执行，形成串行链。 |",
        "| `shared/chain` | 同一个图实例接受多次并发调用，每次调用内部的节点依次执行，形成串行链。 |",
        "| `independent/parallel` | 多个独立图实例同时运行，每张图内部按层执行；同层节点并发执行，当前层完成后再执行下一层。 |",
        "| `shared/parallel` | 同一个图实例接受多次并发调用，每次调用内部按层执行；同层节点并发执行，当前层完成后再执行下一层。 |",
        "| `shared/chain/sleep-1ms` | 同一个图实例接受多次并发调用，图内为串行链；第 1、5、9……个节点执行 `await asyncio.sleep(0.001)`，用于比较不同背压设置下的调度表现。 |",
        "| `shared/chain/sleep-50ms` | 同一个图实例接受多次并发调用，图内为串行链；所有节点执行 `await asyncio.sleep(0.050)`，在同一 pytest 进程内依次比较 128、512、1024 三档背压。 |",
        "",
        "所有场景均同时提交所有调用，每次调用使用独立的 context。`independent` 的图实例数等于并发调用数，`shared` 的图实例数为 1。",
        "",
        "`parallel` 每层最多执行 `--graph-width` 个节点；节点总数不能整除并发宽度时，最后一层执行剩余节点。",
        "",
        "原有四种场景的节点均立即返回；`sleep-1ms` 和 `sleep-50ms` 为等待场景，每次调用的等待节点数和等待时长在表格中列出。",
        "",
        "50 ms 对比默认使用 8,192 次调用、每次 4 个节点；每档背压重建运行时及信号量，容量列读取实际信号量上限。每档预热一轮、测量三轮（可通过参数覆盖），总耗时包含全部轮次、校验及清理。",
        "",
        "三档背压在同一进程中运行，减少分次启动的差异；这不会固定 CPU 核心，操作系统仍可迁移线程，温度和频率变化也可能影响结果。",
    ])
    return "\n".join(lines) + "\n"


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """Persist reports after the suite, independently of terminal output."""
    config = session.config
    reports = getattr(config, "_graph_metrics", [])
    if not reports:
        return
    # The final teardown report is now available. Account for every test so
    # mixed correctness/performance runs do not label other tests as overhead.
    durations = config._graph_test_durations
    for report in reports:
        report["pytest_phase_seconds"] = durations[report["node_id"]]
        report["test_seconds"] = fsum(report["pytest_phase_seconds"].values())
    reported_ids = {report["node_id"] for report in reports}
    performance_seconds = fsum(report["test_seconds"] for report in reports)
    other_seconds = fsum(
        fsum(phases.values()) for node_id, phases in durations.items()
        if node_id not in reported_ids
    )
    session_seconds = perf_counter() - config._graph_session_started
    config._graph_session_timing = session_timing = {
        "total_seconds": session_seconds,
        "performance_test_seconds": performance_seconds,
        "other_test_seconds": other_seconds,
        "overhead_seconds": session_seconds - performance_seconds - other_seconds,
    }
    readme = config.rootpath / "test-results" / "README.md"
    readme.parent.mkdir(parents=True, exist_ok=True)
    readme.write_text(_performance_readme(reports, session_timing), encoding="utf-8")
    destination = config.getoption("--graph-report")
    if destination:
        path = Path(destination).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "process_id": os.getpid(),
            "configured_event_loop_backpressure": EVENT_LOOP_BACKPRESSURE,
            "tested_event_loop_backpressures": sorted({r['event_loop_backpressure'] for r in reports}),
            "event_pipe_max_len": EVENT_PIPE_MAX_LEN,
            "logging": "ERROR; event dispatch logging disabled",
            "node_workload": "Each result records the actual semaphore capacity, waiting node count, and asynchronous sleep duration.",
            "timing": "Fresh graph construction through completion of every invocation; validation and decomposition excluded.",
            "test_timing": "Pytest setup + call + teardown; includes warmup, all measured rounds, validation, and cleanup.",
            "session_timing": "Session start through result aggregation; final report writing and terminal summary are excluded.",
            "session": session_timing,
            "latency": "Completion time measured from the start of graph construction, including queueing.",
            "results": reports,
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def pytest_terminal_summary(terminalreporter, config):
    reports = getattr(config, "_graph_metrics", [])
    if not reports:
        return
    terminalreporter.section("Graph performance (construction included)")
    for report in reports:
        rounds = report["rounds"]
        terminalreporter.write_line(
            f"{_scenario_name(report)}: "
            f"{report['graph_instances']} graphs, {report['invocations']} invocations, "
            f"{report['nodes_per_invocation']} nodes/invocation; "
            f"backpressure={report['event_loop_backpressure']}; "
            f"{report.get('sleep_nodes_per_invocation', 0)} waiting nodes/invocation, "
            f"sleep={report.get('sleep_seconds', 0.0) * 1000:g}ms; "
            f"test total={report['test_seconds']:.3f}s, "
            f"median round={median(r['total_seconds'] for r in rounds):.3f}s, "
            f"median build={median(r['build_seconds'] for r in rounds):.3f}s, "
            f"{median(r['invocations_per_second'] for r in rounds):.1f} invocations/s, "
            f"{median(r['nodes_per_second'] for r in rounds):.1f} nodes/s, "
            f"p95={median(r['latency_p95_ms'] for r in rounds):.1f}ms"
        )
    timing = config._graph_session_timing
    terminalreporter.write_line(
        f"Session total={timing['total_seconds']:.3f}s; "
        f"performance tests={timing['performance_test_seconds']:.3f}s, "
        f"other tests={timing['other_test_seconds']:.3f}s, "
        f"session overhead={timing['overhead_seconds']:.3f}s"
    )
    terminalreporter.write_line(f"Graph results: {config.rootpath / 'test-results' / 'README.md'}")
    destination = config.getoption("--graph-report")
    if destination:
        terminalreporter.write_line(f"Graph metrics: {Path(destination).resolve()}")
