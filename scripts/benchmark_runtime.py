"""Compare runtime revisions in isolated processes using alternating ABBA runs.

Run with the project's Python. The baseline directory must contain an untouched
source tree, for example one extracted with ``git archive``. No network is used.
"""

import argparse
import asyncio
import gc
import hashlib
import importlib
import json
import math
import platform
from pathlib import Path
import statistics
import subprocess
import sys
import time
import tracemalloc
from typing import Annotated, TypedDict


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--count", type=int, default=512)
    parser.add_argument("--nodes", type=int, default=16)
    parser.add_argument("--worker-source", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.count, args.nodes, args.repeats) <= 0:
        parser.error("count, nodes and repeats must be positive")
    return args


async def run_worker(args):
    sys.path.insert(0, str(args.worker_source.resolve()))
    from apixis.core import AutoMerge, Command, Node, NodeGraph
    from apixis.core.event import factory
    from apixis.core.event.base import ApixEventHandler
    from apixis.core.event.event_registry import ApixEventRegistry
    from apixis.core.event.handler_registry import ApixHandlerRegistry

    assert Path(factory.__file__).resolve().is_relative_to(args.worker_source.resolve())
    source_digest = hashlib.sha256()
    for path in sorted((args.worker_source / "apixis").rglob("*.py")):
        if path.name != "_version.py":
            source_digest.update(str(path.relative_to(args.worker_source)).encode())
            source_digest.update(path.read_bytes())
    import inspect

    shared_start = not inspect.signature(factory.start_core).parameters
    bounded_snapshots = "max_snapshots" in inspect.signature(NodeGraph).parameters
    log_module = importlib.import_module("apixis.core.utils.logger")
    loop_module = importlib.import_module("apixis.core.event.event_loop")
    log_module.DEBUG_LEVEL = "ERROR"
    loop_module.SHOW_EVENT_DISPATCH = False

    class WorkloadState(TypedDict):
        run_index: int
        visits: Annotated[list[tuple[int, str]], AutoMerge()]

    async def drain(core):
        async with asyncio.timeout(30):
            while True:
                await core.event_pipe.join()
                tasks = tuple(core.event_loop._dispatch_tasks)
                if not tasks:
                    return
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0)

    async def stop(core):
        await drain(core)
        await core.event_loop.stop()
        await core.event_pipe.stop()
        assert not core.event_loop._dispatch_tasks
        assert core.event_loop._event_semaphore._value == core.event_loop._event_semaphore._bound_value

    async def fresh_core(capacity):
        if factory._core is not None:
            await stop(factory._core)
        loop_module.EVENT_LOOP_BACKPRESSURE = capacity
        factory._core = None
        core = factory._get_core()
        if shared_start:
            await factory.start_core()
        else:
            await factory.start_core(core)
        assert core.event_loop._event_semaphore._bound_value == capacity
        return core

    def make_graph(spec):
        nodes = spec["nodes"]
        width = spec.get("width", 1)
        names = [f"node_{index}" for index in range(nodes)]
        layers = [names[i:i + width] for i in range(0, nodes, width)]

        def make_node(name, target):
            def work(state):
                update = {"visits": [(state["run_index"], name)]} if spec["state"] == "visits" else {"counter": state["counter"] + 1}
                return Command(update=update, goto=target)

            async def sleeping_work(state):
                await asyncio.sleep(spec["sleep"])
                return work(state)

            return Node(sleeping_work if spec.get("sleep") else work, name)

        definitions = {}
        for index, layer in enumerate(layers):
            next_layer = layers[index + 1] if index + 1 < len(layers) else []
            target = next_layer if width > 1 else (next_layer[0] if next_layer else None)
            for name in layer:
                definitions[name] = make_node(name, target)
        return NodeGraph(
            definitions, layers[0] if width > 1 else layers[0][0],
            **({"max_snapshots": len(layers)} if bounded_snapshots else {}),
            max_steps=len(layers), state_schema=WorkloadState if spec["state"] == "visits" else None,
            no_snapshot=spec.get("no_snapshot", False),
        )

    async def graph_round(spec, core):
        graphs, contexts, results, latencies, lags = [], [], [], [], []
        ready = 0
        all_ready, gate = asyncio.Event(), asyncio.Event()
        released = 0.0

        async def invoke(graph, context):
            nonlocal ready
            ready += 1
            if ready == spec["count"]:
                all_ready.set()
            await gate.wait()
            result = await graph.invoke(graph_context=context)
            latencies.append(time.perf_counter() - released)
            results.append((result, context))

        async def monitor():
            while True:
                expected = time.perf_counter() + 0.001
                await asyncio.sleep(0.001)
                lags.append(max(0.0, time.perf_counter() - expected))

        started = time.perf_counter()
        monitor_task = None
        try:
            async with asyncio.timeout(60):
                for _ in range(spec["count"] if spec.get("independent") else 1):
                    graphs.append(make_graph(spec))
                built = time.perf_counter()
                for index in range(spec["count"]):
                    graph = graphs[index] if spec.get("independent") else graphs[0]
                    state = {"run_index": index, "visits": []} if spec["state"] == "visits" else {
                        "counter": 0, "payload": [{"index": i, "text": "message"} for i in range(spec.get("payload_items", 0))],
                    }
                    contexts.append(graph.create_context(state))
                async with asyncio.TaskGroup() as group:
                    for index, context in enumerate(contexts):
                        graph = graphs[index] if spec.get("independent") else graphs[0]
                        group.create_task(invoke(graph, context))
                    await all_ready.wait()
                    monitor_task = asyncio.create_task(monitor())
                    released = time.perf_counter()
                    gate.set()
                finished = time.perf_counter()
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)
            await drain(core)
            for result, context in results:
                assert context.status == "finished"
                assert context.steps == math.ceil(spec["nodes"] / spec.get("width", 1))
                assert len(context.context_snapshot) == (0 if spec.get("no_snapshot") else context.steps)
                if spec["state"] == "visits":
                    index = context.state["run_index"]
                    assert result == {"run_index": index, "visits": [(index, f"node_{n}") for n in range(spec["nodes"])]}
                else:
                    assert result["counter"] == spec["nodes"]
                    assert result["payload"] == [{"index": i, "text": "message"} for i in range(spec.get("payload_items", 0))]
            assert len(results) == spec["count"]
            total = finished - started
            return {
                "seconds": total, "build_seconds": built - started,
                "prepare_seconds": released - built, "execute_seconds": finished - released,
                "nodes_per_second": spec["count"] * spec["nodes"] / total,
                "execute_nodes_per_second": spec["count"] * spec["nodes"] / (finished - released),
                "latency_p50_ms": percentile(latencies, .5) * 1000,
                "latency_p95_ms": percentile(latencies, .95) * 1000,
                "latency_p99_ms": percentile(latencies, .99) * 1000,
                "loop_lag_max_ms": max(lags, default=0) * 1000,
                "loop_lag_p95_ms": percentile(lags, .95) * 1000 if lags else 0,
                "completed": len(results), "node_executions": spec["count"] * spec["nodes"],
            }
        finally:
            if monitor_task is not None:
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)
            for graph in graphs:
                graph.decompose()

    def new_handler(name, pattern):
        async def noop(event):
            pass
        entry = ApixEventHandler(noop, name=name)
        entry.subscribe = [pattern]
        entry.priority = 1
        return entry

    def registry_round(kind):
        registry = ApixHandlerRegistry(ApixEventRegistry())
        if kind == "cold":
            for index in range(512):
                registry.register_handler(new_handler(f"h{index}", f"exact.{index}"))
            start = time.perf_counter()
            for index in range(2000):
                assert registry.get_handlers_for_event(f"new.{index}") == []
            return {"seconds": time.perf_counter() - start, "operations": 2000}
        if kind == "hot":
            registry.register_handler(new_handler("one", "exact"))
            registry.get_handlers_for_event("exact")
            start = time.perf_counter()
            for _ in range(30000):
                assert len(registry.get_handlers_for_event("exact")) == 1
            return {"seconds": time.perf_counter() - start, "operations": 30000}
        for index in range(50000):
            registry.get_handlers_chain_for_event(f"history.{index}")
        samples = []
        for index in range(5):
            entry = new_handler(f"waiter{index}", f"unrelated.{index}")
            started = time.perf_counter()
            registry.register_handler(entry)
            registry.unregister_handler(entry.name)
            samples.append(time.perf_counter() - started)
        return {"seconds": statistics.median(samples), "operations": 1, "retained_names": len(registry.cached_chain)}

    report = {
        "environment": {"python": sys.version, "platform": platform.platform(), "gc_enabled": gc.isenabled(),
                        "source_sha256_excluding_version": source_digest.hexdigest(),
                        "logging": "ERROR", "loop_lag_probe_interval_ms": 1},
        "scenarios": {}, "memory": {},
    }
    for kind in ("cold", "hot", "mutation"):
        registry_round(kind)
        report["scenarios"][f"registry/{kind}"] = {
            "rounds": [registry_round(kind) for _ in range(args.repeats)],
        }

    specs = {
        "graph/shared-chain": {"count": args.count, "nodes": args.nodes, "state": "visits"},
        "graph/independent-chain": {"count": args.count, "nodes": args.nodes, "state": "visits", "independent": True},
        "graph/shared-parallel": {"count": args.count, "nodes": args.nodes, "state": "visits", "width": 4},
        "graph/independent-parallel": {"count": args.count, "nodes": args.nodes, "state": "visits", "width": 4, "independent": True},
        "state/fixed-small": {"count": 256, "nodes": 32, "state": "fixed"},
        "state/large-snapshots": {"count": 32, "nodes": 16, "state": "fixed", "payload_items": 256},
        "state/large-no-snapshots": {"count": 32, "nodes": 16, "state": "fixed", "payload_items": 256, "no_snapshot": True},
    }
    core = await fresh_core(128)
    for name, spec in specs.items():
        await graph_round(spec, core)
        rounds = []
        for _ in range(args.repeats):
            gc.collect()
            rounds.append(await graph_round(spec, core))
        report["scenarios"][name] = {"workload": spec, "capacity": 128, "rounds": rounds}
    for capacity in (128, 512, 1024):
        core = await fresh_core(capacity)
        spec = {"count": 2048, "nodes": 4, "state": "fixed", "sleep": .010}
        await graph_round(spec, core)
        rounds = []
        for _ in range(args.repeats):
            gc.collect()
            rounds.append(await graph_round(spec, core))
        report["scenarios"][f"sleep/capacity-{capacity}"] = {"workload": spec, "capacity": capacity, "rounds": rounds}
    core = await fresh_core(128)
    for name in ("state/large-snapshots", "state/large-no-snapshots"):
        gc.collect()
        tracemalloc.start()
        await graph_round(specs[name], core)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        report["memory"][name] = {"peak_python_MiB": peak / 2**20}
    gc.collect()
    registry = ApixHandlerRegistry(ApixEventRegistry())
    tracemalloc.start()
    for index in range(30000):
        registry.get_handlers_chain_for_event(f"transient.reply.{index:032d}")
    retained, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    report["memory"]["registry/unique-names"] = {"retained_python_MiB": retained / 2**20, "peak_python_MiB": peak / 2**20, "retained_names": len(registry.cached_chain), "queried_names": 30000}
    await stop(core)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def compare(args):
    if args.baseline_source is None:
        raise SystemExit("--baseline-source is required")
    root = Path(__file__).resolve().parents[1]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    workers = []
    for index, label in enumerate(("before", "after", "after", "before")):
        path = args.output.parent / f"{args.output.stem}-{index + 1}-{label}.json"
        source = args.baseline_source if label == "before" else root / "source"
        print(f"Benchmark {index + 1}/4: {label}", flush=True)
        subprocess.run([
            sys.executable, str(Path(__file__).resolve()), "--worker-source", str(source),
            "--output", str(path), "--repeats", str(args.repeats),
            "--count", str(args.count), "--nodes", str(args.nodes),
        ], cwd=root, check=True, timeout=240)
        workers.append({"label": label, "file": path.name, "data": json.loads(path.read_text())})
    comparison = {"order": [worker["label"] for worker in workers], "repeats_per_worker": args.repeats, "summary": {}, "memory": {}, "workers": workers}
    for label in ("before", "after"):
        fingerprints = {worker["data"]["environment"]["source_sha256_excluding_version"] for worker in workers if worker["label"] == label}
        assert len(fingerprints) == 1, f"{label} runtime changed during measurement"
    for name in workers[0]["data"]["scenarios"]:
        summary = {}
        for label in ("before", "after"):
            rounds = [row for worker in workers if worker["label"] == label for row in worker["data"]["scenarios"][name]["rounds"]]
            summary[label] = {key: statistics.median(row[key] for row in rounds) for key in rounds[0]}
            summary[label]["samples"] = len(rounds)
            summary[label]["min_seconds"] = min(row["seconds"] for row in rounds)
            summary[label]["max_seconds"] = max(row["seconds"] for row in rounds)
        summary["elapsed_change_pct"] = (summary["after"]["seconds"] / summary["before"]["seconds"] - 1) * 100
        summary["throughput_change_pct"] = (summary["before"]["seconds"] / summary["after"]["seconds"] - 1) * 100
        comparison["summary"][name] = summary
    for name in workers[0]["data"]["memory"]:
        comparison["memory"][name] = {label: {key: statistics.median(worker["data"]["memory"][name][key] for worker in workers if worker["label"] == label) for key in workers[0]["data"]["memory"][name]} for label in ("before", "after")}
    args.output.write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    print(f"Saved {args.output}", flush=True)


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker_source is not None:
        asyncio.run(run_worker(arguments))
    else:
        compare(arguments)
