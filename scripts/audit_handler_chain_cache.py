"""Count chain builds and verify controlled registration/cache boundaries.

Run with the project's Python. A nonzero exit indicates a reproduced defect;
the JSON preserves both the healthy scenarios and the failing observations.
No runtime source is modified.
"""

import argparse
import asyncio
from collections import Counter
import hashlib
import json
from pathlib import Path
import platform

from apixis import (
    Command,
    EventType,
    Node,
    NodeGraph,
    create_core,
    close_core,
    start_core,
    stop_core,
    expect_event,
    get_handler_meta,
)
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event import factory
from apixis.core.event import core as core_module


class CountingRegistry(ApixHandlerRegistry):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lookups = Counter()
        self.builds = Counter()
        self.snapshots = 0

    def get_subscription_chain_for_event(self, event_name):
        self.lookups[event_name] += 1
        return super().get_subscription_chain_for_event(event_name)

    def _resolve_handler_chain(self, event_name):
        self.builds[event_name] += 1
        return super()._resolve_handler_chain(event_name)

    def get_handlers_for_event(self, event_name):
        self.snapshots += 1
        return super().get_handlers_for_event(event_name)

    def metrics(self):
        lookups, builds = sum(self.lookups.values()), sum(self.builds.values())
        return {
            "lookups": lookups, "builds": builds, "hits": lookups - builds,
            "handler_snapshots": self.snapshots, "cached_names": len(self.cached_chain),
            "cache_capacity": self._cache_size,
        }


async def noop(event):
    pass


def handler(name, patterns, *, callback=noop, group="global", filters=()):
    entry = ApixEventHandler(callback, name=name, group=group)
    entry._subscribe = tuple(patterns)
    entry._filter_event = tuple(filters)
    entry._priority = 1
    return entry


def registry_audit(results):
    for empty in (False, True):
        registry = CountingRegistry(ApixEventRegistry())
        if not empty:
            registry.register_handler(handler("exact", ["hot"]))
            registry.register_handler(handler("glob", ["h*"]))
        for _ in range(100000):
            assert registry.get_handlers_chain_for_event("hot") == ([] if empty else ["exact", "glob"])
        assert registry.builds == {"hot": 1}
        results.append({"scenario": "empty-chain" if empty else "warm-exact-and-glob", **registry.metrics()})

    registry = CountingRegistry(ApixEventRegistry())
    registry.register_handler(handler("hot", ["hot"]))
    cached = registry.get_handlers_chain_for_event("hot")
    for index in range(1000):
        name = f"reply.{index}"
        registry.register_handler(handler("temporary", [name]))
        registry.unregister_handler("temporary")
        assert registry.get_handlers_chain_for_event("hot") is cached
    assert registry.builds == {"hot": 1}
    results.append({"scenario": "unrelated-exact-registration-churn", "registration_pairs": 1000, **registry.metrics()})

    registry = CountingRegistry(ApixEventRegistry())
    registry.register_handler(handler("hot", ["hot"], group="retained"))
    registry.register_handler(handler("old", ["other"], group="removed"))
    cached = registry.get_handlers_chain_for_event("hot")
    registry.get_handlers_chain_for_event("other")
    registry.unregister_group("removed")
    assert registry.get_handlers_chain_for_event("hot") is cached
    assert registry.get_handlers_chain_for_event("other") == []
    assert registry.builds == {"hot": 1, "other": 2}
    results.append({"scenario": "indexed-group-removal", **registry.metrics()})

    registry = CountingRegistry(ApixEventRegistry())
    registry.register_handler(handler("worker", ["old"]))
    cached = {name: registry.get_handlers_chain_for_event(name) for name in ("old", "new", "unrelated")}
    registry.register_handler(handler("worker", ["new"]), exist_ok=True)
    assert registry.get_handlers_chain_for_event("old") == []
    assert registry.get_handlers_chain_for_event("new") == ["worker"]
    assert registry.get_handlers_chain_for_event("unrelated") is cached["unrelated"]
    assert registry.builds == {"old": 2, "new": 2, "unrelated": 1}
    results.append({"scenario": "replacement-with-new-instance", **registry.metrics()})

    registry = CountingRegistry(ApixEventRegistry())
    registry.register_handler(handler("hot", ["hot"]))
    cached = registry.get_handlers_chain_for_event("hot")
    for index in range(100):
        registry.register_handler(handler("temporary", ["*"], filters=["hot"]))
        registry.unregister_handler("temporary")
        assert registry.get_handlers_chain_for_event("hot") is cached
    assert registry.builds == {"hot": 1}
    registry.register_handler(handler("temporary", ["*"]))
    assert registry.get_handlers_chain_for_event("hot") == ["hot", "temporary"]
    registry.unregister_handler("temporary")
    assert registry.get_handlers_chain_for_event("hot") == ["hot"]
    assert registry.builds["hot"] == 3
    results.append({"scenario": "glob-filtered-and-matching-invalidation", **registry.metrics()})

    registry = CountingRegistry(ApixEventRegistry())
    for index in range(50000):
        registry.get_handlers_chain_for_event(f"unique.{index}")
        registry.get_handlers_chain_for_event("hot")
    assert registry.builds["hot"] == 1
    assert len(registry.cached_chain) == 8192
    results.append({"scenario": "hot-key-with-50000-distinct-names", "hot_builds": registry.builds["hot"], **registry.metrics()})

    registry = CountingRegistry(ApixEventRegistry())
    for _ in range(2):
        for index in range(8193):
            registry.get_handlers_chain_for_event(f"cyclic.{index}")
    assert sum(registry.builds.values()) == 16386
    results.append({"scenario": "cyclic-working-set-exceeds-lru", **registry.metrics()})


async def drain(core):
    async with asyncio.timeout(30):
        while True:
            await core._event_pipe.join()
            tasks = tuple(core._event_loop._dispatch_tasks)
            if not tasks:
                return
            await asyncio.gather(*tasks)
            await asyncio.sleep(0)


async def fresh_runtime():
    if factory._core is not None:
        await drain(factory._core)
        await close_core()
    create_core()
    await start_core()
    return factory._core


async def runtime_audit(results):
    core_module.ApixHandlerRegistry = CountingRegistry
    core = await fresh_runtime()
    received = Counter()

    async def receive(event):
        received[event.event_name] += 1

    core._handler_registry.register_handler(handler("receiver", ["runtime.*"], callback=receive))
    for index in range(32768):
        await core._event_pipe.post_event(event_type=EventType.TRANSIENT, event_name=f"runtime.{index % 32}")
    await drain(core)
    assert sum(received.values()) == 32768
    assert len(core._handler_registry.builds) == 32
    assert set(core._handler_registry.builds.values()) == {1}
    results.append({"scenario": "real-dispatch-32768-events-32-names", **core._handler_registry.metrics()})

    original = core._handler_registry.cached_chain["runtime.0"]
    await stop_core()
    await start_core()
    await core._event_pipe.post_event(event_type=EventType.TRANSIENT, event_name="runtime.0")
    await drain(core)
    assert core._handler_registry.cached_chain["runtime.0"] is original
    assert core._handler_registry.builds["runtime.0"] == 1
    results.append({"scenario": "stop-restart-retains-chain", **core._handler_registry.metrics()})

    core = await fresh_runtime()
    instance = handler("worker", ["old"]).register("old")
    core._handler_registry.get_handlers_chain_for_event("old")
    instance.register("new")
    assert core._handler_registry.get_handlers_chain_for_event("old") == []
    assert core._handler_registry.get_handlers_chain_for_event("new") == ["worker"]
    results.append({"scenario": "public-instance-reregistration", **core._handler_registry.metrics()})

    core = await fresh_runtime()
    nodes = {}
    for index in range(16):
        def work(state, next_node=f"node.{index + 1}" if index < 15 else None):
            return Command(update={"steps": state["steps"] + 1}, goto=next_node)
        nodes[f"node.{index}"] = Node(work, f"node.{index}")
    graph = NodeGraph(nodes, "node.0", max_steps=16)
    try:
        outcomes = await asyncio.gather(*(graph.invoke({"steps": 0}) for _ in range(128)))
        assert outcomes == [{"steps": 16}] * 128
        await drain(core)
        assert core._handler_registry.builds[graph.dispatch_name] == 1
        results.append({"scenario": "shared-graph-128-invocations-16-nodes", "node_executions": 2048,
                        "dispatch_lookups": core._handler_registry.lookups[graph.dispatch_name],
                        **core._handler_registry.metrics()})
    finally:
        graph.decompose()

    core = await fresh_runtime()

    async def respond(event):
        await core._event_pipe.post_event(event_type=EventType.TRANSIENT, event_name="reply.fixed")

    core._handler_registry.register_handler(handler("respond", ["request.fixed"], callback=respond))
    for _ in range(512):
        async with expect_event("reply.fixed", point="processed", time_out=10) as waiter:
            await core._event_pipe.post_event(event_type=EventType.TRANSIENT, event_name="request.fixed")
            await waiter.wait()
    await drain(core)
    assert core._handler_registry.builds == {"request.fixed": 1, "reply.fixed": 1}
    results.append({"scenario": "request-reply-with-transient-listeners", "builds_by_name": dict(core._handler_registry.builds), **core._handler_registry.metrics()})
    await stop_core()


def registration_findings():
    """Verify controlled updates and metadata/input alias isolation."""
    findings = []
    registry = factory.get_handler_registry()
    for kind in ("subscriptions", "filters"):
        registry.clear()
        entry = ApixEventHandler(noop, name="worker").register("old")
        assert registry.get_handlers_chain_for_event("old") == ["worker"]
        if kind == "subscriptions":
            entry.register("new")
        else:
            filters = ["old"]
            entry.register("old", filter_event=filters)
            filters.clear()
        cached = registry.get_handlers_chain_for_event("old").copy()
        registry.cached_chain.pop("old")
        rebuilt = registry.get_handlers_chain_for_event("old").copy()
        metadata = get_handler_meta("worker")
        metadata["subscribe"].append("old")
        metadata["filter_event"].clear()
        isolated = registry.get_handlers_chain_for_event("old") == []
        entry.unregister()
        unregistered = registry.get_handlers_for_event("old") == []
        findings.append({
            "scenario": f"public-{kind}-reregistration",
            "expected_chain": [], "cached_chain": cached,
            "freshly_rebuilt_chain": rebuilt,
            "metadata_isolated": isolated, "unregistration_clean": unregistered,
            "defect_reproduced": cached != [] or rebuilt != [] or not isolated or not unregistered,
        })
    return findings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = []
    registry_audit(results)
    asyncio.run(runtime_audit(results))
    findings = registration_findings()
    digest = hashlib.sha256()
    source = Path(__file__).resolve().parents[1] / "src"
    for path in sorted((source / "apixis").rglob("*.py")):
        if path.name != "_version.py":
            digest.update(str(path.relative_to(source)).encode())
            digest.update(path.read_bytes())
    report = {"python": platform.python_version(), "source_sha256_excluding_version": digest.hexdigest(),
              "scenarios": results, "findings": findings}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return int(any(row["defect_reproduced"] for row in findings))


if __name__ == "__main__":
    raise SystemExit(main())
