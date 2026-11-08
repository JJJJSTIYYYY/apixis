"""Count real chain builds and reproduce stale-cache registration boundaries.

Run with the project's Python. A nonzero exit indicates a reproduced defect;
the JSON preserves both the healthy scenarios and the failing observations.
No runtime source is modified.
"""

import argparse
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import platform

from apixis import (
    ApixEventHandler, ApixEventRegistry, ApixHandlerRegistry, Command, EventType,
    Node, NodeGraph, post_event_threadsafe, start_core, stop_core, wait_for_event,
)
from apixis.core.event import factory


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
    entry.subscribe = list(patterns)
    entry.filter_event = list(filters)
    entry.priority = 1
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
            await core.event_pipe.join()
            tasks = tuple(core.event_loop._dispatch_tasks)
            if not tasks:
                return
            await asyncio.gather(*tasks)
            await asyncio.sleep(0)


async def fresh_runtime():
    if factory._core is not None:
        await drain(factory._core)
        await stop_core()
    factory._core = None
    await start_core()
    return factory._core


async def runtime_audit(results):
    factory.ApixHandlerRegistry = CountingRegistry
    core = await fresh_runtime()
    received = Counter()

    async def receive(event):
        received[event.event_name] += 1

    core.handler_registry.register_handler(handler("receiver", ["runtime.*"], callback=receive))
    for index in range(32768):
        await core.event_pipe.post_event(event_type=EventType.TRANSIENT, event_name=f"runtime.{index % 32}")
    await drain(core)
    assert sum(received.values()) == 32768
    assert len(core.handler_registry.builds) == 32
    assert set(core.handler_registry.builds.values()) == {1}
    results.append({"scenario": "real-dispatch-32768-events-32-names", **core.handler_registry.metrics()})

    core = await fresh_runtime()
    received.clear()
    core.handler_registry.register_handler(handler("receiver", ["thread.hot"], callback=receive))

    def publish():
        for _ in range(256):
            post_event_threadsafe(event_type=EventType.TRANSIENT, event_name="thread.hot").result(10)

    def workers():
        with ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(lambda _: publish(), range(32)))

    await asyncio.wait_for(asyncio.to_thread(workers), 30)
    await drain(core)
    assert received == {"thread.hot": 8192}
    assert core.handler_registry.builds == {"thread.hot": 1}
    results.append({"scenario": "thread-publication-32-workers-8192-events", **core.handler_registry.metrics()})

    original = core.handler_registry.cached_chain["thread.hot"]
    await stop_core()
    await start_core()
    await core.event_pipe.post_event(event_type=EventType.TRANSIENT, event_name="thread.hot")
    await drain(core)
    assert core.handler_registry.cached_chain["thread.hot"] is original
    assert core.handler_registry.builds["thread.hot"] == 1
    results.append({"scenario": "stop-restart-retains-chain", **core.handler_registry.metrics()})

    core = await fresh_runtime()
    instance = handler("worker", ["old"]).register("old")
    core.handler_registry.get_handlers_chain_for_event("old")
    instance.register("new")
    assert core.handler_registry.get_handlers_chain_for_event("old") == []
    assert core.handler_registry.get_handlers_chain_for_event("new") == ["worker"]
    results.append({"scenario": "public-instance-reregistration", **core.handler_registry.metrics()})

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
        assert core.handler_registry.builds[graph.dispatch_name] == 1
        results.append({"scenario": "shared-graph-128-invocations-16-nodes", "node_executions": 2048,
                        "dispatch_lookups": core.handler_registry.lookups[graph.dispatch_name],
                        **core.handler_registry.metrics()})
    finally:
        graph.decompose()

    core = await fresh_runtime()

    async def respond(event):
        await core.event_pipe.post_event(event_type=EventType.TRANSIENT, event_name="reply.fixed")

    core.handler_registry.register_handler(handler("respond", ["request.fixed"], callback=respond))
    for _ in range(512):
        async with wait_for_event("reply.fixed", point="processed") as waiter:
            await core.event_pipe.post_event(event_type=EventType.TRANSIENT, event_name="request.fixed")
            await waiter.wait(time_out=10)
    await drain(core)
    assert core.handler_registry.builds == {"request.fixed": 1, "reply.fixed": 1}
    results.append({"scenario": "request-reply-with-transient-listeners", "builds_by_name": dict(core.handler_registry.builds), **core.handler_registry.metrics()})
    await stop_core()


def mutation_findings():
    findings = []
    for kind in ("subscriptions", "filters"):
        registry = CountingRegistry(ApixEventRegistry())
        entry = handler("worker", ["old"])
        registry.register_handler(entry)
        assert registry.get_handlers_chain_for_event("old") == ["worker"]
        if kind == "subscriptions":
            entry.subscribe = ["new"]
        else:
            entry.filter_event = ["old"]
        registry.register_handler(entry, exist_ok=True)
        cached = registry.get_handlers_chain_for_event("old").copy()
        registry.cached_chain.pop("old")
        rebuilt = registry.get_handlers_chain_for_event("old").copy()
        findings.append({
            "scenario": f"in-place-{kind}-before-reregistration",
            "expected_chain": [], "cached_chain": cached, "freshly_rebuilt_chain": rebuilt,
            "defect_reproduced": cached != rebuilt,
        })

    registry = CountingRegistry(ApixEventRegistry())
    entry = handler("worker", ["old"])
    registry.register_handler(entry)
    registry.get_handlers_chain_for_event("old")
    entry.subscribe = ["new"]
    registry.register_handler(entry, exist_ok=True)
    registry.unregister_handler("worker")
    try:
        registry.get_handlers_for_event("old")
    except KeyError as error:
        findings.append({"scenario": "unregister-after-in-place-reregistration", "error": repr(error), "defect_reproduced": True})
    return findings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = []
    registry_audit(results)
    asyncio.run(runtime_audit(results))
    findings = mutation_findings()
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
