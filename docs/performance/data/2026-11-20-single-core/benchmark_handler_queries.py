"""Measure handler snapshot assembly with active wait listeners."""

import argparse
import gc
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--repeats", type=int, default=9)
    args = parser.parse_args()
    sys.path.insert(0, str(args.source.resolve()))
    from apixis import ApixEventHandler, ApixEventRegistry, ApixHandlerRegistry
    import apixis

    assert Path(apixis.__file__).resolve().is_relative_to(args.source.resolve())

    async def noop(event):
        pass

    digest = hashlib.sha256()
    for path in sorted((args.source / "apixis").rglob("*.py")):
        if path.name != "_version.py":
            digest.update(str(path.relative_to(args.source)).encode())
            digest.update(path.read_bytes())
    result = {
        "label": args.label,
        "python": sys.version,
        "gc_enabled": gc.isenabled(),
        "gc_thresholds": gc.get_threshold(),
        "source_sha256_excluding_version": digest.hexdigest(),
        "cellvars": ApixHandlerRegistry.get_handlers_for_event.__code__.co_cellvars,
        "scenarios": {},
    }
    for name, ordinary_count, received_count, processed_count, queries in [
        ("one-ordinary-two-waiters", 1, 1, 1, 1000000),
        ("32-ordinary-32-waiters", 32, 16, 16, 300000),
        ("64-waiters-only", 0, 32, 32, 1000000),
    ]:
        registry = ApixHandlerRegistry(ApixEventRegistry())
        ordinary_handlers = []
        for index in range(ordinary_count):
            handler = ApixEventHandler(noop, name=f"normal.{index}")
            handler.subscribe = ["exact"]
            handler.priority = 1
            registry.register_handler(handler)
            ordinary_handlers.append(handler)
        waiters = {"received": [], "processed": []}
        for point, count in [
            ("received", received_count),
            ("processed", processed_count),
        ]:
            for index in range(count):
                handler = ApixEventHandler(noop, name=f"{point}.{index}")
                handler.subscribe = ["exact"]
                registry.register_wait_handler(handler, point=point)
                waiters[point].append(handler)
        expected = [*waiters["received"], *ordinary_handlers, *waiters["processed"]]
        lookup = registry.get_handlers_for_event
        snapshot = lookup("exact")
        assert snapshot == expected
        cached = registry._waiter_cache.get("exact")
        cache_snapshot = tuple(tuple(handlers) for handlers in cached)
        ordinary_chain = registry.get_subscription_chain_for_event("exact")
        samples = []
        for run in range(args.repeats + 1):
            gc.collect()
            started = time.perf_counter()
            for _ in range(queries):
                assert len(lookup("exact")) == len(expected)
            elapsed = time.perf_counter() - started
            assert tuple(tuple(handlers) for handlers in cached) == cache_snapshot
            assert registry.get_subscription_chain_for_event("exact") is ordinary_chain
            assert lookup("exact") == expected
            if run:
                samples.append(elapsed)
        for point in waiters:
            for handler in waiters[point]:
                registry.unregister_handler(handler.name)
        assert snapshot == expected
        assert all(actual is handler for actual, handler in zip(snapshot, expected))
        assert lookup("exact") == ordinary_handlers
        result["scenarios"][name] = {
            "workload": {
                "ordinary": ordinary_count,
                "received": received_count,
                "processed": processed_count,
                "queries": queries,
            },
            "rounds_seconds": samples,
            "median_ns_per_query": statistics.median(samples) / queries * 1e9,
        }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(args.label, "waiter query benchmark complete", flush=True)


if __name__ == "__main__":
    main()
