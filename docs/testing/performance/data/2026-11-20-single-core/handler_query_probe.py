"""Trace registry query costs without changing production source."""

import argparse
import ast
import asyncio
from collections import Counter, OrderedDict
import copy
import dis
import gc
import hashlib
import importlib
import inspect
import json
from pathlib import Path
import statistics
import sys
import textwrap
import time
import __future__


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument(
        "--mode", choices=["history", "ablation", "reply"], required=True
    )
    parser.add_argument("--repeats", type=int, default=7)
    args = parser.parse_args()
    sys.path.insert(0, str(args.source.resolve()))
    import apixis
    from apixis import (
        ApixEventHandler,
        ApixEventRegistry,
        ApixHandlerRegistry,
        EventType,
    )
    from apixis.core.event import factory, subscription

    assert Path(apixis.__file__).resolve().is_relative_to(args.source.resolve())
    logger = importlib.import_module("apixis.core.utils.logger")
    if not hasattr(logger, "Logger"):
        for name in ("debug", "info", "success", "warning"):
            setattr(logger, name, lambda *a, **k: None)

    async def noop(event):
        pass

    def make_registry(cold=False):
        registry = ApixHandlerRegistry(ApixEventRegistry())
        for index in range(512 if cold else 1):
            entry = ApixEventHandler(noop, name=f"h{index}")
            entry.subscribe = [f"exact.{index}" if cold else "exact"]
            entry.priority = 1
            registry.register_handler(entry)
        return registry

    def ordinary(registry):
        return (
            registry.get_subscription_chain_for_event
            if hasattr(registry, "get_subscription_chain_for_event")
            else registry.get_handlers_chain_for_event
        )

    def measure(kind):
        cold = kind.startswith("cold")
        registry = make_registry(cold)
        names_only = kind.endswith("names")
        lookup = ordinary(registry) if names_only else registry.get_handlers_for_event
        queries = 100000 if cold else 1000000
        samples = []
        for run in range(args.repeats + 1):
            registry.cached_chain.clear()
            if not cold:
                lookup("exact")
            gc.collect()
            started = time.perf_counter()
            if cold:
                for index in range(queries):
                    assert lookup(f"new.{index}") == []
            else:
                for _ in range(queries):
                    assert len(lookup("exact")) == 1
            elapsed = time.perf_counter() - started
            if run:
                samples.append(elapsed)
        return {
            "queries": queries,
            "seconds": samples,
            "ns_per_query": statistics.median(samples) / queries * 1e9,
        }

    def code_info(fn):
        code = fn.__code__
        signature = (
            code.co_code.hex(),
            code.co_names,
            code.co_varnames,
            code.co_consts[1:],
            code.co_flags,
        )
        return {
            "cellvars": code.co_cellvars,
            "locals": code.co_nlocals,
            "stacksize": code.co_stacksize,
            "head": [
                (i.opname, i.argrepr) for i in list(dis.get_instructions(fn))[:18]
            ],
            "signature": hashlib.sha256(repr(signature).encode()).hexdigest(),
        }

    def count_paths():
        counts = Counter()

        class CountedLRU(OrderedDict):
            def get(self, key, default=None):
                counts["get"] += 1
                return super().get(key, default)

            def move_to_end(self, key, last=True):
                counts["move_to_end"] += 1
                return super().move_to_end(key, last=last)

            def popitem(self, last=True):
                counts["popitem"] += 1
                return super().popitem(last=last)

        registry = make_registry(True)
        registry.cached_chain = CountedLRU()
        resolver = registry._resolve_handler_chain

        def resolve(name):
            counts["resolve"] += 1
            return resolver(name)

        registry._resolve_handler_chain = resolve
        if hasattr(registry, "_waiter_cache"):
            get_waiters = registry._waiter_cache.get

            def waiters(name):
                counts["waiter_cache_get"] += 1
                return get_waiters(name)

            registry._waiter_cache.get = waiters
        for index in range(100000):
            assert registry.get_handlers_for_event(f"new.{index}") == []
        assert counts["get"] == counts["move_to_end"] == counts["resolve"] == 100000
        assert counts["popitem"] == 100000 - 8192
        assert counts["waiter_cache_get"] == 0
        return {
            **counts,
            "retained": len(registry.cached_chain),
            "waiter_cache_get": counts["waiter_cache_get"],
        }

    async def drain(core):
        while True:
            await core.event_pipe.join()
            tasks = tuple(core.event_loop._dispatch_tasks)
            if not tasks:
                return
            await asyncio.gather(*tasks)
            await asyncio.sleep(0)

    async def reply_benchmark():
        loop = asyncio.get_running_loop()
        assert not loop.get_debug()
        module = importlib.import_module("apixis.core.event.event_loop")
        module.EVENT_LOOP_BACKPRESSURE = 128
        module.SHOW_EVENT_DISPATCH = False
        await factory.start_core()
        core = factory._get_core()
        pipe = core.event_pipe
        expect = getattr(subscription, "expect_event", subscription.wait_for_event)
        samples = []
        try:
            async with asyncio.timeout(60):
                for run in range(6):
                    core.handler_registry.clear()

                    async def respond(event):
                        await pipe.post_event(
                            event_type=EventType.INFO,
                            event_name="cause.reply",
                            context=event.context,
                        )

                    entry = ApixEventHandler(respond, name="respond")
                    entry.subscribe = ["cause.request"]
                    entry.priority = 1
                    core.handler_registry.register_handler(entry)
                    gc.collect()
                    started = time.perf_counter()
                    for index in range(2048):
                        async with expect("cause.reply", point="processed") as waiter:
                            await pipe.post_event(
                                event_type=EventType.INFO,
                                event_name="cause.request",
                                context=index,
                            )
                            result = await waiter.wait()
                            assert result.context == index and not result.has_error
                    elapsed = time.perf_counter() - started
                    await drain(core)
                    assert set(core.handler_registry.registry) == {"respond"}
                    assert core.event_loop._event_semaphore._value == 128
                    if run:
                        samples.append(elapsed)
        finally:
            await factory.stop_core()
        return {"roundtrips": 2048, "seconds": samples}

    digest = hashlib.sha256()
    for p in sorted((args.source / "apixis").rglob("*.py")):
        if p.name != "_version.py":
            digest.update(str(p.relative_to(args.source)).encode())
            digest.update(p.read_bytes())
    result = {
        "label": args.label,
        "mode": args.mode,
        "python": sys.version,
        "gc": gc.isenabled(),
        "source_sha256": digest.hexdigest(),
        "public_code": code_info(ApixHandlerRegistry.get_handlers_for_event),
        "ordinary_code": code_info(ordinary(make_registry()).__func__),
    }
    if args.mode == "history":
        result["measurements"] = {
            kind: measure(kind)
            for kind in ["hot_handlers", "hot_names", "cold_handlers", "cold_names"]
        }
        result["path_counts"] = count_paths()
        with asyncio.Runner(debug=False) as runner:
            result["request_reply"] = runner.run(reply_benchmark())
    elif args.mode == "reply":
        with asyncio.Runner(debug=False) as runner:
            result["request_reply"] = runner.run(reply_benchmark())
    else:
        original = ApixHandlerRegistry.get_handlers_for_event
        tree = ast.parse(textwrap.dedent(inspect.getsource(original)))
        variants = {"current": original}
        sources = {"current": ast.unparse(tree)}
        for name, no_cell, no_check in [
            ("no_cell", True, False),
            ("no_check", False, True),
            ("neither", True, True),
        ]:
            variant = copy.deepcopy(tree)
            if no_cell:

                class Inline(ast.NodeTransformer):
                    def visit_GeneratorExp(self, node):
                        return ast.copy_location(
                            ast.ListComp(elt=node.elt, generators=node.generators), node
                        )

                variant = Inline().visit(variant)
            if no_check:
                condition = next(
                    n for n in variant.body[0].body if isinstance(n, ast.If)
                )
                condition.test = ast.Constant(value=True)
            ast.fix_missing_locations(variant)
            namespace = dict(original.__globals__)
            exec(
                compile(
                    variant,
                    f"<probe-{name}>",
                    "exec",
                    flags=__future__.annotations.compiler_flag,
                ),
                namespace,
            )
            variants[name] = namespace["get_handlers_for_event"]
            sources[name] = ast.unparse(variant)
        for name, body in [
            (
                "local_only",
                "chain = self.get_subscription_chain_for_event(event_name)\nreturn [self.registry[name] for name in chain]",
            ),
            (
                "inline_only",
                "return [self.registry[name] for name in self.get_subscription_chain_for_event(event_name)]",
            ),
        ]:
            source = (
                "def get_handlers_for_event(self,event_name):\n"
                + textwrap.indent(body, "    ")
                + "\n"
            )
            namespace = dict(original.__globals__)
            exec(source, namespace)
            variants[name] = namespace["get_handlers_for_event"]
            sources[name] = source
        result["variants"] = {name: code_info(fn) for name, fn in variants.items()}
        result["sources"] = sources
        rows = []
        order = [*variants, *reversed(variants)]
        try:
            for name in order:
                ApixHandlerRegistry.get_handlers_for_event = variants[name]
                assert not make_registry()._waiter_cache.positions
                rows.append(
                    {
                        "variant": name,
                        "measurements": {
                            kind: measure(kind)
                            for kind in ["hot_handlers", "cold_handlers"]
                        },
                    }
                )
        finally:
            ApixHandlerRegistry.get_handlers_for_event = original
        result["rows"] = rows
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(args.label, args.mode, "complete", flush=True)


if __name__ == "__main__":
    main()
