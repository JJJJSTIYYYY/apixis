"""Reproduce the reviewed defects without modifying application source.

Run with the repository Python. Each probe runs in a separate process and
prints observations; a zero exit means the probes completed, not that the
runtime is free of defects. See findings.json and the accompanying report.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from apixis import (
    ApixEventHandler,
    EventWaiter,
    GraphManager,
    Logger,
    close_core,
    create_core,
    get_stream_writer,
    namespace_set,
    post_event,
    release_namespace,
    start_core,
    unsubscribe,
)


async def handler_rename():
    core = create_core()

    async def noop(event):
        pass

    handler = ApixEventHandler(noop, name="original").register("alpha")
    core.handler_registry.get_handlers_for_event("alpha")
    handler.name = "renamed"
    handler.register("beta")
    observation = {"keys_after_reregister": list(core.handler_registry.registry)}
    handler.unregister()
    observation["keys_after_unregister"] = list(core.handler_registry.registry)
    unsubscribe("original")
    observation["keys_after_remove_old_name"] = list(core.handler_registry.registry)
    observation["cached_alpha"] = core.handler_registry.cached_chain.get("alpha")
    try:
        core.handler_registry.get_handlers_for_event("alpha")
    except KeyError as error:
        observation["query_error"] = f"KeyError: {error}"
    await close_core()
    return observation


async def deadline(eager):
    create_core()
    await start_core()
    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory if eager else None)

    async def cancel(event):
        raise asyncio.CancelledError

    try:
        ApixEventHandler(cancel).register("deadline")
        waiter = EventWaiter("deadline", point="processed", time_out=0)
        await post_event(event_name="deadline")
        try:
            result = await waiter.wait()
        except TimeoutError:
            outcome = "TimeoutError"
        else:
            outcome = repr(result)
        await close_core()
        return {"eager": eager, "outcome": outcome}
    finally:
        loop.set_task_factory(original_factory)


async def stale_writer(late):
    create_core()
    await start_core()
    release, node_finished = asyncio.Event(), asyncio.Event()
    errors = []

    async def work(state):
        writer = get_stream_writer()
        writer("before")
        await release.wait()
        try:
            writer("after-abort")
        except RuntimeError as error:
            errors.append(str(error))
        node_finished.set()
        return {}

    graph = GraphManager().add_node(work).compile_graph("work")
    context = graph.create_context({})
    stream = graph.stream(graph_context=context)
    first = await anext(stream)
    await graph.abort(context)
    if not late:
        release.set()
    remaining = [chunk async for chunk in stream]
    if late:
        release.set()
    await node_finished.wait()
    observation = {
        "send_after_channel_closed": late,
        "status": context.status,
        "first": first,
        "chunks_after_abort": remaining,
        "errors": errors,
    }
    graph.decompose()
    await close_core()
    return observation


def fail_unregister_log(message):
    if message.startswith("Unregistered handler"):
        raise OSError("logger output unavailable")


async def graph_cleanup():
    core = create_core()
    graph = (
        GraphManager().add_node(lambda state: {}, "work")
        .compile_graph("work", using_namespace="audit-retire")
    )

    @graph.add_interrupted_hook
    async def hook(block):
        pass

    Logger.set_debug(fail_unregister_log)
    try:
        graph.decompose()
    except OSError:
        pass
    finally:
        Logger.set_debug(None)
    graph.decompose()
    observation = {
        "decomposed": graph._decomposed,
        "namespace_retained": graph.namespace in namespace_set,
        "remaining_group_handlers": len(
            core.handler_registry._group_handlers.get(graph.handler_group, ())
        ),
    }
    # Explicitly remove the resources normal decompose can no longer release.
    core.handler_registry.unregister_group(graph.handler_group, missing_ok=True)
    release_namespace(graph, decompose_immediately=False)
    await close_core()
    return observation


async def waiter_cleanup():
    core = create_core()
    waiter = EventWaiter(["alpha", "beta"])
    Logger.set_debug(fail_unregister_log)
    try:
        waiter.close()
    except OSError:
        pass
    finally:
        Logger.set_debug(None)
    waiter.close()
    await close_core()
    return {
        "core_state": core.state.value,
        "waiter_active": waiter.active,
        "waiter_registry_released": waiter._registry is None,
        "remaining_waiter_owners": len(core.handler_registry._waiter_cache.waiters),
    }


CASES = {
    "handler_rename": handler_rename,
    "deadline_default": lambda: deadline(False),
    "deadline_eager": lambda: deadline(True),
    "writer_early": lambda: stale_writer(False),
    "writer_late": lambda: stale_writer(True),
    "graph_cleanup": graph_cleanup,
    "waiter_cleanup": waiter_cleanup,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=CASES)
    args = parser.parse_args()
    if args.case:
        print(json.dumps(asyncio.run(CASES[args.case]())))
        return
    script = str(Path(__file__).resolve())
    findings = {}
    for case in CASES:
        result = subprocess.run(
            [sys.executable, script, "--case", case],
            capture_output=True, text=True, check=True, timeout=20,
        )
        findings[case] = json.loads(result.stdout)
    root = next(parent for parent in Path(script).parents if (parent / "pyproject.toml").is_file())
    env = {**os.environ, "PYTHONPATH": str(root / "src")}
    with tempfile.TemporaryDirectory(prefix="apixis-config-audit-") as temporary:
        (Path(temporary) / "config.yaml").write_text(
            'APIXIS:\n  LOG:\n    show_event_dispatch: "false"\n', encoding="utf-8",
        )
        result = subprocess.run(
            [sys.executable, "-c", (
                "import json; from apixis.core.config.core_config import SHOW_EVENT_DISPATCH as v; "
                "print(json.dumps({'type':type(v).__name__,'value':v,'enabled':bool(v)}))"
            )], cwd=temporary, env=env, capture_output=True, text=True,
            check=True, timeout=20,
        )
        findings["quoted_false_config"] = json.loads(result.stdout)
    print(json.dumps(findings, indent=2))


if __name__ == "__main__":
    main()
