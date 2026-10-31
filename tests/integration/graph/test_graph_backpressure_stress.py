"""Many graph instances and callers under tiny dispatch and queue capacities.

Scale with APIXIS_STRESS_GRAPHS / APIXIS_STRESS_CALLS / APIXIS_STRESS_TIMEOUT.
All callers start together, without a caller-side concurrency limiter.
"""

import asyncio
from collections import Counter
from contextvars import ContextVar
from itertools import product
import os
from time import perf_counter
from typing import Annotated, TypedDict

import pytest

from apixis.core.event import (
    ApixEvent, ApixEventPipe, BuiltinChannel, EventType, EventWaiter,
    await_for, subscribe, wait_for_event,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_context
from apixis.core.graph import (
    AutoMerge, Command, GraphManager, ParallelNode, get_graph_dispatch_name,
    get_stream_writer,
)
from apixis.core.graph.context import get_graph_context
from apixis.core.utils import logger


_request = ContextVar("graph_backpressure_stress_request", default=None)
_LIMITS = list(product((1, 2, 3), repeat=2))
pytestmark = pytest.mark.parametrize(
    "event_runtime_capacity", _LIMITS, indirect=True,
    ids=[f"slots-{slots}-queue-{queue}" for slots, queue in _LIMITS],
)


class StressState(TypedDict):
    request: str
    visits: Annotated[list[str], AutoMerge()]


@pytest.fixture
def event_runtime_capacity(request, monkeypatch):
    capacity, queue_capacity = request.param
    monkeypatch.setattr(
        factory, "ApixEventPipe",
        lambda: ApixEventPipe(builtin=BuiltinChannel(maxsize=queue_capacity)),
    )
    return capacity


@pytest.fixture
async def stress_runtime(cleanup_event_runtime, monkeypatch):
    core = cleanup_event_runtime
    failures = []
    callbacks = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: callbacks.append(context))
    for name in ("debug", "info", "success", "warning"):
        monkeypatch.setattr(logger, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(logger, "error", lambda *args: failures.append(args))
    monkeypatch.setattr(
        "apixis.core.event.event_loop.SHOW_EVENT_DISPATCH", False,
    )
    try:
        yield core
        assert not failures, failures[:5]
        assert not callbacks, callbacks[:5]
    finally:
        loop.set_exception_handler(previous_handler)


async def _drain(core):
    """Include tail plugins and background handlers that can publish more work."""
    while True:
        await core.event_pipe.join()
        tasks = tuple(
            core.event_loop._dispatch_tasks | core.event_loop._background_handler_tasks
        )
        if not tasks and core.event_loop._pending_dispatch is None and core.event_pipe.empty():
            return
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)


def _assert_drained(core, registered):
    assert core.event_pipe.empty()
    assert core.event_loop._pending_dispatch is None
    assert not core.event_loop._dispatch_tasks
    assert not core.event_loop._background_handler_tasks
    semaphore = core.event_loop._event_semaphore
    assert semaphore._value == semaphore._bound_value
    assert not any(not waiter.done() for waiter in (semaphore._waiters or ()))
    assert set(core.handler_registry.registry) == registered


async def test_many_graphs_and_callers_with_nested_event_waits(stress_runtime, record_property):
    core = stress_runtime
    graph_count = int(os.getenv("APIXIS_STRESS_GRAPHS", "16"))
    calls_per_graph = int(os.getenv("APIXIS_STRESS_CALLS", "16"))
    timeout = float(os.getenv("APIXIS_STRESS_TIMEOUT", "120"))
    assert graph_count > 0 and calls_per_graph > 0 and timeout > 0
    total = graph_count * calls_per_graph
    node_visits = Counter()
    rpc_calls = Counter()
    service_calls = Counter()
    plugins = {phase: Counter() for phase in ("before", "after", "background")}
    stats = Counter()
    graphs = []
    children = []
    contexts = []
    ready = 0
    all_ready, release = asyncio.Event(), asyncio.Event()
    original_put = core.event_pipe.put

    async def track_put(event):
        stats["events"] += 1
        if core.event_pipe.full():
            stats["blocked_publications"] += 1
        if core.event_loop._event_semaphore._value == 0:
            stats["saturated_publications"] += 1
        await original_put(event)
        stats["queue_peak"] = max(stats["queue_peak"], core.event_pipe.qsize())

    # Runtime probes are temporary test instrumentation; publication still uses
    # the real bounded pipe, semaphore, dispatcher, handlers, and waiters.
    core.event_pipe.put = track_put

    def observe(label):
        request = _request.get()
        assert request is not None
        assert get_graph_context().state["request"] == request
        stats["dispatch_peak"] = max(
            stats["dispatch_peak"], len(core.event_loop._dispatch_tasks),
        )
        node_visits[request, label] += 1
        return request

    async def post(name, payload):
        await core.event_pipe.post_event(
            event_type=EventType.TRANSIENT, event_name=name, context=payload,
        )

    @subscribe("stress.backend.request", time_out=None)
    async def backend(event):
        payload = event.context
        assert _request.get() == payload["request"]
        assert get_graph_context().state["request"] == payload["request"]
        service_calls[payload["key"], "backend"] += 1
        await asyncio.sleep(0)
        await post(payload["backend_reply"], payload)

    @subscribe("stress.tool.request", time_out=None)
    async def tool(event):
        payload = event.context
        assert _request.get() == payload["request"]
        assert get_graph_context().state["request"] == payload["request"]
        service_calls[payload["key"], "tool"] += 1
        async with wait_for_event(payload["backend_reply"], point="processed") as waiter:
            await post("stress.backend.request", payload)
            result = await waiter.wait()
        assert result.context == payload
        await post(payload["reply"], payload)
        if payload["multi"]:
            await post(payload["ack"], payload)

    async def rpc(label, style, *, point="received"):
        request = _request.get()
        key = f"{request}.{label}"
        assert rpc_calls[key] == 0
        rpc_calls[key] += 1
        payload = {
            "request": request, "key": key, "reply": f"stress.reply.{key}",
            "ack": f"stress.ack.{key}", "backend_reply": f"stress.backend.{key}",
            "multi": style in ("multi", "direct"),
        }
        patterns = [payload["reply"], payload["ack"]] if payload["multi"] else payload["reply"]
        if style == "deferred":
            # await_for registers synchronously before its zero-budget timeout;
            # the returned waiter is a deterministic publish-before-wait barrier.
            waiter = await await_for(patterns, point=point, time_out=0, keep_listening=True)
            assert isinstance(waiter, EventWaiter)
            try:
                await post("stress.tool.request", payload)
                result = await waiter.get_result()
            finally:
                waiter.close()
        elif style == "direct":
            async with EventWaiter(patterns, point=point) as waiter:
                assert await waiter.wait(time_out=0, keep_listening=True) is waiter
                await post("stress.tool.request", payload)
                result = await waiter.get_result()
        else:
            async with wait_for_event(patterns, point=point) as waiter:
                await post("stress.tool.request", payload)
                result = await waiter.wait()
        events = result.values() if isinstance(result, dict) else [result]
        assert all(event.context == payload and not event.has_error for event in events)
        assert _request.get() == request
        assert get_graph_context().state["request"] == request

    async def leaf(state):
        observe("leaf")
        await rpc("leaf", "direct", point="processed")
        return {"leaf_done": True}

    async def start(state):
        observe("start")
        await rpc("start", "single")
        get_stream_writer()("start")
        return Command(update={"visits": ["start"]}, goto=["left", "parallel"])

    def make_left(child):
        async def left(state):
            context = get_graph_context()
            observe("left")
            assert await child.invoke({"request": state["request"]}) == {
                "request": state["request"], "leaf_done": True,
            }
            assert get_graph_context() is context
            get_stream_writer()("left")
            return Command(update={"visits": ["left"]}, goto="finish")
        return left

    def make_branch(index, style):
        async def branch(state):
            label = f"branch-{index}"
            observe(label)
            await rpc(label, style, point="processed" if index % 2 else "received")
            get_stream_writer()(label)
            return Command(update={"visits": [label]}, goto="finish")
        return branch

    async def finish(state):
        observe("finish")
        # Hard timeout must remove its listener and return borrowed capacity
        # before the same node can continue with a successful event round-trip.
        with pytest.raises(TimeoutError):
            await await_for(f"stress.never.{state['request']}", time_out=0)
        await rpc("finish", "single", point="processed")
        get_stream_writer()("finish")
        return {"visits": ["finish"], "done": True}

    for index in range(graph_count):
        child = GraphManager().add_node(leaf).compile_graph(
            "leaf", using_namespace=f"stress-child-{index}", max_snapshots=2,
        )
        children.append(child)
        graphs.append(
            GraphManager(StressState).add_node(start).add_node(make_left(child), "left")
            .add_node(ParallelNode([
                make_branch(0, "multi"), make_branch(1, "deferred"), make_branch(2, "direct"),
            ], name="parallel"))
            .add_node(finish).compile_graph(
                "start", using_namespace=f"stress-parent-{index}", max_snapshots=2,
            )
        )

    pattern = get_graph_dispatch_name("stress-parent-*")

    @subscribe(pattern, priority=10, time_out=None)
    async def before(event):
        request = event.context.state["request"]
        assert _request.get() == request and get_graph_context() is event.context
        plugins["before"][request] += 1
        if event.context.steps == 0:
            await rpc("before", "single")

    @subscribe(pattern, priority=0, time_out=None)
    async def after(event):
        request = event.context.state["request"]
        assert _request.get() == request and get_graph_context() is event.context
        plugins["after"][request] += 1
        assert handler_chain_context.get().held

    @subscribe(pattern, background=True, time_out=None)
    async def background(event):
        request = event.context.state["request"]
        await asyncio.sleep(0)
        assert _request.get() == request and get_graph_context() is event.context
        assert handler_chain_context.get() is None
        plugins["background"][request] += 1

    registered = set(core.handler_registry.registry)
    expected_visits = ["start", "left", "branch-0", "branch-1", "branch-2", "finish"]

    async def run(graph_index, call_index):
        nonlocal ready
        ready += 1
        if ready == total:
            all_ready.set()
        await release.wait()
        request = f"g{graph_index}-c{call_index}"
        token = _request.set(request)
        try:
            graph = graphs[graph_index]
            context = graph.create_context({"request": request, "visits": []})
            contexts.append(context)
            if call_index % 2:
                chunks = []
                async for chunk in graph.stream(graph_context=context):
                    assert _request.get() == request
                    with pytest.raises(RuntimeError, match="only available"):
                        get_graph_context()
                    chunks.append(chunk)
                assert sorted(chunks) == sorted(expected_visits)
                result = context.state
            else:
                result = await graph.invoke(graph_context=context)
            assert result == {"request": request, "visits": expected_visits, "done": True}
            assert context.status == "finished" and context.steps == 3
            assert _request.get() == request
            with pytest.raises(RuntimeError, match="only available"):
                get_graph_context()
        finally:
            _request.reset(token)

    started = perf_counter()
    try:
        async with asyncio.timeout(timeout):
            async with asyncio.TaskGroup() as group:
                for graph_index, call_index in product(range(graph_count), range(calls_per_graph)):
                    group.create_task(run(graph_index, call_index))
                await all_ready.wait()
                release.set()
            await _drain(core)
        expected_requests = {f"g{g}-c{c}" for g, c in product(range(graph_count), range(calls_per_graph))}
        assert len(contexts) == total
        assert len({context.run_id for context in contexts}) == total
        for request in expected_requests:
            assert all(node_visits[request, label] == 1 for label in [*expected_visits, "leaf"])
            assert all(plugins[phase][request] == 4 for phase in plugins)
            for label in ("before", "start", "leaf", "branch-0", "branch-1", "branch-2", "finish"):
                key = f"{request}.{label}"
                assert rpc_calls[key] == 1
                assert service_calls[key, "tool"] == service_calls[key, "backend"] == 1
        assert sum(node_visits.values()) == total * 7
        assert len(rpc_calls) == total * 7
        assert stats["events"] == total * 37
        assert stats["blocked_publications"] > 0
        assert stats["saturated_publications"] > 0
        assert stats["queue_peak"] == core.event_pipe.maxsize
        assert not any(graph._contexts for graph in [*graphs, *children])
        _assert_drained(core, registered)
        assert _request.get() is None
        for name, value in {
            "parent_graphs": graph_count, "child_graphs": graph_count,
            "calls_per_graph": calls_per_graph, "invocations": total,
            "dispatch_capacity": core.event_loop._event_semaphore._bound_value,
            "queue_capacity": core.event_pipe.maxsize,
            "node_executions": total * 7, "rpc_round_trips": total * 7,
            "seconds": round(perf_counter() - started, 4), **stats,
        }.items():
            record_property(name, value)
    finally:
        release.set()
        core.event_pipe.put = original_put


async def test_many_waiting_callers_recover_after_cancellation_timeout_and_restart(
    stress_runtime, record_property,
):
    """Interrupt a waiting cohort, resume a full queue, then reuse every graph."""
    core = stress_runtime
    graph_count = int(os.getenv("APIXIS_STRESS_GRAPHS", "16"))
    calls_per_graph = int(os.getenv("APIXIS_STRESS_CALLS", "16"))
    timeout = float(os.getenv("APIXIS_STRESS_TIMEOUT", "120"))
    total = graph_count * calls_per_graph
    scopes = {}
    all_waiting = asyncio.Event()

    async def node(state):
        request = state["request"]
        assert _request.get() == request
        context = get_graph_context()
        assert context.state["request"] == request
        async with wait_for_event(f"recovery.reply.{request}", point="processed") as waiter:
            scope = asyncio.timeout(None)
            scopes[request] = scope
            if len(scopes) == total:
                all_waiting.set()
            try:
                async with scope:
                    event = await waiter.wait()
                assert event.context == request
                outcome = "reply"
            except TimeoutError:
                assert scope.expired()
                outcome = "timeout"
        assert _request.get() == request
        assert get_graph_context() is context
        get_stream_writer()(outcome)
        return {"outcome": outcome}

    graphs = [
        GraphManager().add_node(node).compile_graph("node")
        for _ in range(graph_count)
    ]
    registered = set(core.handler_registry.registry)
    counts = Counter()

    async def run(graph, context, mode):
        token = _request.set(context.state["request"])
        try:
            if mode == "invoke":
                return await graph.invoke(graph_context=context)
            chunks = [chunk async for chunk in graph.stream(graph_context=context)]
            assert chunks == [context.state["outcome"]]
            return context.state
        finally:
            _request.reset(token)

    async def cohort(wave):
        scopes.clear()
        all_waiting.clear()
        tasks = []
        contexts = []
        actions = []
        for graph_index, call_index in product(range(graph_count), range(calls_per_graph)):
            request = f"w{wave}-g{graph_index}-c{call_index}"
            context = graphs[graph_index].create_context({"request": request})
            contexts.append(context)
            actions.append(("cancel", "timeout", "reply")[call_index % 3] if wave == 0 else "reply")
            tasks.append(asyncio.create_task(run(
                graphs[graph_index], context, "stream" if call_index % 2 else "invoke",
            )))
        try:
            await all_waiting.wait()
            assert len(scopes) == total
            # All node handlers have lent their slots while waiting for events.
            assert len(core.event_loop._dispatch_tasks) >= total
            if wave == 0:
                await core.event_loop.stop()
                for index in range(core.event_pipe.maxsize):
                    name = f"recovery.filler.{index}"
                    core.event_pipe.put_nowait(ApixEvent(name, EventType.TRANSIENT, name, None, 0))
                assert core.event_pipe.full()
                for task, context, action in zip(tasks, contexts, actions, strict=True):
                    if action == "cancel":
                        task.cancel()
                    elif action == "timeout":
                        scopes[context.state["request"]].reschedule(asyncio.get_running_loop().time())
                # Let scheduled cancellations/deadlines fire while consumption
                # is paused, including while terminal events encounter a full pipe.
                await asyncio.sleep(0)
                await core.event_loop.start()

            # Late replies also reach cancelled attempts; stale node results
            # must not commit, leak listeners, or obstruct the healthy cohort.
            for context in contexts:
                request = context.state["request"]
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT,
                    event_name=f"recovery.reply.{request}", context=request,
                )
            results = await asyncio.gather(*tasks, return_exceptions=True)
            await _drain(core)
            for result, context, action in zip(results, contexts, actions, strict=True):
                counts[wave, action] += 1
                if action == "cancel":
                    assert isinstance(result, asyncio.CancelledError)
                    assert context.status == "aborted"
                    assert context.state == {"request": context.state["request"]}
                else:
                    assert result == {"request": context.state["request"], "outcome": action}
                    assert context.status == "finished"
            assert not any(graph._contexts for graph in graphs)
            _assert_drained(core, registered)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async with asyncio.timeout(timeout):
        await cohort(0)
        await cohort(1)
    assert counts[0, "cancel"] > 0 and counts[0, "timeout"] > 0 and counts[0, "reply"] > 0
    assert counts[1, "reply"] == total
    assert _request.get() is None
    for name, value in {
        "graphs": graph_count, "calls_per_graph": calls_per_graph,
        "invocations": total * 2, "cancelled": counts[0, "cancel"],
        "timed_out": counts[0, "timeout"], "recovery_successes": total,
    }.items():
        record_property(name, value)
