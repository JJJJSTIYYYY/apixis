"""Graph execution across real thread cores with both bounded queues saturated.

Scale with APIXIS_STRESS_CORES / GRAPHS / CALLS / TIMEOUT. Mutable state is
owned by each core; remote requests carry only immutable tuples.
"""

import asyncio
from collections import Counter
from contextvars import ContextVar
from functools import partial
from itertools import product
import os
from threading import get_ident, local
from typing import Annotated, TypedDict

import pytest

from apixis import (
    ApixEventPipe,
    AutoMerge,
    Command,
    EventType,
    GraphManager,
    ParallelNode,
    expect_event,
    get_event_runtime_status,
    get_graph_context,
    get_stream_writer,
    get_thread_bridge,
    post_and_wait_threadsafe,
    post_event_threadsafe,
    subscribe,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_token_context


_request = ContextVar("thread_graph_stress_request", default=None)
_LIMITS = list(product((1, 2), repeat=2))
pytestmark = pytest.mark.parametrize(
    "event_runtime_capacity",
    _LIMITS,
    indirect=True,
    ids=[f"slots-{slots}-queue-{queue}" for slots, queue in _LIMITS],
)


class ThreadState(TypedDict):
    request: str
    value: int
    visits: Annotated[list[str], AutoMerge()]
    replies: Annotated[list[int], AutoMerge()]


@pytest.fixture
def event_runtime_capacity(request, monkeypatch):
    capacity, queue_capacity = request.param
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=queue_capacity))
    return capacity


async def _on_core(owner, operation):
    """Inspect/control the owner loop even while its bridge consumer is blocked."""
    return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(operation(), owner.loop))


@pytest.fixture
def task_factory(request):
    return asyncio.eager_task_factory if request.param else None


@pytest.fixture
async def graph_cores(thread_cores, task_factory):
    count = int(os.getenv("APIXIS_STRESS_CORES", "3"))
    graph_count = int(os.getenv("APIXIS_STRESS_GRAPHS", "2"))
    calls = int(os.getenv("APIXIS_STRESS_CALLS", "6"))
    timeout = float(os.getenv("APIXIS_STRESS_TIMEOUT", "60"))
    assert count >= 2 and graph_count > 0 and calls >= 3 and timeout > 0
    names = tuple(f"graph-{index}" for index in range(count))
    runtime = local()

    def setup():
        asyncio.get_running_loop().set_task_factory(task_factory)
        core = factory._get_core()
        runtime.core = core
        runtime.owner = get_ident()
        runtime.target = names[(names.index(core.thread_id) + 1) % count]
        runtime.graphs = []
        runtime.children = []
        runtime.waiting = {}
        runtime.tasks = {}
        runtime.cancelled = []
        runtime.wave = -1
        runtime.errors = []
        runtime.deliveries = Counter()
        runtime.holds = 0
        runtime.release_holds = asyncio.Event()
        asyncio.get_running_loop().set_exception_handler(
            lambda _, context: runtime.errors.append(repr(context)),
        )

        def check(request):
            assert get_ident() == runtime.owner
            assert _request.get() == request
            assert get_graph_context().state["request"] == request

        async def rpc(value):
            request = _request.get()
            context = get_graph_context()
            check(request)
            event = await post_and_wait_threadsafe(
                event_type=EventType.TRANSIENT,
                event_name="stress.rpc",
                context=(core.thread_id, request, value),
                thread_ids=runtime.target,
            )
            assert not event.has_error, event.error_stack
            assert event.context == (request, value * 2 + 1)
            assert get_graph_context() is context
            check(request)
            chain = handler_chain_token_context.get()
            assert chain.loop is asyncio.get_running_loop() and not chain.closed
            return event.context[1]

        @subscribe("stress.echo", time_out=None)
        async def echo(event):
            request, value = event.context
            check(request)
            event.context = (request, value * 2)

        @subscribe("stress.rpc", time_out=None)
        async def service(event):
            source, request, value = event.context
            check(request)
            reply = await post_and_wait_threadsafe(
                event_type=EventType.TRANSIENT,
                event_name="stress.echo",
                context=(request, value),
                thread_ids=source,
            )
            assert not reply.has_error, reply.error_stack
            assert reply.context == (request, value * 2)
            check(request)
            assert handler_chain_token_context.get().held
            event.context = (request, reply.context[1] + 1)

        @subscribe("stress.hold", time_out=None)
        async def hold(event):
            runtime.holds += 1
            # Deliberately keep every dispatch permit occupied during saturation.
            await runtime.release_holds.wait()

        @subscribe("stress.filler", time_out=None)
        async def filler(event):
            assert get_ident() == runtime.owner
            runtime.deliveries[event.context] += 1

        async def leaf(state):
            return {"child": await rpc(state["value"])}

        def entry(child):
            async def start(state):
                request = state["request"]
                check(request)
                async with expect_event(f"stress.gate.{request}", point="processed") as waiter:
                    scope = asyncio.timeout(None)
                    runtime.waiting[request] = scope
                    try:
                        async with scope:
                            event = await waiter.get_result()
                        assert event.context == request
                    except TimeoutError:
                        assert scope.expired()
                        get_stream_writer()("timeout")
                        return {"outcome": "timeout"}
                context = get_graph_context()
                result = await child.invoke({"request": request, "value": state["value"]})
                assert get_graph_context() is context
                check(request)
                get_stream_writer()("start")
                return Command(
                    update={"child": result["child"], "visits": ["start"]}, goto="parallel"
                )

            return start

        def branch(index, label):
            async def execute(state):
                reply = await rpc(state["value"] + index)
                get_stream_writer()(label)
                return Command(update={"visits": [label], "replies": [reply]}, goto="finish")

            return execute

        async def finish(state):
            check(state["request"])
            get_stream_writer()("finish")
            return {"visits": ["finish"], "outcome": "reply"}

        for index in range(graph_count):
            child = (
                GraphManager()
                .add_node(leaf)
                .compile_graph(
                    "leaf",
                    using_namespace=f"child-{index}",
                    max_snapshots=2,
                )
            )
            runtime.children.append(child)
            runtime.graphs.append(
                GraphManager(ThreadState)
                .add_node(entry(child), "start")
                .add_node(ParallelNode([branch(1, "left"), branch(2, "right")], name="parallel"))
                .add_node(finish)
                .compile_graph(
                    "start",
                    using_namespace=f"parent-{index}",
                    max_snapshots=2,
                )
            )
        runtime.registered = set(core.handler_registry.registry)
        runtime.wait_listeners = core.handler_registry.waiter_count

    owners = {name: await thread_cores(name, setup) for name in names}
    try:
        yield runtime, owners, graph_count, calls, timeout
    finally:

        async def release():
            runtime.release_holds.set()
            for task in runtime.tasks.values():
                task.cancel()

        await asyncio.gather(*(_on_core(owner, release) for owner in owners.values()))


def _snapshot(runtime):
    """Read private leak checks only on the owning thread, after real work."""
    core = runtime.core
    return {
        "status": get_event_runtime_status(),
        "owner": runtime.owner,
        "wave": runtime.wave,
        "waiting": tuple(runtime.waiting),
        "holds": runtime.holds,
        "errors": tuple(runtime.errors),
        "deliveries": tuple(runtime.deliveries.items()),
        "cancelled_states_intact": all(
            context.status == "aborted" and context.state == initial
            for context, initial in runtime.cancelled
        ),
        "graph_contexts": sum(
            len(graph._contexts) for graph in [*runtime.graphs, *runtime.children]
        ),
        "registrations_restored": set(core.handler_registry.registry) == runtime.registered,
        "waiters_restored": core.handler_registry.waiter_count == runtime.wait_listeners,
        "semaphore_waiters": sum(
            not waiter.done() for waiter in (core.event_loop._event_semaphore._waiters or ())
        ),
    }


def _idle(snapshot):
    status = snapshot["status"]
    loop, bridge = status.event_loop, status.thread_bridge
    return (
        status.event_queue_size == 0
        and snapshot["waiters_restored"]
        and loop.dispatch_tasks == loop.background_tasks == loop.dispatch_permits_in_use == 0
        and not loop.pending_dispatch
        and bridge.queue_size == bridge.active_requests == bridge.admission_waiters == 0
        and snapshot["graph_contexts"] == snapshot["semaphore_waiters"] == 0
        and snapshot["registrations_restored"]
    )


@pytest.mark.parametrize("interrupt", [False, True], ids=["complete", "cancel-timeout-recover"])
@pytest.mark.parametrize("task_factory", [False, True], indirect=True, ids=["default", "eager"])
async def test_thread_graphs_recover_from_saturated_queues(graph_cores, interrupt, record_property):
    runtime, owners, graph_count, calls, timeout = graph_cores
    total = graph_count * calls

    async def inspect():
        return _snapshot(runtime)

    async def snapshots_until(predicate):
        snapshots = []
        try:
            while True:
                snapshots = await asyncio.gather(
                    *(_on_core(owner, inspect) for owner in owners.values())
                )
                assert all(not snapshot["errors"] for snapshot in snapshots), snapshots
                if all(predicate(snapshot) for snapshot in snapshots):
                    return snapshots
                await asyncio.sleep(0)
        except asyncio.CancelledError as error:
            error.add_note(f"Last owner-loop snapshots: {snapshots!r}")
            raise

    async def run_wave(wave):
        runtime.wave = wave
        runtime.waiting.clear()
        runtime.cancelled.clear()
        contexts = []
        actions = []

        async def run(graph, context, stream):
            request = context.state["request"]
            token = _request.set(request)
            try:
                if stream:
                    chunks = [chunk async for chunk in graph.stream(graph_context=context)]
                    assert sorted(chunks) == sorted(
                        ["timeout"]
                        if context.state["outcome"] == "timeout"
                        else ["start", "left", "right", "finish"],
                    )
                    return context.state
                return await graph.invoke(graph_context=context)
            finally:
                _request.reset(token)

        for graph_index, call_index in product(range(graph_count), range(calls)):
            value = graph_index * calls + call_index
            request = f"{runtime.core.thread_id}.{wave}.{value}"
            context = runtime.graphs[graph_index].create_context(
                {
                    "request": request,
                    "value": value,
                    "visits": [],
                    "replies": [],
                }
            )
            contexts.append(context)
            actions.append(
                ("cancel", "timeout", "reply")[value % 3] if interrupt and wave == 0 else "reply"
            )
            runtime.tasks[request] = asyncio.create_task(
                run(runtime.graphs[graph_index], context, bool(call_index % 2))
            )
        try:
            results = await asyncio.gather(*runtime.tasks.values(), return_exceptions=True)
            for result, context, action in zip(results, contexts, actions, strict=True):
                initial = {
                    "request": context.state["request"],
                    "value": context.state["value"],
                    "visits": [],
                    "replies": [],
                }
                if action == "cancel":
                    assert isinstance(result, asyncio.CancelledError)
                    assert context.status == "aborted" and context.state == initial
                    runtime.cancelled.append((context, initial))
                elif action == "timeout":
                    assert context.status == "finished"
                    assert result == initial | {"outcome": "timeout"}, result
                else:
                    value = initial["value"]
                    assert context.status == "finished", (result, context.state)
                    assert result == initial | {
                        "visits": ["start", "left", "right", "finish"],
                        "replies": [2 * (value + 1) + 1, 2 * (value + 2) + 1],
                        "child": 2 * value + 1,
                        "outcome": "reply",
                    }, result
                    assert context.steps == 3
            assert len({context.run_id for context in contexts}) == total
            assert _request.get() is None
            return dict(Counter(actions))
        finally:
            for task in runtime.tasks.values():
                task.cancel()
            await asyncio.gather(*runtime.tasks.values(), return_exceptions=True)
            runtime.tasks.clear()

    async def change_waiters():
        for request, scope in runtime.waiting.items():
            # Scheduling order need not match invocation order across graph tasks.
            action = int(request.rsplit(".", 1)[1]) % 3
            if action == 0:
                runtime.tasks[request].cancel()
            elif action == 1:
                scope.reschedule(asyncio.get_running_loop().time())

    async def release_holds():
        runtime.release_holds.set()

    async def release_graphs(name, requests):
        for request in requests:
            await post_event_threadsafe(
                event_name=f"stress.gate.{request}",
                context=request,
                thread_ids=name,
            )

    producers = {}
    wave_task = None
    outcomes = Counter()
    try:
        async with asyncio.timeout(timeout):
            for wave in range(2):
                wave_task = asyncio.create_task(
                    get_thread_bridge().call(partial(run_wave, wave), thread_ids=list(owners))
                )
                ready = await snapshots_until(
                    lambda snapshot: snapshot["wave"] == wave and len(snapshot["waiting"]) == total
                )
                if wave == 0:
                    slots = ready[0]["status"].event_loop.dispatch_capacity
                    queue_size = ready[0]["status"].event_queue_capacity
                    for _ in range(slots):
                        await post_event_threadsafe(
                            event_name="stress.hold", thread_ids=list(owners)
                        )
                    await snapshots_until(lambda snapshot: snapshot["holds"] == slots)
                    await post_event_threadsafe(
                        event_name="stress.pending", thread_ids=list(owners)
                    )
                    await snapshots_until(
                        lambda snapshot: snapshot["status"].event_loop.pending_dispatch
                    )
                    for index in range(queue_size):
                        await post_event_threadsafe(
                            event_name="stress.filler",
                            context=("prefill", index),
                            thread_ids=list(owners),
                        )
                    for name in owners:
                        for index in range(queue_size + 3):
                            producers[name, index] = asyncio.create_task(
                                post_event_threadsafe(
                                    event_name="stress.filler",
                                    context=("producer", index),
                                    thread_ids=name,
                                )
                            )
                    saturated = await snapshots_until(
                        lambda snapshot: (
                            snapshot["status"].event_queue_size == queue_size
                            and snapshot["status"].event_loop.dispatch_permits_in_use == slots
                            and snapshot["status"].thread_bridge.queue_size == queue_size
                            and snapshot["status"].thread_bridge.admission_waiters > 0
                        )
                    )
                    assert len({snapshot["owner"] for snapshot in saturated}) == len(owners)
                    if interrupt:
                        await asyncio.gather(
                            *(_on_core(owner, change_waiters) for owner in owners.values())
                        )
                        for index, task in enumerate(producers.values()):
                            if index % 2 == 0:
                                task.cancel()
                    await asyncio.gather(
                        *(_on_core(owner, release_holds) for owner in owners.values())
                    )

                # Include late replies for cancelled/timed-out invocations.
                await asyncio.gather(
                    *(
                        release_graphs(name, snapshot["waiting"])
                        for name, snapshot in zip(owners, ready, strict=True)
                    )
                )
                results = await wave_task
                assert set(results) == set(owners)
                if wave == 0:
                    published = await asyncio.gather(*producers.values(), return_exceptions=True)
                    assert all(
                        result is None or isinstance(result, asyncio.CancelledError)
                        for result in published
                    )
                    assert sum(
                        isinstance(result, asyncio.CancelledError) for result in published
                    ) == ((len(producers) + 1) // 2 if interrupt else 0)
                drained = await snapshots_until(_idle)
                assert all(snapshot["cancelled_states_intact"] for snapshot in drained), drained
                deliveries = {
                    name: dict(snapshot["deliveries"])
                    for name, snapshot in zip(owners, drained, strict=True)
                }
                for counts in deliveries.values():
                    assert all(count == 1 for count in counts.values()), counts
                    assert all(counts["prefill", index] == 1 for index in range(queue_size))
                for (name, index), result in zip(producers, published, strict=True):
                    if result is None:
                        assert deliveries[name]["producer", index] == 1
                for counts in results.values():
                    outcomes.update(counts)
                    assert sum(counts.values()) == total
                    if interrupt and wave == 0:
                        assert all(counts[action] > 0 for action in ("cancel", "timeout", "reply"))
                    else:
                        assert counts == {"reply": total}
    finally:
        await asyncio.gather(*(_on_core(owner, release_holds) for owner in owners.values()))
        for task in producers.values():
            task.cancel()
        if wave_task is not None:
            wave_task.cancel()
        await asyncio.gather(
            *producers.values(),
            *([wave_task] if wave_task is not None else []),
            return_exceptions=True,
        )
    record_property("cores", len(owners))
    record_property("graphs_per_core", graph_count)
    record_property("calls_per_graph", calls)
    record_property("invocations", total * len(owners) * 2)
    for action in ("reply", "cancel", "timeout"):
        record_property(action, outcomes[action])
