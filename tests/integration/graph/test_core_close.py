"""Graph execution shares explicit stop/resume and terminal core cancellation."""

import asyncio

import pytest

from apixis import CoreState, close_core, start_core, stop_core, subscribe
from apixis.core.graph import GraphManager


@pytest.mark.parametrize("phase", ["created", "queued", "running"])
@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_close_terminates_graph_invocations_and_streams(
    cleanup_event_runtime, phase, mode
):
    core = cleanup_event_runtime
    if phase == "created":
        from apixis import create_core
        await close_core()
        core = create_core()
    entered = asyncio.Event()

    async def node(state):
        entered.set()
        await asyncio.Future()

    graph = GraphManager().add_node(node).compile_graph("node")
    context = graph.create_context({})
    if phase == "queued":
        await stop_core()

    async def call():
        if mode == "invoke":
            return await graph.invoke(graph_context=context)
        async for _ in graph.stream(graph_context=context):
            pass

    task = asyncio.create_task(call())
    try:
        async with asyncio.timeout(2):
            if phase == "running":
                await entered.wait()
            else:
                while core._event_pipe.empty():
                    await asyncio.sleep(0)
            await close_core()
        done, _ = await asyncio.wait({task}, timeout=0.1)
        assert task in done, "Core closure left the graph invocation pending."
        with pytest.raises(asyncio.CancelledError):
            await task
        assert context.status == "aborted"
        assert context.completion.cancelled()
        assert entered.is_set() == (phase == "running")
        assert core.state is CoreState.CLOSED
        assert core._event_pipe.empty()
    finally:
        if not task.done():
            task.cancel()
        graph.decompose()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_closed_pipe_cancels_graph_when_next_dispatch_cannot_be_published(
    cleanup_event_runtime, monkeypatch, mode,
):
    core = cleanup_event_runtime
    publishing, release = asyncio.Event(), asyncio.Event()
    downstream_called = asyncio.Event()

    def first(state):
        from apixis.core.graph import Command
        return Command(goto="downstream")

    def downstream(state):
        downstream_called.set()
        return state

    graph = GraphManager().add_nodes([first, downstream]).compile_graph("first")
    context = graph.create_context({})
    original_post = core._event_pipe.post_event

    async def post_event(**kwargs):
        if kwargs["event_name"] == graph.dispatch_name and context.steps == 1:
            publishing.set()
            await release.wait()
        await original_post(**kwargs)

    monkeypatch.setattr(core._event_pipe, "post_event", post_event)

    async def call():
        if mode == "invoke":
            return await graph.invoke(graph_context=context)
        async for _ in graph.stream(graph_context=context):
            pass

    task = asyncio.create_task(call())
    try:
        async with asyncio.timeout(2):
            await publishing.wait()
        core._event_pipe.close()
        release.set()
        done, _ = await asyncio.wait({task}, timeout=0.5)
        assert task in done, "Rejected dispatch publication left the graph pending."
        with pytest.raises(asyncio.CancelledError):
            await task
        assert context.status == "aborted"
        assert context.completion.cancelled()
        assert not downstream_called.is_set()
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        graph.decompose()


async def test_stop_does_not_pause_scheduled_graph_node_or_implicitly_resume_consumption(
    cleanup_event_runtime,
):
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def node(state):
        entered.set()
        await release.wait()
        finished.set()
        return {"value": 2}

    graph = GraphManager().add_node(node).compile_graph("node")
    invocation = asyncio.create_task(graph.invoke({"value": 1}))
    try:
        await entered.wait()
        await stop_core()
        release.set()
        await asyncio.wait_for(finished.wait(), 1)
        await asyncio.sleep(0)
        assert cleanup_event_runtime.state is CoreState.STOPPED
        assert not invocation.done()
        await start_core()
        assert await asyncio.wait_for(invocation, 1) == {"value": 2}
    finally:
        graph.decompose()
        await asyncio.gather(invocation, return_exceptions=True)


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_close_completes_all_queued_invocations_across_graphs(cleanup_event_runtime, mode):
    core = cleanup_event_runtime
    await stop_core()
    called = []

    def node(state):
        called.append(True)
        return state

    graphs = [GraphManager().add_node(node).compile_graph("node") for _ in range(3)]
    contexts = [(graph, graph.create_context({})) for graph in graphs for _ in range(2)]

    async def call(graph, context):
        if mode == "invoke":
            return await graph.invoke(graph_context=context)
        async for _ in graph.stream(graph_context=context):
            pass

    tasks = [asyncio.create_task(call(graph, context)) for graph, context in contexts]
    try:
        async with asyncio.timeout(1):
            while core._event_pipe.qsize() != len(tasks):
                await asyncio.sleep(0)
            await close_core()
        done, _ = await asyncio.wait(tasks, timeout=0.1)
        assert len(done) == len(tasks)
        assert all(task.cancelled() for task in tasks)
        assert all(context.completion.cancelled() for _, context in contexts)
        assert called == []
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for graph in graphs:
            graph.decompose()


@pytest.mark.parametrize("event_runtime_capacity", [1], indirect=True)
@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_close_completes_graph_waiting_for_dispatch_capacity(cleanup_event_runtime, mode):
    core = cleanup_event_runtime
    assert core._event_loop._event_semaphore.capacity == 1
    entered = asyncio.Event()

    @subscribe("occupy.capacity")
    async def occupy(event):
        entered.set()
        await asyncio.Future()

    from apixis import post_event
    await post_event(event_name="occupy.capacity")
    await entered.wait()

    def node(state):
        pytest.fail("Unassigned graph node must not execute during closure.")

    graph = GraphManager().add_node(node).compile_graph("node")
    context = graph.create_context({})

    async def call():
        if mode == "invoke":
            return await graph.invoke(graph_context=context)
        async for _ in graph.stream(graph_context=context):
            pass

    task = asyncio.create_task(call())
    try:
        async with asyncio.timeout(1):
            while core._event_loop._pending_dispatch is None:
                await asyncio.sleep(0)
            assert core._event_loop._pending_dispatch[0].context is context
            await close_core()
        done, _ = await asyncio.wait({task}, timeout=0.1)
        assert task in done
        assert task.cancelled()
        assert context.completion.cancelled()
        assert core._event_loop._pending_dispatch is None
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        graph.decompose()


async def test_decomposition_releases_core_close_callback(cleanup_event_runtime):
    import gc
    import weakref

    graph = GraphManager().compile_graph(entry_point=None)
    reference = weakref.ref(graph)
    graph.decompose()
    del graph
    gc.collect()
    assert reference() is None
    assert cleanup_event_runtime._close_task_done_callbacks == []


async def test_graph_cannot_silently_move_to_a_new_core(cleanup_event_runtime):
    from apixis import CoreClosedError, create_core

    graph = (
        GraphManager()
        .add_node(lambda state: {"done": True}, "node")
        .compile_graph("node")
    )
    await close_core()
    replacement = create_core()
    await start_core()
    try:
        with pytest.raises(CoreClosedError, match="closed event core"):
            await graph.invoke({})
        assert replacement._event_pipe.empty()
    finally:
        graph.decompose()


async def test_old_graph_cleanup_does_not_touch_the_replacement_registry(
    cleanup_event_runtime,
):
    from apixis import create_core

    old = (
        GraphManager()
        .add_node(lambda state: {}, "node")
        .compile_graph("node", using_namespace="reused")
    )
    await close_core()
    replacement = create_core()
    current = (
        GraphManager()
        .add_node(lambda state: {"new": True}, "node")
        .compile_graph("node", using_namespace="reused", exist_ok=True)
    )
    old.decompose()
    try:
        await start_core()
        assert await asyncio.wait_for(current.invoke({}), 1) == {"new": True}
        assert current.handler_group in replacement._handler_registry._group_handlers
    finally:
        current.decompose()
