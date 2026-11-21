"""Graph execution shares explicit stop/resume and terminal core cancellation."""

import asyncio

import pytest

from apixis import CoreState, close_core, start_core, stop_core
from apixis.core.graph import GraphManager


@pytest.mark.parametrize("phase", ["queued", "running"])
@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_close_terminates_graph_invocations_and_streams(
    cleanup_event_runtime, phase, mode
):
    core = cleanup_event_runtime
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
                while core.event_pipe.empty():
                    await asyncio.sleep(0)
            await close_core()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert context.status == "aborted"
        assert context.completion.cancelled()
        assert entered.is_set() == (phase == "running")
        assert core.state is CoreState.CLOSED
        assert core.event_pipe.empty()
    finally:
        graph.decompose()
        await asyncio.gather(task, return_exceptions=True)


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
        assert replacement.event_pipe.empty()
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
        assert current.handler_group in replacement.handler_registry._group_handlers
    finally:
        current.decompose()
