"""Plugin-facing cancellation contracts for graph invocation and interruption."""

import asyncio

import pytest

from apixis.core.graph.base import _END

from apixis.core.event import ApixEventHandler, subscribe, unsubscribe
from apixis.core.event.factory import get_event_pipe
from apixis.core.graph import GraphManager
from apixis.core.graph.context import get_stream_writer
from apixis.core.graph.interrupter.graph_interrupter import interrupt
from apixis.core.graph.utils.namespace import get_graph_interrupted_name


async def run_graph(graph, context, mode, chunks):
    """Exercise both public invocation interfaces."""
    if mode == "invoke":
        return await graph.invoke(graph_context=context)
    async for chunk in graph.stream(graph_context=context):
        chunks.append(chunk)


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("target", ["business", _END])
@pytest.mark.parametrize("cause", ["raise", "child", "future"])
async def test_upstream_plugin_cancellation_ends_call_and_runtime_remains_usable(mode, target, cause):
    """A cancelled plugin dependency must not strand an invocation or queue join."""
    called = []
    chunks = []
    dependency_ready = asyncio.Event()
    dependency = None
    notifications = []

    def business(state):
        called.append("business")
        get_stream_writer()("business chunk")
        return {"done": True}

    graph = (GraphManager().add_node(business).compile_graph(entry_point="business"))

    async def wait_forever():
        await asyncio.Future()

    async def plugin(event):
        nonlocal dependency
        if event.context.target_node_name != target:
            return
        if cause == "raise":
            raise asyncio.CancelledError("plugin cancelled")
        dependency = (asyncio.create_task(wait_forever()) if cause == "child"
                      else asyncio.get_running_loop().create_future())
        dependency_ready.set()
        await dependency

    async def cleanup(event):
        notifications.append(event)

    handler = subscribe(graph.dispatch_name, priority=10)(
        ApixEventHandler(plugin, on_cancelled=cleanup)
    )
    context = graph.create_context({})
    task = asyncio.create_task(run_graph(graph, context, mode, chunks))
    try:
        async with asyncio.timeout(2):
            if cause != "raise":
                await dependency_ready.wait()
                dependency.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await get_event_pipe().join()
            assert task.cancelled()
            assert context.status == "aborted"
            assert not context.is_active
            assert context.completion.cancelled()
            assert len(notifications) == 1
            assert len(notifications[0].error_stack) == 1 and notifications[0].error_stack[0].exception_type == 'CancelledError'
            assert called == (["business"] if target == _END else [])
            if mode == "stream":
                assert chunks == (["business chunk"] if target == _END else [])

            unsubscribe(handler.name)
            assert await graph.invoke({}) == {"done": True}
            await get_event_pipe().join()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if dependency is not None:
            dependency.cancel()
            await asyncio.gather(dependency, return_exceptions=True)
        unsubscribe(handler.name)
        graph.decompose()


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_node_cancellation_ends_call_without_committing_or_routing(mode):
    calls = []

    async def business(state):
        state["value"] = "uncommitted"
        raise asyncio.CancelledError("node cancelled")

    def downstream(state):
        calls.append("downstream")
        return state

    graph = (GraphManager().add_node(business).add_node(downstream)
             .compile_graph(entry_point="business"))
    context = graph.create_context({"value": "initial"})
    try:
        async with asyncio.timeout(1):
            with pytest.raises(asyncio.CancelledError):
                await run_graph(graph, context, mode, [])
            await get_event_pipe().join()
        assert context.completion.cancelled()
        assert context.status == "aborted"
        assert context.state == {"value": "initial"}
        assert calls == []
    finally:
        graph.decompose()


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("origin", ["plugin", "hook"])
@pytest.mark.parametrize("timeout", [None, 10])
async def test_interruption_dispatch_cancellation_releases_block_and_call(mode, origin, timeout):
    blocks = []
    resumed = []

    async def business(state):
        await interrupt(timeout=timeout)
        resumed.append(True)
        return state

    graph = (GraphManager().add_node(business)
             .compile_graph(entry_point="business"))

    @subscribe(get_graph_interrupted_name(graph.namespace, missing_ok=True), priority=10)
    async def plugin(event):
        blocks.append(event.context)
        if origin == "plugin":
            raise asyncio.CancelledError()

    async def hook(block):
        raise asyncio.CancelledError()

    graph.add_interrupted_hook(hook)
    context = graph.create_context({})
    try:
        async with asyncio.timeout(1):
            with pytest.raises(asyncio.CancelledError):
                await run_graph(graph, context, mode, [])
            await get_event_pipe().join()
        assert context.status == "aborted"
        assert context.completion.cancelled()
        assert len(blocks) == 1 and blocks[0].done
        assert resumed == []
    finally:
        unsubscribe(plugin.__name__)
        graph.decompose()


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_background_plugin_cancellation_does_not_cancel_graph(mode):
    cancelled = asyncio.Event()
    cleanup_events = []

    def business(state):
        return {"done": True}

    graph = (GraphManager().add_node(business)
             .compile_graph(entry_point="business"))

    async def background(event):
        if event.context.target_node_name == "business":
            raise asyncio.CancelledError()

    async def cleanup(event):
        cleanup_events.append(event)
        cancelled.set()

    background_handler = subscribe(graph.dispatch_name, background=True, priority=20)(
        ApixEventHandler(background, on_cancelled=cleanup)
    )

    @subscribe(graph.dispatch_name, priority=10)
    async def wait_for_background(event):
        await cancelled.wait()

    context = graph.create_context({})
    try:
        async with asyncio.timeout(1):
            await run_graph(graph, context, mode, [])
            await get_event_pipe().join()
        assert context.status == "finished"
        assert context.state == {"done": True}
        assert len(cleanup_events) == 1
        assert cleanup_events[0].error_stack == []
    finally:
        unsubscribe(background_handler.name)
        unsubscribe(wait_for_background.__name__)
        graph.decompose()


async def test_cleanup_failure_before_graph_notification_cannot_strand_call():
    def business(state):
        pytest.fail("Cancelled dispatch must not execute its graph node.")

    graph = (GraphManager().add_node(business)
             .compile_graph(entry_point="business"))

    async def plugin(event):
        raise asyncio.CancelledError()

    async def cleanup(event):
        raise ValueError("optional cleanup failed")

    handler = subscribe(graph.dispatch_name, priority=10)(
        ApixEventHandler(plugin, on_cancelled=cleanup)
    )
    context = graph.create_context({})
    try:
        async with asyncio.timeout(1):
            with pytest.raises(asyncio.CancelledError):
                await graph.invoke(graph_context=context)
            await get_event_pipe().join()
        assert context.completion.cancelled()
    finally:
        unsubscribe(handler.name)
        graph.decompose()
