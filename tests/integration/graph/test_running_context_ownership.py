"""Runtime ownership follows context state rather than caller handle lifetime."""

import asyncio
import gc
import weakref

import pytest

from apixis import GraphManager, get_stream_writer


pytestmark = pytest.mark.asyncio(loop_scope="session")


class Payload:
    """Weak-referenceable state value used to observe actual retention."""


@pytest.mark.parametrize("restored", [False, True])
@pytest.mark.parametrize("aborted", [False, True])
async def test_graph_does_not_retain_unused_context_state(restored, aborted):
    """Dropping a prepared context releases its state while its graph stays live."""
    graph = GraphManager().add_node(lambda state: {}, "work").compile_graph("work")
    context = graph.create_context({"payload": Payload()})
    if restored:
        await graph.invoke(graph_context=context)
        context = graph.restore_context(context)
    if aborted:
        context.abort()

    retained = weakref.ref(context.state["payload"])
    del context
    gc.collect()

    assert retained() is None
    assert await graph.invoke({"still_live": True}) == {"still_live": True}


async def test_direct_abort_releases_running_ownership_before_invocation_resumes():
    """A direct abort permits decomposition without waiting for invoke cleanup."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def work(state):
        started.set()
        await release.wait()
        return {"value": "late"}

    graph = GraphManager().add_node(work).compile_graph("work")
    context = graph.create_context({"value": "saved"})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))
    try:
        await asyncio.wait_for(started.wait(), 1)
        with pytest.raises(RuntimeError, match="running"):
            graph.decompose(force=False)
        context.abort()
        # No suspension occurs between abort and decomposition.
        graph.decompose(force=False)
        assert await asyncio.wait_for(invocation, 1) == {"value": "saved"}
    finally:
        release.set()
        await asyncio.gather(invocation, return_exceptions=True)


@pytest.mark.parametrize("outcome", ["finished", "failed", "cancelled"])
async def test_terminal_stream_releases_ownership_before_iteration_ends(outcome):
    """A stream paused at a chunk does not keep its terminal context managed."""
    release = asyncio.Event()

    async def work(state):
        get_stream_writer()("first")
        await release.wait()
        if outcome == "failed":
            raise ValueError("node failed")
        if outcome == "cancelled":
            raise asyncio.CancelledError
        return {}

    graph = GraphManager().add_node(work).compile_graph("work")
    context = graph.create_context({})
    stream = graph.stream(graph_context=context)
    try:
        assert await asyncio.wait_for(anext(stream), 1) == "first"
        with pytest.raises(RuntimeError, match="running"):
            graph.decompose(force=False)
        release.set()
        expected_error = {
            "finished": None,
            "failed": ValueError,
            "cancelled": asyncio.CancelledError,
        }[outcome]
        if expected_error is None:
            await asyncio.wait_for(asyncio.shield(context.completion), 1)
        else:
            with pytest.raises(expected_error):
                await asyncio.wait_for(asyncio.shield(context.completion), 1)

        assert context.status == ("aborted" if outcome == "cancelled" else outcome)
        graph.decompose(force=False)
        with pytest.raises(expected_error or StopAsyncIteration):
            await anext(stream)
    finally:
        release.set()
        await stream.aclose()
