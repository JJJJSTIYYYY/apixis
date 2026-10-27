"""Integration tests for aborting event-driven graph invocations."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from apixis.core.graph.base import _END

from apixis.core.graph import AutoMerge, Command, GraphManager
from apixis.core.utils.exception import InvalidContextError
from apixis.core.graph.context import get_stream_writer


pytestmark = pytest.mark.asyncio(loop_scope="session")


class AbortState(TypedDict, total=False):
    """State shared by the multi-stage abort integration graphs."""

    history: Annotated[list[str], AutoMerge()]
    route: str
    slow_result: str
    final_result: str


async def test_abort_invoke_returns_last_completed_snapshot_and_stops_routing():
    """Non-streaming abort skips the current update and all downstream nodes."""
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    slow_finished = asyncio.Event()
    final_called = asyncio.Event()

    def prepare(state):
        return Command(update={"history": ["prepare"], "route": "slow"}, goto="slow")

    async def slow(state):
        slow_started.set()
        await release_slow.wait()
        slow_finished.set()
        return Command(update={"history": ["slow"], "slow_result": "completed"}, goto="final")

    def unused(state):
        return Command(update={"history": ["unused"]}, goto="final")

    def final(state):
        final_called.set()
        return {"history": ["final"], "final_result": "completed"}

    graph = (
        GraphManager(AbortState)
        .add_nodes([prepare, slow, unused, final])

        .compile_graph(entry_point="prepare")
    )
    context = graph.create_context({"history": ["initial"]})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))

    await asyncio.wait_for(slow_started.wait(), timeout=1)
    await graph.abort(context)
    result = await asyncio.wait_for(invocation, timeout=0.5)

    assert result == {
        "history": ["initial", "prepare"],
        "route": "slow",
    }
    assert not slow_finished.is_set()
    assert not final_called.is_set()
    assert not graph._contexts

    # Abort is cooperative: the running node is allowed to finish, but its
    # result cannot reactivate routing or mutate the returned checkpoint.
    release_slow.set()
    await asyncio.wait_for(slow_finished.wait(), timeout=1)
    await asyncio.sleep(0.02)
    assert not final_called.is_set()
    assert result == {
        "history": ["initial", "prepare"],
        "route": "slow",
    }


async def test_context_abort_directly_finishes_its_invocation():
    """The caller-owned context is itself a usable cooperative abort handle."""
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    slow_finished = asyncio.Event()

    async def slow(state):
        slow_started.set()
        await release_slow.wait()
        slow_finished.set()
        return {"history": ["slow"]}

    graph = (
        GraphManager(AbortState).add_node(slow).compile_graph(entry_point="slow")
    )
    context = graph.create_context({"history": ["initial"]})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))

    await asyncio.wait_for(slow_started.wait(), timeout=1)
    context.abort()
    result = await asyncio.wait_for(invocation, timeout=0.5)

    assert result == {"history": ["initial"]}
    assert not graph._contexts
    assert not slow_finished.is_set()

    release_slow.set()
    await asyncio.wait_for(slow_finished.wait(), timeout=1)
    await asyncio.sleep(0.02)
    assert result == {"history": ["initial"]}


async def test_aborted_snapshot_recovers_while_stale_node_is_still_running():
    """A new context retries without waiting or accepting the stale result."""
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    first_finished = asyncio.Event()
    attempts = 0

    async def recoverable(state):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            first_started.set()
            await release_first.wait()
            first_finished.set()
            return {"history": ["stale-result"]}
        return {"history": ["recovered-result"]}

    graph = (
        GraphManager(AbortState)
        .add_node(recoverable)
        .compile_graph(entry_point="recoverable")
    )
    context = graph.create_context({"history": ["initial"]})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))

    await asyncio.wait_for(first_started.wait(), timeout=1)
    await graph.abort(context)
    assert await asyncio.wait_for(invocation, timeout=0.5) == {"history": ["initial"]}
    assert context.status == "aborted"
    assert context.target_node_name == "recoverable"

    recovered = graph.restore_context(context.context_snapshot)
    result = await graph.invoke(graph_context=recovered)

    assert result == {
        "history": ["initial", "recovered-result"],
    }
    assert "stale-result" not in result["history"]
    assert attempts == 2
    assert context.status == "aborted"
    assert recovered.status == "finished"
    assert recovered.target_node_name == _END
    assert recovered.steps == 1
    assert not first_finished.is_set()

    release_first.set()
    await asyncio.wait_for(first_finished.wait(), timeout=1)
    assert "stale-result" not in recovered.state["history"]


async def test_failed_snapshot_recovers_at_failed_node():
    """A failed node is retried from its pre-execution state snapshot."""
    attempts = 0

    async def flaky(state):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("first attempt failed")
        return {"history": ["recovered"]}

    graph = (
        GraphManager(AbortState)
        .add_node(flaky)
        .compile_graph(entry_point="flaky")
    )

    context = graph.create_context({"history": ["initial"]})
    with pytest.raises(RuntimeError, match="first attempt failed"):
        await graph.invoke(graph_context=context)

    assert context.status == "failed"
    assert context.target_node_name == "flaky"
    assert context.state == {"history": ["initial"]}

    recovered = graph.restore_context(context.context_snapshot)
    result = await graph.invoke(graph_context=recovered)

    assert result == {"history": ["initial", "recovered"]}
    assert attempts == 2
    assert context.status == "failed"
    assert recovered.status == "finished"


async def test_replacement_rejects_old_context_and_snapshot_while_stale_node_finishes():
    """A decomposed graph cannot intercept or control its namespace successor."""
    old_started = asyncio.Event()
    release_old = asyncio.Event()
    old_finished = asyncio.Event()

    async def old_node(state):
        old_started.set()
        await release_old.wait()
        old_finished.set()
        return {"history": ["stale-old"]}

    old_graph = (
        GraphManager(AbortState)
        .add_node(old_node, "shared")
        .compile_graph(entry_point="shared", using_namespace="replace-run")
    )
    old_context = old_graph.create_context({"history": ["initial"]})
    old_invocation = asyncio.create_task(old_graph.invoke(graph_context=old_context))
    await asyncio.wait_for(old_started.wait(), timeout=1)
    await old_graph.abort(old_context)
    assert await asyncio.wait_for(old_invocation, timeout=1) == {"history": ["initial"]}

    recovered = old_graph.restore_context(old_context.context_snapshot)

    def replacement_node(state):
        return {"history": ["replacement"]}

    replacement = (
        GraphManager(AbortState)
        .add_node(replacement_node, "shared")
        .compile_graph(entry_point="shared", using_namespace="replace-run", exist_ok=True)
    )
    with pytest.raises(ValueError, match="different graph"):
        replacement.restore_context(old_context.context_snapshot)
    with pytest.raises(InvalidContextError, match="different graph"):
        await replacement.invoke(graph_context=recovered)
    assert recovered.status == "pending"
    replacement_context = replacement.create_context({"history": ["initial"]})
    result = await replacement.invoke(graph_context=replacement_context)

    assert result == {"history": ["initial", "replacement"]}
    assert replacement_context.status == "finished"
    with pytest.raises(RuntimeError, match="has been decomposed"):
        await old_graph.abort(recovered)

    release_old.set()
    await asyncio.wait_for(old_finished.wait(), timeout=1)
    await asyncio.sleep(0)
    assert "stale-old" not in recovered.state["history"]


async def test_abort_stream_flushes_chunks_then_stops_complex_graph():
    """Streaming abort preserves queued output and prevents later nodes."""
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    slow_finished = asyncio.Event()
    final_called = asyncio.Event()

    async def prepare(state):
        writer = get_stream_writer()
        writer("prepare:started")
        await asyncio.sleep(0)
        writer("prepare:finished")
        return Command(update={"history": ["prepare"], "route": "slow"}, goto="slow")

    async def slow(state):
        get_stream_writer()("slow:started")
        slow_started.set()
        await release_slow.wait()
        slow_finished.set()
        return Command(update={"history": ["slow"], "slow_result": "completed"}, goto="final")

    def unused(state):
        get_stream_writer()("unused")
        return Command(update={"history": ["unused"]}, goto="final")

    def final(state):
        final_called.set()
        get_stream_writer()("final")
        return {"history": ["final"], "final_result": "completed"}

    graph = (
        GraphManager(AbortState)
        .add_nodes([prepare, slow, unused, final])

        .compile_graph(entry_point="prepare")
    )
    context = graph.create_context({"history": ["initial"]})
    stream = graph.stream(graph_context=context)

    chunks = [await asyncio.wait_for(anext(stream), timeout=1) for _ in range(3)]
    await asyncio.wait_for(slow_started.wait(), timeout=1)
    await graph.abort(context)

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(stream), timeout=0.5)

    assert chunks == ["prepare:started", "prepare:finished", "slow:started"]
    assert not slow_finished.is_set()
    assert not final_called.is_set()
    assert not graph._contexts

    release_slow.set()
    await asyncio.wait_for(slow_finished.wait(), timeout=1)
    await asyncio.sleep(0.02)
    assert not final_called.is_set()
