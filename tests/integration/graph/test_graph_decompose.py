"""Graph-owned lifecycle and forceful teardown across execution orders."""

import asyncio
import gc
import weakref
from types import SimpleNamespace
from typing import Annotated, TypedDict

import pytest

from apixis.core.event.factory import get_handler_registry
from apixis.core.graph import NodeGraph, Node, KeepRef, AutoMerge
from apixis.core.graph.base import namespace_set
from apixis.core.graph.utils import acquire_namespace, release_namespace
from apixis.core.graph.context import get_stream_writer


pytestmark = pytest.mark.asyncio(loop_scope="session")


class State(TypedDict):
    history: Annotated[list[str], AutoMerge()]
    resource: Annotated[dict, KeepRef()]


@pytest.mark.parametrize("force", [False, True])
async def test_decompose_leaves_pending_contexts_unchanged(force):
    graph = NodeGraph({}, None)
    context = graph.create_context({"value": [1]})
    graph.decompose(force=force)
    assert context.status == "pending"
    assert context.state == {"value": [1]}
    assert graph.namespace not in namespace_set
    with pytest.raises(RuntimeError, match="decomposed"):
        await graph.invoke(graph_context=context)
    with pytest.raises(RuntimeError, match="decomposed"):
        await anext(graph.stream(graph_context=context))
    graph.decompose()


async def test_context_and_checkpoint_do_not_retain_their_graph():
    graph = NodeGraph({"node": Node(lambda state: {})}, "node")
    context = graph.create_context({"value": [1]})
    await graph.invoke(graph_context=context)
    snapshot = context.get_snapshot()
    graph_id = graph.graph_id
    retained = weakref.ref(graph)
    graph.decompose()
    del graph
    gc.collect()
    assert retained() is None
    assert context.graph_id == graph_id
    assert snapshot["graph_id"] == graph_id


async def test_forced_decompose_aborts_only_running_contexts():
    started = asyncio.Queue()
    release = asyncio.Event()
    finished = asyncio.Queue()

    async def work(state):
        await started.put(state["label"])
        await release.wait()
        await finished.put(state["label"])
        return {"history": ["late"]}

    graph = NodeGraph({"work": Node(work)}, "work", state_schema=State)
    contexts = [
        graph.create_context({"label": name, "history": [name]}) for name in ("a", "b")
    ]
    pending = graph.create_context({"history": ["pending"]})
    tasks = [asyncio.create_task(graph.invoke(graph_context=c)) for c in contexts]
    try:
        await asyncio.wait_for(started.get(), 1)
        await asyncio.wait_for(started.get(), 1)
        graph.decompose(force=True)
        assert all(c.status == "aborted" for c in contexts)
        assert pending.status == "pending"
        assert not graph._contexts
        results = await asyncio.wait_for(asyncio.gather(*tasks), 1)
        assert [r["history"] for r in results] == [["a"], ["b"]]
        # Register and run a new graph while both old nodes are still blocked.
        replacement = NodeGraph({}, None, using_namespace=graph.namespace)
        assert await replacement.invoke({"fresh": True}) == {"fresh": True}
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 1)
    await asyncio.wait_for(finished.get(), 1)
    await asyncio.wait_for(finished.get(), 1)
    await asyncio.sleep(0)
    assert [c.state["history"] for c in contexts] == [["a"], ["b"]]


async def test_replacement_forces_active_old_graph_and_keeps_new_listener():
    started = asyncio.Event()
    release = asyncio.Event()

    async def work(state):
        started.set()
        await release.wait()
        return {"value": "late"}

    old = NodeGraph({"work": Node(work)}, "work", using_namespace="swap")
    context = old.create_context({"value": "saved"})
    old_task = asyncio.create_task(old.invoke(graph_context=context))
    try:
        await asyncio.wait_for(started.wait(), 1)
        replacement = NodeGraph({}, None, using_namespace="swap", exist_ok=True)
        assert context.status == "aborted"
        assert await asyncio.wait_for(old_task, 1) == {"value": "saved"}
        old.decompose()
        assert await replacement.invoke({"value": "new"}) == {"value": "new"}
    finally:
        release.set()
        await asyncio.wait_for(old_task, 1)


async def test_force_decompose_drains_buffered_stream_and_releases_registration():
    started = asyncio.Event()
    release = asyncio.Event()

    async def work(state):
        writer = get_stream_writer()
        writer("first")
        writer("second")
        started.set()
        await release.wait()
        writer("late")
        return {}

    graph = NodeGraph({"work": Node(work)}, "work")
    context = graph.create_context({})
    stream = graph.stream(graph_context=context)
    try:
        assert await asyncio.wait_for(anext(stream), 1) == "first"
        await asyncio.wait_for(started.wait(), 1)
        graph.decompose()
        replacement = NodeGraph({}, None)
        chunks = [chunk async for chunk in stream]
        assert chunks == ["second"]
        assert await replacement.invoke({"new": True}) == {"new": True}
    finally:
        release.set()
        await stream.aclose()
    assert context.status == "aborted"


async def test_graph_copies_raw_context_result_using_its_own_policy():
    graph = NodeGraph({}, None, state_schema=State)
    resource = {"shared": True}
    context = graph.create_context({"history": ["original"], "resource": resource})
    result = await graph.invoke(graph_context=context)
    assert context.completion.result() is context.state
    assert result is not context.state
    assert result["history"] is not context.state["history"]
    assert result["resource"] is resource
    result["history"].append("caller")
    assert context.state["history"] == ["original"]


async def test_abort_result_is_copied_by_graph_without_mutating_snapshot():
    started = asyncio.Event()
    release = asyncio.Event()

    async def work(state):
        state["resource"]["items"].append("node")
        started.set()
        await release.wait()
        return {}

    graph = NodeGraph({"work": Node(work)}, "work", state_schema=State)
    context = graph.create_context({"history": [], "resource": {"items": []}})
    task = asyncio.create_task(graph.invoke(graph_context=context))
    try:
        await asyncio.wait_for(started.wait(), 1)
        graph.decompose()
        result = await asyncio.wait_for(task, 1)
        result["resource"]["items"].append("caller")
        assert context.completion.result() is context.context_snapshot[-1]["state"]
        assert context.get_snapshot()["state"]["resource"]["items"] == []
    finally:
        release.set()
        await asyncio.wait_for(task, 1)


async def test_rejected_shortcut_does_not_leave_an_unreachable_context():
    graph = NodeGraph({"work": Node(lambda state: {})}, "work", max_steps=0)
    with pytest.raises(RecursionError):
        await graph.invoke({})
    with pytest.raises(RecursionError):
        await anext(graph.stream({}))
    graph.decompose(force=False)


async def test_namespace_registration_without_decomposition_only_changes_ownership():
    """Explicit registry-only cleanup does not require lifecycle capabilities."""
    owner = SimpleNamespace(namespace="pure-registry")
    before = dict(get_handler_registry().registry)
    try:
        assert acquire_namespace(owner) is owner
        assert "pure-registry" in namespace_set
        assert get_handler_registry().registry == before
        with pytest.raises(ValueError, match="already in use"):
            acquire_namespace(SimpleNamespace(namespace="pure-registry"))
    finally:
        release_namespace(owner, decompose_immediately=False)
    assert get_handler_registry().registry == before
