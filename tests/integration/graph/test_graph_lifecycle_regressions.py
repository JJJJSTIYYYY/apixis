"""Regression coverage for centralized transitions and namespace teardown."""

import asyncio

import pytest

from apixis.core.graph import Command

from apixis.core.event import get_handler
from apixis.core.graph import NodeGraph, Node, get_graph_dispatch_name
from apixis.core.graph.base import namespace_set
from apixis.core.graph.utils import acquire_namespace, release_namespace
from apixis.core.graph.context import noop_stream_writer


pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest.mark.parametrize("terminal", [None, "finished", "failed", "aborted"])
async def test_rejected_bind_preserves_original_run_resources(terminal):
    graph = NodeGraph({}, None)
    context = graph.create_context({"value": 1})
    completion = asyncio.get_running_loop().create_future()
    writer = noop_stream_writer()
    context._bind(run_id="original", completion=completion, stream_writer=writer)
    if terminal == "finished":
        context._finish()
    elif terminal == "failed":
        context._fail(ValueError("original failure"))
        with pytest.raises(ValueError, match="original failure"):
            await completion
    elif terminal == "aborted":
        context.abort()

    attempted_completion = asyncio.get_running_loop().create_future()
    with pytest.raises(RuntimeError, match=f"{terminal or 'running'} -> running"):
        context._bind(
            run_id="rejected",
            completion=attempted_completion,
            stream_writer=noop_stream_writer(),
        )
    assert context.run_id == "original"
    assert context.completion is completion
    assert context.stream_writer is writer
    assert context.status == (terminal or "running")
    assert not attempted_completion.done()
    if terminal is None:
        context.abort()
        assert await completion == {"value": 1}


async def test_pending_failure_is_terminal_without_requiring_a_future():
    graph = NodeGraph({}, None)
    context = graph.create_context({"value": 1})
    context._fail(ValueError("preparation failed"))
    context._fail(ValueError("duplicate notification"))
    assert context.status == "failed"
    assert context.completion is None
    with pytest.raises(RuntimeError, match="failed -> aborted"):
        context.abort()
    graph.decompose(force=False)


async def test_release_namespace_retires_only_running_contexts():
    entered = asyncio.Event()
    leave = asyncio.Event()

    async def work(state):
        entered.set()
        await leave.wait()
        return {"value": "late"}

    graph = NodeGraph({"work": Node(work)}, "work", using_namespace="release")
    pending = graph.create_context({"value": "pending"})
    running = graph.create_context({"value": "checkpoint"})
    task = asyncio.create_task(graph.invoke(graph_context=running))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        release_namespace(graph)
        assert pending.status == "pending"
        assert running.status == "aborted"
        assert await asyncio.wait_for(task, 1) == {"value": "checkpoint"}
        assert graph.namespace not in namespace_set
        assert get_handler(graph.dispatch_name) is None
        with pytest.raises(RuntimeError, match="decomposed"):
            graph.create_context({})
        replacement = NodeGraph({}, None, using_namespace="release")
        release_namespace(graph)
        graph.decompose()
        assert await replacement.invoke({"value": "new"}) == {"value": "new"}
    finally:
        leave.set()
        await asyncio.wait_for(task, 1)


async def test_release_without_decompose_can_reacquire_the_same_live_graph():
    graph = NodeGraph({}, None, using_namespace="reacquire")
    pending = graph.create_context({"value": 1})
    handler = get_handler(graph.dispatch_name)
    release_namespace(graph, decompose_immediately=False)
    assert graph.namespace not in namespace_set
    assert pending.status == "pending"
    assert get_handler(graph.dispatch_name) is handler
    assert acquire_namespace(graph) is graph
    assert acquire_namespace(graph) is graph
    assert await graph.invoke(graph_context=pending) == {"value": 1}
    release_namespace(graph)
    release_namespace(graph)
    assert get_handler(graph.dispatch_name) is None


async def test_result_copy_error_is_not_masked_by_a_terminal_transition():
    class CopyOnce:
        """Allow initial state preparation but reject the output boundary copy."""

        def __init__(self, copied=False):
            self.copied = copied

        def __deepcopy__(self, memo):
            if self.copied:
                raise LookupError("output copy failed")
            return CopyOnce(copied=True)

    graph = NodeGraph({}, None)
    context = graph.create_context({"resource": CopyOnce()})
    with pytest.raises(LookupError, match="output copy failed"):
        await graph.invoke(graph_context=context)
    assert context.status == "finished"
    graph.decompose(force=False)


async def test_post_error_after_abort_preserves_the_original_exception(monkeypatch):
    graph = NodeGraph({}, None)
    context = graph.create_context({"value": 1})

    async def abort_then_reject(node_name, context):
        context.abort()
        raise LookupError("post failed after abort")

    monkeypatch.setattr(graph, "_post_next", abort_then_reject)
    with pytest.raises(LookupError, match="post failed after abort"):
        await graph.invoke(graph_context=context)
    assert context.status == "aborted"
    assert context.completion.result() == {"value": 1}


@pytest.mark.parametrize("version", [0, 1, -1, -2])
async def test_restore_context_input_selects_and_isolates_its_history(version):
    def increment(state):
        return {"value": state["value"] + 1}

    graph = NodeGraph(
        {"first": Node(lambda state: Command(update=increment(state), goto="second")),
         "second": Node(increment)},
        "first",
    )
    context = graph.create_context({"value": 0})
    assert await graph.invoke(graph_context=context) == {"value": 2}
    restored = graph.restore_context(context, version=version)
    expected = context.get_snapshot(version)
    assert restored.state == expected["state"]
    assert restored.target_node_name == expected["target_node_name"]
    assert restored.get_all_snapshots() == context.get_all_snapshots()[:version] + [expected]
    restored.state["value"] = 100
    assert context.get_snapshot(version) == expected
    assert restored.get_snapshot() == expected
    assert await graph.invoke(graph_context=restored) == {
        "value": 102 if version in (0, -2) else 101
    }


async def test_restore_foreign_context_checks_ownership_before_reading_history():
    class CannotCopy:
        def __deepcopy__(self, memo):
            raise AssertionError("Foreign state must not be copied")

    old = NodeGraph({}, None, using_namespace="restore")
    context = old.create_context({})
    context.context_snapshot.append({"state": CannotCopy()})
    replacement = NodeGraph({}, None, using_namespace="restore", exist_ok=True)
    with pytest.raises(ValueError, match="different graph"):
        replacement.restore_context(context)


async def test_restore_context_copies_only_the_selected_history():
    class CannotCopy:
        def __deepcopy__(self, memo):
            raise AssertionError("Discarded versions must not be copied")

    graph = NodeGraph({"node": Node(lambda state: {})}, "node")
    context = graph.create_context({"value": 1})
    await graph.invoke(graph_context=context)
    context.context_snapshot.append({"state": CannotCopy()})
    restored = graph.restore_context(context, version=0)
    assert await graph.invoke(graph_context=restored) == {"value": 1}


async def test_dispatch_name_accepts_a_graph_instance():
    graph = NodeGraph({}, None, using_namespace="dispatch-instance")
    assert get_graph_dispatch_name(graph) == graph.dispatch_name
    assert get_graph_dispatch_name(graph, missing_ok=False) == graph.dispatch_name
    graph.decompose()
    assert get_graph_dispatch_name(graph) == graph.dispatch_name
    with pytest.raises(KeyError, match="not found"):
        get_graph_dispatch_name(graph, missing_ok=False)
