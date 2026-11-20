"""Compiled configuration isolation, bounded checkpoints and ownership."""

import asyncio
import importlib

import pytest

from apixis import BaseNode, Command, GraphContext, GraphManager, Node, NodeGraph
from apixis.core.event.factory import get_handler_registry
from apixis.core.graph.base import _END, _namespace_graphs
from apixis.core.graph.context import noop_stream_writer


class ResourceNode(BaseNode):
    def __init__(self):
        self.name = "original"
        self.timeout = 1
        self.calls = []
        self.resource = asyncio.Lock()

    def __deepcopy__(self, memo):
        raise AssertionError("Execution resources must never be copied")

    async def execute(self, state):
        async with self.resource:
            self.calls.append(state["value"])
            await asyncio.sleep(0.01)
            return Command(update={"value": state["value"] + 1})


async def test_reusing_node_preserves_original_and_existing_graph_configuration():
    node = ResourceNode()
    first = GraphManager().add_node(node, "first", timeout=1).compile_graph("first")
    second = GraphManager().add_node(node, "second", timeout=2).compile_graph("second")
    assert node.name == "original"
    assert node.timeout == 1
    node.name = "changed"
    node.timeout = 0.0001
    assert await asyncio.wait_for(first.invoke({"value": 1}), 1) == {"value": 2}
    assert await asyncio.wait_for(second.invoke({"value": 3}), 1) == {"value": 4}
    assert node.calls == [1, 3]


async def test_direct_compilation_captures_configuration_but_shares_execution():
    node = ResourceNode()
    nodes = {"work": node}
    graph = NodeGraph(nodes, "work")
    node.timeout = 0.0001
    nodes["work"] = Node(lambda state: {"value": 0})
    assert await asyncio.wait_for(graph.invoke({"value": 4}), 1) == {"value": 5}
    assert node.calls == [4]


@pytest.mark.parametrize("name, timeout, error", [
    ("duplicate", 2, ValueError), ("new", float("nan"), ValueError),
    ("new", True, TypeError), (_END, 2, ValueError),
])
def test_failed_registration_does_not_mutate_source_or_manager(name, timeout, error):
    node = ResourceNode()
    manager = GraphManager().add_node(lambda state: {}, "duplicate")
    before = dict(manager._nodes)
    with pytest.raises(error):
        manager.add_node(node, name, timeout=timeout)
    assert manager._nodes == before
    assert node.name == "original"
    assert node.timeout == 1


def make_loop_graph(**options):
    def step(state):
        count = state["count"] + 1
        return Command(update={"count": count}, goto="step" if count < 20 else None)

    return GraphManager().add_node(step).compile_graph("step", **options)


@pytest.mark.parametrize("limit", [None, 1, 3])
async def test_snapshot_limit_keeps_recent_checkpoints_and_restores_independently(limit):
    graph = make_loop_graph(max_snapshots=limit)
    context = graph.create_context({"count": 0})
    assert await asyncio.wait_for(graph.invoke(graph_context=context), 2) == {"count": 20}
    expected_limit = 16 if limit is None else limit
    history = context.get_all_snapshots()
    assert context.max_snapshots == expected_limit
    assert [snapshot["state"]["count"] for snapshot in history] == list(range(20 - expected_limit, 20))
    restored = graph.restore_context(context)
    assert restored.max_snapshots == expected_limit
    restored.context_snapshot[-1]["state"]["count"] = -1
    assert context.context_snapshot[-1]["state"]["count"] == 19
    assert await graph.invoke(graph_context=restored) == {"count": 20}
    assert len(restored.context_snapshot) == expected_limit


async def test_configured_snapshot_default_is_used_by_compilation(monkeypatch):
    module = importlib.import_module("apixis.core.graph.node_graph")
    monkeypatch.setattr(module, "GRAPH_MAX_SNAPSHOTS", 2)
    graph = make_loop_graph()
    context = graph.create_context({"count": 0})
    await asyncio.wait_for(graph.invoke(graph_context=context), 2)
    assert context.max_snapshots == 2
    assert len(context.context_snapshot) == 2


async def test_manual_snapshots_and_restored_history_are_bounded():
    graph = NodeGraph({}, None, max_snapshots=2)
    context = graph.create_context({})
    context._bind(
        run_id="manual", completion=asyncio.get_running_loop().create_future(),
        stream_writer=noop_stream_writer(),
    )
    history = []
    for count in range(5):
        context.state = {"count": count}
        context.take_a_snapshot()
        history.append(context.get_snapshot())
    assert [item["state"]["count"] for item in context.context_snapshot] == [3, 4]
    restored = graph.restore_context(history)
    assert [item["state"]["count"] for item in restored.context_snapshot] == [3, 4]
    restored.context_snapshot[0]["state"]["count"] = 0
    assert history[3]["state"]["count"] == 3
    context.abort()


@pytest.mark.parametrize("value, error", [
    (True, TypeError), (1.5, TypeError), ("1", TypeError),
    (float("nan"), TypeError), (float("inf"), TypeError), (-1, ValueError),
])
def test_invalid_step_limit_fails_before_registration_and_setter_is_atomic(value, error):
    before_namespaces = dict(_namespace_graphs)
    before_handlers = dict(get_handler_registry().registry)
    with pytest.raises(error):
        NodeGraph({}, None, max_steps=value)
    assert dict(_namespace_graphs) == before_namespaces
    assert dict(get_handler_registry().registry) == before_handlers
    graph = NodeGraph({}, None, max_steps=2)
    with pytest.raises(error):
        graph.set_max_steps(value)
    assert graph._max_steps == 2


async def test_zero_steps_allows_completion_but_rejects_node_execution():
    assert await NodeGraph({}, None, max_steps=0).invoke({}) == {}
    graph = GraphManager().add_node(lambda state: {}, "step").compile_graph("step", max_steps=0)
    with pytest.raises(RecursionError):
        await graph.invoke({})


@pytest.mark.parametrize("value, error", [
    (0, ValueError), (-1, ValueError), (True, TypeError),
    (1.5, TypeError), (float("inf"), TypeError),
])
def test_invalid_snapshot_capacity_is_rejected(value, error):
    with pytest.raises(error):
        NodeGraph({}, None, max_snapshots=value)
    with pytest.raises(error):
        GraphContext("graph", value)


@pytest.mark.parametrize("status", ["pending", "finished", "failed", "aborted"])
async def test_abort_validates_foreign_context_even_after_completion(status):
    owner = NodeGraph({}, None)
    other = NodeGraph({}, None)
    context = owner.create_context({"value": 1})
    if status == "finished":
        await owner.invoke(graph_context=context)
    elif status == "aborted":
        context.abort()
    elif status == "failed":
        context._fail(RuntimeError("failure"))
    with pytest.raises(ValueError, match="different graph"):
        await other.abort(context)
    assert context.status == status
    if status in ("finished", "failed", "aborted"):
        await owner.abort(context)
        assert context.status == status
