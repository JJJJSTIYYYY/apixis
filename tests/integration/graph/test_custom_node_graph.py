"""End-to-end tests for core node extensions inside the compiled graph runtime."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from apixis.core.graph import (
    AutoMerge,
    BaseNode,
    Command,
    GraphManager,
    Node,
    NodeGraph,
    ParallelNode,
)


pytestmark = pytest.mark.asyncio(loop_scope="session")


class WorkflowState(TypedDict, total=False):
    """State schema used by the complete custom-node graph."""

    audit: Annotated[list[str], AutoMerge()]
    winner: str
    observed: dict
    route: str


class CommandBatchNode(BaseNode):
    """User-defined node that supplies an ordered batch through the core API."""

    def __init__(self, commands: list[Command], name: str = "batch") -> None:
        self.name = name
        self.commands = commands

    async def execute(self, state: dict) -> list[Command]:
        return self.commands


@pytest.mark.parametrize("list_target", [False, True], ids=["string", "singleton-list"])
@pytest.mark.parametrize("node_kind", ["ordinary", "parallel", "batch", "empty-batch"])
async def test_explicit_context_target_preserves_node_result_groups(list_target, node_kind):
    """Explicit singleton batches preserve commands and update order."""
    commands = [
        Command(update={"audit": ["first"], "winner": "first"}, goto="observe"),
        Command(update={"audit": ["second"], "winner": "second"}, goto="observe"),
    ]
    if node_kind == "ordinary":
        node = Node(lambda state: commands[0], name="work")
        expected_audit, expected_winner = ["first"], "first"
    elif node_kind == "parallel":
        node = ParallelNode(
            [lambda state: commands[0], lambda state: commands[1]], name="work"
        )
        expected_audit, expected_winner = ["first", "second"], "second"
    else:
        node = CommandBatchNode(commands if node_kind == "batch" else [], name="work")
        expected_audit = ["first", "second"] if node_kind == "batch" else []
        expected_winner = "second" if node_kind == "batch" else "initial"

    def observe(state: dict) -> dict:
        return {"observed": {"audit": state["audit"], "winner": state["winner"]}}

    graph = (
        GraphManager(WorkflowState)
        .add_nodes([node, observe])
        .compile_graph(entry_point="work")
    )
    context = graph.create_context({"audit": [], "winner": "initial"})
    context.target_node_name = ["work"] if list_target else "work"

    result = await asyncio.wait_for(graph.invoke(graph_context=context), timeout=1)

    expected = {"audit": expected_audit, "winner": expected_winner}
    if node_kind != "empty-batch":
        expected["observed"] = {"audit": expected_audit, "winner": expected_winner}
    assert result == expected
    assert context.steps == (1 if node_kind == "empty-batch" else 2)


async def test_concurrent_branches_are_applied_in_declaration_order():
    """GraphManager compiles ParallelNode into one ordered state flow."""
    second_started = asyncio.Event()
    completion_order: list[str] = []

    async def first(state: dict) -> Command:
        await second_started.wait()
        completion_order.append("first")
        return Command(update={"audit": ["first"], "winner": "first"}, goto="observe")

    async def second(state: dict) -> Command:
        completion_order.append("second")
        second_started.set()
        return Command(update={"audit": ["second"], "winner": "second"}, goto="observe")

    def observe(state: dict) -> dict:
        return {"observed": {"audit": state["audit"], "winner": state["winner"]}}

    parallel_node = ParallelNode([first, second])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([parallel_node, observe])
        .compile_graph(entry_point=parallel_node.name)
    )

    assert isinstance(graph, NodeGraph)
    result = await asyncio.wait_for(
        graph.invoke({"audit": [], "winner": "initial"}), timeout=1
    )

    assert completion_order == ["second", "first"]
    assert result["audit"] == ["first", "second"]
    assert result["winner"] == "second"
    assert result["observed"] == {"audit": ["first", "second"], "winner": "second"}


async def test_custom_command_goto_selects_next_node():
    """A custom node's route is honored after it returns its command list."""
    def fallback(state: dict) -> dict:
        return {"route": "fallback"}

    def selected(state: dict) -> dict:
        return {"route": "selected"}

    batch = CommandBatchNode([
        Command(update={"audit": ["selected"]}, goto="selected"),
    ])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([batch, fallback, selected])
        .compile_graph(entry_point=batch.name)
    )

    result = await graph.invoke({"audit": []})

    assert result["route"] == "selected"
    assert result["audit"] == ["selected"]


async def test_empty_command_list_finishes():
    """An empty command list completes the graph without further work."""
    def after_batch(state: dict) -> dict:
        return {"route": "after-batch"}

    batch = CommandBatchNode([])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([batch, after_batch])
        .compile_graph(entry_point=batch.name)
    )

    result = await graph.invoke({"audit": ["initial"]})

    assert result == {"audit": ["initial"]}


async def test_branch_exception_propagates_and_stops_downstream_node():
    """Branch failures complete NodeGraph.invoke with the original exception."""
    downstream_calls: list[str] = []

    async def explode(state: dict) -> dict:
        await asyncio.sleep(0)
        raise RuntimeError("branch exploded")

    def downstream(state: dict) -> dict:
        downstream_calls.append("called")
        return {"route": "downstream"}

    parallel_node = ParallelNode([explode])
    graph = (
        GraphManager(WorkflowState)
        .add_nodes([parallel_node, downstream])
        .compile_graph(entry_point=parallel_node.name)
    )

    with pytest.raises(RuntimeError, match="branch exploded"):
        await graph.invoke({})

    assert downstream_calls == []


async def test_graph_timeout_cancels_running_parallel_node():
    """GraphManager timeout applies to specialised core node execution."""
    cancelled = asyncio.Event()

    async def slow_branch(state: dict) -> dict:
        try:
            await asyncio.sleep(60)
            return {}
        except asyncio.CancelledError:
            cancelled.set()
            raise

    parallel_node = ParallelNode([slow_branch])
    graph = (
        GraphManager(WorkflowState)
        .add_node(parallel_node, timeout=0.02)
        .compile_graph(entry_point=parallel_node.name)
    )

    with pytest.raises(
        TimeoutError,
        match=r"Graph node `parallel` timed out after 0.02 seconds",
    ):
        await graph.invoke({})

    assert cancelled.is_set()
