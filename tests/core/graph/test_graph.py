"""Behaviour tests for the event-driven graph runtime."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from apixis.core.utils.exception import InvalidNodeReturnsError
from apixis.core.graph import (
    AutoMerge,
    BaseNode,
    Command,
    GraphManager,
    Node,
    NodeGraph,
    ParallelNode,
    Reset,
)


pytestmark = pytest.mark.asyncio(loop_scope="session")


class CommandListNode(BaseNode):
    """Specialised node used to exercise the multi-command runtime contract."""

    def __init__(self, commands: list[Command], name: str = "commands"):
        self.name = name
        self.commands = commands

    async def execute(self, state: dict) -> list[Command]:
        return self.commands


@pytest.mark.parametrize("namespace", [None, "", "<global>", "named-graph"])
async def test_start_node_routes_to_configured_node(namespace):
    """A new invocation activates the configured entry point."""
    calls = []

    def first(state):
        calls.append(state)
        return {}

    graph = (
        GraphManager()
        .add_node(first)
        .compile_graph(entry_point="first", using_namespace=namespace)
    )

    await asyncio.wait_for(graph.invoke({"value": 1}), timeout=1)

    assert calls == [{"value": 1}]


async def test_node_update_is_carried_to_end():
    """A node's Command.update is returned after terminal dispatch."""

    def increment(state):
        return Command(update={"number": state["number"] + 1})

    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )

    assert await graph.invoke({"number": 1}) == {"number": 2}


class AccumulatingState(TypedDict):
    """Graph state containing additive and replacement fields."""

    messages: Annotated[list[str], AutoMerge()]
    status: str


async def test_annotated_state_field_auto_increases_across_nodes():
    """GraphManager forwards its schema to the compiled runtime."""

    def append_message(state):
        return {
            "messages": ["second"],
            "status": "updated",
        }

    graph = (
        GraphManager(AccumulatingState)
        .add_node(append_message)
        .compile_graph(entry_point="append_message")
    )

    result = await graph.invoke(
        {
            "messages": ["first"],
            "status": "initial",
        }
    )

    assert result == {
        "messages": ["first", "second"],
        "status": "updated",
    }


async def test_command_list_is_applied_sequentially_in_original_order():
    """A specialised BaseNode may return commands in application order."""
    append_messages = CommandListNode(
        [
            Command(update={"messages": ["second"]}),
            Command(update={"messages": ["third"]}),
        ],
        "append_messages",
    )

    graph = (
        GraphManager(AccumulatingState)
        .add_node(append_messages)
        .compile_graph(entry_point="append_messages")
    )

    result = await graph.invoke(
        {
            "messages": ["first"],
            "status": "unchanged",
        }
    )

    assert result == {
        "messages": ["first", "second", "third"],
        "status": "unchanged",
    }


async def test_parallel_node_joins_commands_in_branch_declaration_order():
    """Parallel completion order cannot change state merge or routing order."""
    first_started = asyncio.Event()
    second_finished = asyncio.Event()
    unused_called = False

    async def first_branch(state):
        first_started.set()
        await second_finished.wait()
        return Command(
            update={
                "messages": ["first-branch"],
                "status": "first",
            },
            goto=[],
        )

    async def second_branch(state):
        await first_started.wait()
        second_finished.set()
        return Command(
            update={
                "messages": ["second-branch"],
                "status": "second",
            },
            goto="joined",
        )

    def joined(state):
        return {
            "messages": ["joined"],
            "status": "joined",
        }

    def unused(state):
        nonlocal unused_called
        unused_called = True
        return {"status": "unused"}

    parallel = ParallelNode(
        [first_branch, second_branch],
        name="parallel_work",
    )
    graph = (
        GraphManager(AccumulatingState)
        .add_nodes([parallel, joined, unused])
        .compile_graph(entry_point=parallel.name)
    )

    result = await asyncio.wait_for(
        graph.invoke(
            {
                "messages": ["initial"],
                "status": "initial",
            }
        ),
        timeout=1,
    )

    assert result == {
        "messages": [
            "initial",
            "first-branch",
            "second-branch",
            "joined",
        ],
        "status": "joined",
    }
    assert unused_called is False


async def test_empty_command_list_finishes_without_updates():
    """An empty list from a specialised node finishes without another step."""
    no_op = CommandListNode([], "no_op")
    graph = GraphManager().add_node(no_op).compile_graph(entry_point="no_op")

    assert await graph.invoke({"value": 1}) == {"value": 1}


@pytest.mark.parametrize(
    ("commands", "expected_routes"),
    [
        ([Command(goto="first"), Command()], "first"),
        ([Command(goto="first"), Command(goto="second")], ["first", "second"]),
        ([Command(goto="first"), Command(goto=None)], "first"),
        ([Command(goto="first"), Command(goto=None)], "first"),
        ([Command(), Command(goto="second")], "second"),
    ],
)
async def test_command_list_routes_are_flattened_in_order(
    commands,
    expected_routes,
):
    """Specialised multi-command nodes contribute every selected route."""
    graph = NodeGraph(
        {
            name: Node(lambda state: {}, name)
            for name in ("command_node", "default", "first", "second")
        },
        "command_node",
    )
    context = graph.create_context({})

    assert graph.apply_command(commands, "command_node", context) == expected_routes


async def test_later_command_overwrites_same_state_key():
    """Ordinary state updates use deterministic later-command-wins order."""
    command_node = CommandListNode(
        [
            Command(update={"winner": "first"}),
            Command(update={"winner": "second"}),
        ]
    )
    graph = (
        GraphManager()
        .add_node(command_node)
        .compile_graph(entry_point=command_node.name)
    )

    assert await graph.invoke({}) == {"winner": "second"}


async def test_replace_explicitly_overwrites_auto_increase_field():
    """A node can bypass AutoMerge for one Command update."""

    def replace_messages(state):
        return Command(
            update={
                "messages": Reset(["replacement"]),
            }
        )

    graph = (
        GraphManager(AccumulatingState)
        .add_node(replace_messages)
        .compile_graph(entry_point="replace_messages")
    )

    result = await graph.invoke(
        {
            "messages": ["original"],
            "status": "unchanged",
        }
    )

    assert result == {
        "messages": ["replacement"],
        "status": "unchanged",
    }


async def test_mapping_result_finishes_at_end():
    """A mapping result updates state and finishes the graph."""
    graph = (
        GraphManager()
        .add_node(lambda state: {"finished": True}, "final")
        .compile_graph(entry_point="final")
    )

    assert await graph.invoke({}) == {"finished": True}


async def test_async_node_is_awaited_by_graph():
    """The graph executes asynchronous user nodes before routing onward."""

    async def async_node(state):
        await asyncio.sleep(0)
        return {"done": True}

    graph = (
        GraphManager()
        .add_node(async_node)
        .compile_graph(entry_point="async_node")
    )

    assert await graph.invoke({}) == {"done": True}


async def test_node_command_selects_next_node():
    """An explicit goto selects the next node."""

    def source(state):
        return Command(update={"selected": True}, goto="target")

    graph = (
        GraphManager()
        .add_node(source)
        .add_node(lambda state: {"reached": True}, "target")
        .compile_graph(entry_point="source")
    )

    assert await graph.invoke({}) == {"selected": True, "reached": True}


async def test_none_goto_finishes_the_graph():
    """A None goto terminates the invocation."""

    def source(state):
        return Command(update={"finished": True}, goto=None)

    graph = GraphManager().add_node(source).compile_graph(entry_point="source")

    assert await graph.invoke({}) == {"finished": True}


async def test_unknown_command_goto_is_propagated_to_caller():
    """Runtime jumps to unknown nodes fail the graph invocation."""

    def source(state):
        return Command(update={}, goto="missing")

    graph = GraphManager().add_node(source).compile_graph(entry_point="source")

    with pytest.raises(ValueError, match="Unknown graph node `missing`"):
        await graph.invoke({})


async def test_concurrent_batch_is_isolated_and_collected_in_route_order():
    """Completion order cannot affect state visibility or merge order."""

    class State(TypedDict):
        history: Annotated[list[str], AutoMerge()]

    c_finished = asyncio.Event()
    a_finished = asyncio.Event()
    observed_states = []

    async def a(state):
        observed_states.append(dict(state))
        state["private"] = "a"
        await c_finished.wait()
        a_finished.set()
        return Command(update={"history": ["A"]}, goto=[])

    async def b(state):
        observed_states.append(dict(state))
        state["private"] = "b"
        await a_finished.wait()
        return Command(update={"history": ["B"]}, goto=[])

    async def c(state):
        observed_states.append(dict(state))
        state["private"] = "c"
        c_finished.set()
        return Command(update={"history": ["C"]}, goto=[])

    graph = (
        GraphManager(State)
        .add_node(lambda state: Command(goto=["a", "b", "c"]), "launch")
        .add_nodes([a, b, c])
        .compile_graph(entry_point="launch")
    )

    result = await graph.invoke({"history": []})

    assert result == {"history": ["A", "B", "C"]}
    assert all("private" not in state for state in observed_states)


async def test_concurrent_non_auto_merge_conflict_uses_retry_snapshot():
    """Direct writes may be partial while recovery retains the batch boundary."""
    graph = (
        GraphManager()
        .add_node(
            lambda state: Command(update={"ready": True}, goto=["a", "b"]),
            "launch",
        )
        .add_node(lambda state: {"shared": "a", "only_a": True}, "a")
        .add_node(lambda state: {"shared": "b"}, "b")
        .compile_graph(entry_point="launch")
    )

    context = graph.create_context({})
    with pytest.raises(ValueError, match="non-AutoMerge state field `shared`"):
        await graph.invoke(graph_context=context)

    assert context.state == {
        "ready": True,
        "shared": "a",
        "only_a": True,
    }
    assert context.get_snapshot()["target_node_name"] == ["a", "b"]
    assert context.get_snapshot()["state"] == {"ready": True}


async def test_concurrent_node_failure_cancels_siblings_without_commit():
    """One branch failure cancels unfinished siblings and rejects all updates."""
    slow_started = asyncio.Event()
    slow_cancelled = asyncio.Event()

    async def slow(state):
        slow_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            slow_cancelled.set()
            raise

    async def fail(state):
        await slow_started.wait()
        raise RuntimeError("batch failed")

    graph = (
        GraphManager()
        .add_node(lambda state: Command(goto=["slow", "fail"]), "launch")
        .add_nodes([slow, fail])
        .compile_graph(entry_point="launch")
    )

    context = graph.create_context({"original": True})
    with pytest.raises(RuntimeError, match="batch failed"):
        await graph.invoke(graph_context=context)

    assert slow_cancelled.is_set()
    assert context.state == {"original": True}
    assert context.get_snapshot()["target_node_name"] == ["slow", "fail"]


async def test_concurrent_routes_follow_command_order_and_deduplicate():
    """Empty targets, explicit lists, and duplicates compose stably."""
    graph = NodeGraph(
        {name: Node(lambda state: {}, name) for name in ("A", "B", "C", "D")},
        "A",
    )
    context = graph.create_context({})

    routes = graph.apply_command(
        [Command(), Command(goto="A"), Command(goto=None), Command(goto=["A", "B"])],
        ["A", "B", "C", "D"],
        context,
    )

    assert routes == ["A", "B"]


async def test_concurrent_node_timeout_cancels_siblings_without_commit():
    """A branch timeout cancels its batch peers and leaves retry state intact."""
    peer_started = asyncio.Event()
    peer_cancelled = asyncio.Event()

    async def timeout_branch(state):
        await peer_started.wait()
        await asyncio.Event().wait()

    async def peer(state):
        peer_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            peer_cancelled.set()
            raise

    graph = (
        GraphManager()
        .add_node(lambda state: Command(goto=["timeout_branch", "peer"]), "launch")
        .add_node(timeout_branch, timeout=0.01)
        .add_node(peer)
        .compile_graph(entry_point="launch")
    )

    context = graph.create_context({"original": True})
    with pytest.raises(TimeoutError, match="timed out"):
        await graph.invoke(graph_context=context)

    assert peer_cancelled.is_set()
    assert context.state == {"original": True}
    assert context.get_snapshot()["target_node_name"] == ["timeout_branch", "peer"]


async def test_node_exception_is_propagated_to_caller():
    """Exceptions raised inside a node complete the invocation with that error."""

    def fail(state):
        raise RuntimeError("node failed")

    graph = GraphManager().add_node(fail).compile_graph(entry_point="fail")

    with pytest.raises(RuntimeError, match="node failed"):
        await graph.invoke({})


async def test_node_without_timeout_waits_for_normal_completion():
    """Omitting timeout leaves node execution unlimited."""

    async def slow_but_valid(state):
        await asyncio.sleep(0.02)
        return {"finished": True}

    graph = (
        GraphManager()
        .add_node(slow_but_valid)
        .compile_graph(entry_point="slow_but_valid")
    )

    assert graph._nodes["slow_but_valid"].timeout is None
    assert await graph.invoke({}) == {"finished": True}


async def test_explicit_node_timeout_fails_invocation_and_cancels_node():
    """A configured timeout settles completion with a visible error."""
    cancelled = asyncio.Event()

    async def slow(state):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    graph = (
        GraphManager()
        .add_node(slow, timeout=0.02)
        .compile_graph(entry_point="slow")
    )

    with pytest.raises(
        TimeoutError,
        match=r"Graph node `slow` timed out after 0.02 seconds",
    ):
        await graph.invoke({})

    assert cancelled.is_set()
    assert not graph._contexts


async def test_node_raised_timeout_error_is_not_mislabeled_as_deadline():
    """A node's own TimeoutError remains distinct from graph timeout."""

    async def fail(state):
        raise TimeoutError("provider timeout")

    graph = (
        GraphManager().add_node(fail, timeout=1).compile_graph(entry_point="fail")
    )

    with pytest.raises(TimeoutError, match="provider timeout"):
        await graph.invoke({})


async def test_invalid_node_result_is_propagated_to_caller():
    """Node return-contract violations are visible to graph callers."""
    graph = (
        GraphManager()
        .add_node(lambda state: None, "invalid")
        .compile_graph(entry_point="invalid")
    )

    with pytest.raises(InvalidNodeReturnsError, match="must return a dict or Command"):
        await graph.invoke({})


async def test_invalid_start_target_is_rejected_during_construction():
    """A malformed direct NodeGraph fails before registering its dispatch."""
    with pytest.raises(ValueError, match="Unknown graph node `missing`"):
        NodeGraph({}, "missing")


async def test_max_steps_stops_a_cycle():
    """A cyclic graph fails once its configured execution limit is exceeded."""
    loop_node = Node(
        lambda state: Command(update={"count": state.get("count", 0) + 1}, goto="loop"),
        "loop",
    )
    graph = NodeGraph(
        {"loop": loop_node},
        "loop",
        max_steps=2,
    )

    with pytest.raises(RecursionError, match="maximum of 2 steps"):
        await graph.invoke({})


async def test_graph_requires_dict_state():
    """Graph invocations reject non-dict state before posting an event."""
    graph = (
        GraphManager()
        .add_node(lambda state: {}, "node")
        .compile_graph(entry_point="node")
    )

    with pytest.raises(TypeError, match="Graph state must be a dict"):
        await graph.invoke([])


async def test_original_nested_input_is_not_mutated():
    """Nodes receive a deep copy and cannot mutate their caller's input."""
    original = {"wrapper": {"number": 1}}

    def mutate(state):
        state["wrapper"]["number"] = 2
        return state

    graph = GraphManager().add_node(mutate).compile_graph(entry_point="mutate")

    assert await graph.invoke(original) == {"wrapper": {"number": 2}}
    assert original == {"wrapper": {"number": 1}}


async def test_graphs_with_same_node_name_do_not_handle_each_others_runs():
    """Namespace-scoped dispatch listeners isolate graphs with shared node names."""
    calls = []

    def graph_a_node(state):
        calls.append("a")
        return {"graph": "a"}

    def graph_b_node(state):
        calls.append("b")
        return {"graph": "b"}

    graph_a = (
        GraphManager()
        .add_node(graph_a_node, "shared")
        .compile_graph(entry_point="shared", using_namespace="graph-a")
    )
    graph_b = (
        GraphManager()
        .add_node(graph_b_node, "shared")
        .compile_graph(entry_point="shared", using_namespace="graph-b")
    )

    results = await asyncio.gather(graph_a.invoke({}), graph_b.invoke({}))

    assert results == [{"graph": "a"}, {"graph": "b"}]
    assert sorted(calls) == ["a", "b"]


async def test_concurrent_invocations_keep_context_state_isolated():
    """Concurrent calls carry independent state through their event contexts."""

    def increment(state):
        return {"number": state["number"] + 1}

    graph = (
        GraphManager()
        .add_node(increment)
        .compile_graph(entry_point="increment")
    )

    results = await asyncio.gather(
        graph.invoke({"number": 1}),
        graph.invoke({"number": 10}),
    )

    assert results == [{"number": 2}, {"number": 11}]


async def test_concurrent_invocations_keep_context_state_deep_isolated():
    """Concurrent calls carry independent state through their event contexts."""

    def deep_increment(state):
        return {"number_wrapper": {"number": state["number_wrapper"]["number"] + 1}}

    graph = (
        GraphManager()
        .add_node(deep_increment)
        .compile_graph(entry_point="deep_increment")
    )

    results = await asyncio.gather(
        graph.invoke({"number_wrapper": {"number": 1}}),
        graph.invoke({"number_wrapper": {"number": 10}}),
    )

    assert results == [
        {"number_wrapper": {"number": 2}},
        {"number_wrapper": {"number": 11}},
    ]
