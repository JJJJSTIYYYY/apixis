"""Public contracts for graphs driven exclusively by Command.goto."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from apixis import (
    AutoMerge, Command, GraphManager, Node, NodeGraph,
    get_graph_context,
)


@pytest.mark.parametrize("target", [None, []])
@pytest.mark.asyncio
async def test_empty_target_finishes_after_applying_update(target):
    """Ending a step commits its update without executing unrelated nodes."""
    def first(state):
        return Command(update={"done": True}, goto=target)

    def unused(state):
        pytest.fail("An unselected node must not execute")

    graph = GraphManager().add_nodes([first, unused]).compile_graph(entry_point="first")
    context = graph.create_context({})
    assert await asyncio.wait_for(graph.invoke(graph_context=context), 1) == {"done": True}
    assert context.steps == 1
    assert context.status == "finished"


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.asyncio
async def test_node_selects_next_step_using_its_updated_value(enabled):
    """Conditional execution needs only an ordinary node and one command."""
    async def choose(state):
        await asyncio.sleep(0)
        count = state["count"] + 1
        return Command(update={"count": count}, goto="finish" if enabled and count == 2 else None)

    def finish(state):
        return {"observed": state["count"]}

    graph = GraphManager().add_nodes([choose, finish]).compile_graph(entry_point="choose")
    result = await asyncio.wait_for(graph.invoke({"count": 1}), 1)
    assert result == ({"count": 2, "observed": 2} if enabled else {"count": 2})


@pytest.mark.parametrize("target", ["work", ["work"], ["work", "work"]])
@pytest.mark.asyncio
async def test_target_shape_is_preserved_and_duplicates_execute_once(target):
    """A singleton list remains a concurrent step in context and snapshots."""
    seen = []

    def launch(state):
        return Command(goto=target)

    def work(state):
        seen.append(get_graph_context().target_node_name)
        return {"done": True}

    graph = GraphManager().add_nodes([launch, work]).compile_graph(entry_point="launch")
    context = graph.create_context({})
    assert await asyncio.wait_for(graph.invoke(graph_context=context), 1) == {"done": True}
    expected = "work" if isinstance(target, str) else ["work"]
    assert seen == [expected]
    assert context.get_snapshot()["target_node_name"] == expected
    assert context.steps == 2


@pytest.mark.parametrize("terminal", [None, []])
@pytest.mark.asyncio
async def test_parallel_step_joins_commands_in_declaration_order(terminal):
    """Terminal branches do not suppress another branch's requested work."""
    class State(TypedDict):
        trace: Annotated[list[str], AutoMerge()]

    right_started = asyncio.Event()
    observed = []

    async def left(state):
        await right_started.wait()
        return Command(update={"trace": ["left"]}, goto=terminal)

    async def right(state):
        right_started.set()
        return Command(update={"trace": ["right"]}, goto=["join", "join"])

    def join(state):
        observed.append(state["trace"])
        return {"trace": ["join"]}

    graph = GraphManager(State).add_nodes([left, right, join]).compile_graph(
        entry_point=["left", "right", "left"]
    )
    context = graph.create_context({"trace": []})
    result = await asyncio.wait_for(graph.invoke(graph_context=context), 1)
    assert result == {"trace": ["left", "right", "join"]}
    assert observed == [["left", "right"]]
    assert context.steps == 2


@pytest.mark.asyncio
async def test_parallel_branches_converge_on_one_next_node():
    """Shared successors execute once after every branch update is available."""
    calls = []

    def left(state):
        return Command(update={"left": True}, goto="join")

    def right(state):
        return Command(update={"right": True}, goto="join")

    def join(state):
        calls.append(state)
        return {}

    graph = GraphManager().add_nodes([left, right, join]).compile_graph(entry_point=["left", "right"])
    assert await asyncio.wait_for(graph.invoke({}), 1) == {"left": True, "right": True}
    assert calls == [{"left": True, "right": True}]


@pytest.mark.parametrize("entry", [None, []])
@pytest.mark.asyncio
async def test_empty_graph_needs_no_execution_budget(entry):
    """An explicitly empty entry completes without executing a node batch."""
    graph = NodeGraph({}, entry, max_steps=0)
    context = graph.create_context({"unchanged": True})
    assert await asyncio.wait_for(graph.invoke(graph_context=context), 1) == {"unchanged": True}
    assert context.steps == 0


@pytest.mark.parametrize("entry, error", [
    ("missing", ValueError), (["missing"], ValueError),
    ("", ValueError),
    (1, TypeError), ([None], TypeError), (("work",), TypeError),
])
def test_invalid_entry_is_rejected_before_namespace_replacement(entry, error):
    """A malformed replacement must leave the original graph usable."""
    original = NodeGraph({}, None, using_namespace="entry-validation")
    with pytest.raises(error):
        NodeGraph({"work": Node(lambda state: {}, "work")}, entry,
                  using_namespace=original.namespace, exist_ok=True)
    assert original.create_context({}).status == "pending"


def test_compile_requires_an_explicit_entry_point():
    """Node registration order cannot silently choose the first step."""
    with pytest.raises(TypeError, match="entry_point"):
        GraphManager().compile_graph()


@pytest.mark.asyncio
async def test_entry_list_is_copied_at_compilation():
    """Changing a caller-owned entry list cannot mutate the compiled graph."""
    entry = ["first"]
    graph = GraphManager().add_node(lambda state: {"done": True}, "first").compile_graph(entry_point=entry)
    entry[:] = ["missing"]
    assert await asyncio.wait_for(graph.invoke({}), 1) == {"done": True}


@pytest.mark.asyncio
async def test_unregistered_next_node_is_rejected():
    """Commands may only select registered nodes."""
    graph = GraphManager().add_node(lambda state: Command(goto="node"), "work").compile_graph(entry_point="work")
    with pytest.raises(ValueError, match="Unknown graph node"):
        await asyncio.wait_for(graph.invoke({}), 1)


@pytest.mark.parametrize("terminal", [None, []])
@pytest.mark.asyncio
async def test_terminal_branch_does_not_change_single_successor_shape(terminal):
    """An ending branch contributes neither work nor a concurrent target."""
    seen = []

    def finish(state):
        return Command(goto=terminal)

    def proceed(state):
        return Command(goto="next")

    def next_node(state):
        seen.append(get_graph_context().target_node_name)
        return {}

    graph = (GraphManager().add_nodes([finish, proceed])
             .add_node(next_node, "next")
             .compile_graph(["finish", "proceed"]))
    await asyncio.wait_for(graph.invoke({}), 1)
    assert seen == ["next"]


@pytest.mark.parametrize("target", ["work", ["work"], ["work", "peer"]])
@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("steps", [0, 1])
@pytest.mark.asyncio
async def test_restored_target_is_never_replaced_with_graph_entry(target, mode, steps):
    """Recovery resumes the saved target, including zero-step and batch snapshots."""
    calls = []

    def entry(state):
        calls.append("entry")
        return Command(update={"prepared": True}, goto=target)

    def work(state):
        calls.append("work")
        return {"work": True}

    def peer(state):
        calls.append("peer")
        return {"peer": True}

    graph = GraphManager().add_nodes([entry, work, peer]).compile_graph("entry")
    original = graph.create_context({})
    await asyncio.wait_for(graph.invoke(graph_context=original), 1)
    snapshot = original.get_snapshot()
    assert snapshot["target_node_name"] == target
    snapshot["steps"] = steps
    restored = graph.restore_context(snapshot)
    calls.clear()

    async with asyncio.timeout(1):
        if mode == "invoke":
            await graph.invoke(graph_context=restored)
        else:
            async for _ in graph.stream(graph_context=restored):
                pass

    assert calls == ([target] if isinstance(target, str) else target)
    assert restored.steps == steps + 1
    assert restored.status == "finished"
    assert restored.state["prepared"] is True
    assert restored.state["work"] is True


def test_new_contexts_copy_the_entry_batch_independently():
    """Preparing one invocation cannot change another invocation's entry."""
    graph = GraphManager().add_node(lambda state: {}, "work").compile_graph(["work"])
    first = graph.create_context({})
    second = graph.create_context({})
    assert first.target_node_name == second.target_node_name == ["work"]
    first.target_node_name.clear()
    assert second.target_node_name == ["work"]
    assert graph.create_context({}).target_node_name == ["work"]


@pytest.mark.asyncio
async def test_first_dispatch_targets_the_entry_without_a_start_event():
    """Plugins see the actual entry node on the first dispatch."""
    from apixis import subscribe, unsubscribe
    from apixis.core.event.factory import get_event_pipe

    seen = []
    graph = GraphManager().add_node(lambda state: {"done": True}, "work").compile_graph("work")

    @subscribe(graph.dispatch_name, priority=10)
    async def observe(event):
        seen.append((event.context.target_node_name, event.context.steps))

    try:
        assert await asyncio.wait_for(graph.invoke({}), 1) == {"done": True}
        await get_event_pipe().join()
        assert len(seen) == 2
        assert seen[0] == ("work", 0)
        assert seen[1][1] == 1
    finally:
        unsubscribe(observe.__name__)


@pytest.mark.asyncio
async def test_old_start_spelling_is_an_ordinary_node_name():
    """The former start spelling no longer has reserved dispatch behavior."""
    graph = GraphManager().add_node(lambda state: {"executed": True}, "__start__").compile_graph("__start__")
    assert await asyncio.wait_for(graph.invoke({}), 1) == {"executed": True}
