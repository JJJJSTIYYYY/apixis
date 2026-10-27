"""Interactions between graph batches, ParallelNode, and GraphContext."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from apixis.core.graph.base import _END

from apixis.core.graph import AutoMerge, Command, GraphManager, ParallelNode
from apixis.core.graph.context import GraphContext, get_graph_context


pytestmark = pytest.mark.asyncio(loop_scope="session")


class HistoryState(TypedDict, total=False):
    history: Annotated[list[str], AutoMerge()]


async def test_graph_batch_containing_parallel_node_keeps_both_orderings():
    """Parallel branches and graph nodes each retain declaration order."""
    first_branch_started = asyncio.Event()
    second_branch_finished = asyncio.Event()
    regular_finished = asyncio.Event()
    observations: dict[str, bool] = {}

    async def first_branch(state):
        state["parallel_local"] = True
        first_branch_started.set()
        await second_branch_finished.wait()
        await regular_finished.wait()
        return Command(update={"history": ["parallel-1"]}, goto=[])

    async def second_branch(state):
        await first_branch_started.wait()
        observations["parallel_branch_shared_state"] = state["parallel_local"]
        second_branch_finished.set()
        return Command(
            update={"history": ["parallel-2"]},
            goto="parallel_done",
        )

    async def regular(state):
        await first_branch_started.wait()
        observations["graph_node_state_isolated"] = "parallel_local" not in state
        regular_finished.set()
        return Command(update={"history": ["regular"]}, goto="regular_done")

    def parallel_done(state):
        return Command(update={"history": ["parallel-done"]}, goto=[])

    def regular_done(state):
        return Command(update={"history": ["regular-done"]}, goto=[])

    parallel = ParallelNode(
        [first_branch, second_branch],
        name="parallel",
    )
    graph = (
        GraphManager(HistoryState)
        .add_node(lambda state: Command(goto=["parallel", "regular"]), "launch")
        .add_nodes(
            [
                parallel,
                regular,
                parallel_done,
                regular_done,
            ]
        )
        .compile_graph(entry_point="launch")
    )

    result = await graph.invoke({"history": []})

    assert result == {
        "history": [
            "parallel-1",
            "parallel-2",
            "regular",
            "parallel-done",
            "regular-done",
        ]
    }
    assert observations == {
        "parallel_branch_shared_state": True,
        "graph_node_state_isolated": True,
    }


async def test_parallel_node_is_one_graph_node_for_conflict_detection():
    """Its own commands may overwrite, but a graph sibling still conflicts."""
    parallel = ParallelNode(
        [
            lambda state: {"ordinary": "branch-1"},
            lambda state: {"ordinary": "branch-2"},
        ],
        name="parallel",
    )
    graph = (
        GraphManager()
        .add_node(lambda state: Command(goto=["parallel", "regular"]), "launch")
        .add_node(parallel)
        .add_node(lambda state: {"ordinary": "regular"}, "regular")
        .compile_graph(entry_point="launch")
    )

    context = graph.create_context({})
    with pytest.raises(ValueError, match="non-AutoMerge state field `ordinary`"):
        await graph.invoke(graph_context=context)

    assert context.state == {"ordinary": "branch-2"}
    assert context.get_snapshot()["state"] == {}
    assert context.get_snapshot()["target_node_name"] == ["parallel", "regular"]


async def test_concurrent_invocations_bind_batches_to_their_own_contexts():
    """A shared graph keeps context identity and state isolated per invoke."""
    observed_contexts: dict[str, list[GraphContext]] = {
        "first": [],
        "second": [],
    }
    both_invocations_started = asyncio.Event()
    started = 0
    started_lock = asyncio.Lock()

    async def member(state):
        nonlocal started
        label = state["label"]
        observed_contexts[label].append(get_graph_context())
        async with started_lock:
            started += 1
            if started == 4:
                both_invocations_started.set()
        await both_invocations_started.wait()
        return Command(
            update={"history": [state["member"]]},
            goto=[],
        )

    async def left(state):
        state["member"] = f"{state['label']}-left"
        return await member(state)

    async def right(state):
        state["member"] = f"{state['label']}-right"
        return await member(state)

    graph = (
        GraphManager(HistoryState)
        .add_node(lambda state: Command(goto=["left", "right"]), "launch")
        .add_nodes([left, right])
        .compile_graph(entry_point="launch")
    )

    contexts = {
        label: graph.create_context({"label": label, "history": []})
        for label in ("first", "second")
    }

    first_result, second_result = await asyncio.gather(
        graph.invoke(graph_context=contexts["first"]),
        graph.invoke(graph_context=contexts["second"]),
    )

    assert first_result["history"] == ["first-left", "first-right"]
    assert second_result["history"] == ["second-left", "second-right"]
    assert contexts["first"].run_id != contexts["second"].run_id
    for label, context in contexts.items():
        assert observed_contexts[label] == [context, context]
        assert context.target_node_name == _END
        assert context.steps == 2
        assert context.get_snapshot()["target_node_name"] == ["left", "right"]


async def test_failed_batch_recovers_from_graph_context_snapshot():
    """Partial live writes are discarded when the complete batch is retried."""
    fail_with_conflict = True
    calls = {"a": 0, "b": 0}

    def a(state):
        calls["a"] += 1
        return {"shared": "a", "a": True}

    def b(state):
        calls["b"] += 1
        if fail_with_conflict:
            return {"shared": "b"}
        return {"b": True}

    graph = (
        GraphManager()
        .add_node(
            lambda state: Command(update={"ready": True}, goto=["a", "b"]),
            "launch",
        )
        .add_nodes([a, b])
        .compile_graph(entry_point="launch")
    )

    failed = graph.create_context({})
    with pytest.raises(ValueError, match="non-AutoMerge state field `shared`"):
        await graph.invoke(graph_context=failed)

    assert failed.state == {"ready": True, "shared": "a", "a": True}
    assert failed.steps == 1
    assert failed.get_snapshot()["state"] == {"ready": True}
    assert failed.get_snapshot()["target_node_name"] == ["a", "b"]

    fail_with_conflict = False
    recovered = graph.restore_context(failed.context_snapshot)
    result = await graph.invoke(graph_context=recovered)

    assert result == {"ready": True, "shared": "a", "a": True, "b": True}
    assert recovered.steps == 2
    assert calls == {"a": 2, "b": 2}
