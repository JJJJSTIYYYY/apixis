"""Ownership boundaries for graph targets and recovery checkpoints."""

import asyncio

import pytest

from apixis.core.graph import GraphManager
from apixis.core.event.factory import get_event_loop


@pytest.mark.parametrize("names", [["first"], ["first", "second"]])
async def test_snapshot_and_restored_targets_have_independent_lists(names):
    """Target lists remain independent across live execution and recovery."""
    manager = GraphManager()
    for name in names:
        manager.add_node(lambda state: {}, name)
    graph = manager.compile_graph(entry_point=names)
    context = graph.create_context({"nested": ["initial"]})
    live_target = context.target_node_name

    await asyncio.wait_for(graph.invoke(graph_context=context), timeout=1)
    snapshot = context.context_snapshot[-1]
    restored = graph.restore_context(context)

    live_target.append("live-only")
    assert snapshot["target_node_name"] == names
    restored.target_node_name.append("restored-only")
    assert restored.context_snapshot[-1]["target_node_name"] == names
    snapshot["target_node_name"].append("snapshot-only")
    assert restored.context_snapshot[-1]["target_node_name"] == names
    restored.state["nested"].append("restored-only")
    assert restored.context_snapshot[-1]["state"] == {"nested": ["initial"]}


async def test_running_batch_preserves_result_order_after_live_target_edit(
    wait_for_dispatch,
):
    """Mutable context targets cannot change an already dispatched batch."""
    started, release_nodes = asyncio.Event(), asyncio.Event()

    async def first(state):
        started.set()
        await release_nodes.wait()
        return {"first": True}

    async def second(state):
        await release_nodes.wait()
        return {"second": True}

    graph = (
        GraphManager()
        .add_nodes([first, second])
        .compile_graph(entry_point=["first", "second"])
    )
    context = graph.create_context({})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        context.target_node_name.append("caller-only")
        release_nodes.set()

        result = await asyncio.wait_for(invocation, timeout=1)

        assert result == {"first": True, "second": True}
        assert context.steps == 1
        assert context.context_snapshot[-1]["target_node_name"] == ["first", "second"]
    finally:
        release_nodes.set()
        if not invocation.done():
            invocation.cancel()
        await asyncio.gather(invocation, return_exceptions=True)
        await wait_for_dispatch(get_event_loop())
