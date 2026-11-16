"""Graphs use the calling thread's event core and namespace ownership."""

import asyncio

from apixis import Command, GraphManager, get_thread_bridge, namespace_set
from apixis.core.event import factory
from apixis.core.graph.base import get_namespace_graphs


async def test_same_namespace_graphs_compile_and_invoke_on_three_cores(thread_cores):
    def compile_graph():
        async def first(state):
            return Command(update={"owner": factory._get_core().thread_id}, goto="second")

        async def second(state):
            return {"result": state["value"] * 2}

        GraphManager().add_nodes([first, second]).compile_graph("first", using_namespace="same")

    compile_graph()
    await thread_cores("graph-a", compile_graph)
    await thread_cores("graph-b", compile_graph)

    async def invoke():
        graph = get_namespace_graphs()["same"]
        assert namespace_set == {"same"}
        return await graph.invoke({"value": 21})

    result = await asyncio.wait_for(get_thread_bridge().call(
        invoke, thread_ids=["main", "graph-a", "graph-b"], background=True,
    ), 5)
    assert result == {
        name: {"value": 21, "owner": name, "result": 42}
        for name in ("main", "graph-a", "graph-b")
    }
    assert get_namespace_graphs()["same"].namespace == "same"


async def test_worker_namespace_replacement_and_cleanup_leave_other_cores_intact(thread_cores):
    def compile_graph():
        GraphManager().compile_graph(None, using_namespace="same")

    compile_graph()
    main_graph = get_namespace_graphs()["same"]
    await thread_cores("graph-worker", compile_graph)

    async def replace_and_decompose():
        old = get_namespace_graphs()["same"]
        replacement = GraphManager().compile_graph(None, using_namespace="same", exist_ok=True)
        assert old._decomposed
        assert await replacement.invoke({"worker": True}) == {"worker": True}
        replacement.decompose()
        assert not namespace_set
        assert not factory.get_handler_registry().registry

    await get_thread_bridge().call(replace_and_decompose, thread_ids="graph-worker", background=True)
    assert get_namespace_graphs()["same"] is main_graph
    assert not main_graph._decomposed
    assert await main_graph.invoke({"main": True}) == {"main": True}
