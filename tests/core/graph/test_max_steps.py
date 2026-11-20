"""Public execution contracts at the graph's node-batch budget boundary."""

import asyncio

import pytest


from apixis.core.graph import Command, GraphManager, get_stream_writer


pytestmark = pytest.mark.asyncio(loop_scope="session")


async def run_graph(graph, context, mode):
    """Bound invocation time and collect the selected public execution API."""
    async with asyncio.timeout(1):
        if mode == "invoke":
            return await graph.invoke(graph_context=context)
        return [chunk async for chunk in graph.stream(graph_context=context)]


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("limit", [1, 3])
@pytest.mark.parametrize("ending", ["implicit", "empty"])
async def test_last_allowed_node_can_finish(mode, limit, ending):
    """Every supported terminal route succeeds after exactly the allowed work."""
    calls = []

    def work(state):
        count = state["count"] + 1
        calls.append(count)
        get_stream_writer()(count)
        if count < limit:
            return Command(update={"count": count}, goto="work")
        target = {"empty": []}.get(ending)
        return Command(update={"count": count}, goto=target)

    manager = GraphManager().add_node(work)
    graph = manager.compile_graph(entry_point="work").set_max_steps(limit)
    context = graph.create_context({"count": 0})

    result = await run_graph(graph, context, mode)

    assert calls == list(range(1, limit + 1))
    assert context.status == "finished"
    assert context.steps == limit
    assert context.state == {"count": limit}
    assert result == ({"count": limit} if mode == "invoke" else calls)


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("target", ["extra", ["extra", "other"]])
async def test_exhausted_budget_prevents_next_node_side_effects(mode, target):
    """Remaining work must respect the execution budget."""
    calls = []

    def first(state):
        calls.append("first")
        return Command(update={"committed": True}, goto=target)

    def extra(state):
        calls.append("extra")
        return {}

    def other(state):
        calls.append("other")
        return {}

    graph = (
        GraphManager().add_nodes([first, extra, other]).compile_graph(entry_point="first").set_max_steps(1)
    )
    context = graph.create_context({})

    with pytest.raises(RecursionError, match="maximum of 1 steps"):
        await run_graph(graph, context, mode)

    assert calls == ["first"]
    assert context.status == "failed"
    assert context.steps == 1
    assert context.state == {"committed": True}


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("target", [None, []])
async def test_parallel_batch_can_finish_at_the_limit(mode, target):
    """A final concurrent batch costs one step regardless of its node count."""
    calls = []
    both_started = asyncio.Event()

    def route(state):
        return Command(goto=["left", "right"])

    async def branch(name):
        calls.append(name)
        if len(calls) == 2:
            both_started.set()
        await both_started.wait()
        return Command(update={name: True}, goto=target)

    async def left(state):
        return await branch("left")

    async def right(state):
        return await branch("right")

    graph = (
        GraphManager().add_nodes([route, left, right]).compile_graph(entry_point="route").set_max_steps(2)
    )
    context = graph.create_context({})

    result = await run_graph(graph, context, mode)

    assert sorted(calls) == ["left", "right"]
    assert context.steps == 2
    assert context.status == "finished"
    assert context.state == {"left": True, "right": True}
    assert result == (context.state if mode == "invoke" else [])


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_restored_context_uses_only_its_remaining_steps(mode):
    """Restoring the last checkpoint preserves its budget and permits ending."""
    calls = []

    def work(state):
        count = state["count"] + 1
        calls.append(count)
        return Command(update={"count": count}, goto="work" if count < 2 else None)

    graph = (
        GraphManager().add_node(work)
        .compile_graph(entry_point="work").set_max_steps(2)
    )
    original = graph.create_context({"count": 0})
    await run_graph(graph, original, "invoke")
    restored = graph.restore_context(original)
    assert restored.steps == 1
    calls.clear()

    await run_graph(graph, restored, mode)

    assert calls == [2]
    assert restored.steps == 2
    assert restored.status == "finished"
    assert restored.state == {"count": 2}
