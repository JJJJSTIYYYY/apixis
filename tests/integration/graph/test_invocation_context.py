"""Invocation context propagation through graph events and plugin handlers."""

import asyncio
from contextvars import ContextVar

import pytest

from apixis.core.event import subscribe
from apixis.core.event.base import handler_chain_context
from apixis.core.event.factory import get_event_loop
from apixis.core.graph import Command, GraphManager, ParallelNode, get_stream_writer
from apixis.core.graph.context import (
    apix_graph_context,
    get_current_run_id,
    get_graph_context,
)
from apixis.core.graph.interrupter.graph_interrupter import interrupt


_request_id = ContextVar("invocation_context_test_request_id", default=None)


@pytest.fixture(params=[1, 2])
def event_runtime_capacity(request):
    return request.param


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_caller_context_is_available_in_every_node_and_plugin(mode, wait_for_dispatch):
    """Caller bindings reach sequential nodes, concurrent branches, and plugins."""
    request = object()
    nodes = []
    plugins = {phase: [] for phase in ("before", "after", "background")}
    background_done = asyncio.Event()

    def observe_node(name):
        assert _request_id.get() is request
        nodes.append(name)
        get_stream_writer()(name)

    def start(state):
        observe_node("start")
        return Command(goto=["left", "right"])

    async def left(state):
        await asyncio.sleep(0)
        observe_node("left")
        return {}

    def sync_branch(state):
        observe_node("sync_branch")
        return {}

    async def async_branch(state):
        await asyncio.sleep(0)
        observe_node("async_branch")
        return {}

    graph = (
        GraphManager().add_node(start).add_node(left)
        .add_node(ParallelNode([sync_branch, async_branch], name="right"))
        .compile_graph(entry_point="start")
    )

    def observe_plugin(phase, event):
        assert _request_id.get() is request
        assert get_graph_context() is event.context
        plugins[phase].append(event.event_id)

    @subscribe(graph.dispatch_name, between_handlers=(None, graph.dispatch_name))
    async def before(event):
        observe_plugin("before", event)

    @subscribe(graph.dispatch_name, between_handlers=(graph.dispatch_name, None))
    async def after(event):
        observe_plugin("after", event)

    @subscribe(graph.dispatch_name, background=True)
    async def background(event):
        observe_plugin("background", event)
        if len(plugins["background"]) == 3:
            background_done.set()

    # Set only at the public call site, without passing the value through state.
    token = _request_id.set(request)
    try:
        async with asyncio.timeout(3):
            if mode == "invoke":
                assert await graph.invoke({}) == {}
            else:
                chunks = []
                async for chunk in graph.stream({}):
                    assert _request_id.get() is request
                    chunks.append(chunk)
                assert sorted(chunks) == ["async_branch", "left", "start", "sync_branch"]
            await background_done.wait()
            await wait_for_dispatch(get_event_loop())
        assert _request_id.get() is request
    finally:
        _request_id.reset(token)

    assert sorted(nodes) == ["async_branch", "left", "start", "sync_branch"]
    assert len(plugins["before"]) == 3
    assert set(plugins["before"]) == set(plugins["after"]) == set(plugins["background"])
    assert _request_id.get() is None


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_concurrent_invocations_share_context_with_plugins(mode, wait_for_dispatch):
    observations = []
    chains = {}
    background_finished = {name: asyncio.Event() for name in ("left", "right")}

    def observe(phase, context):
        assert get_graph_context() is context
        assert get_current_run_id() == context.run_id
        assert _request_id.get() == context.state["request"]
        observations.append((context.state["request"], phase))

    async def first(state):
        observe("first", get_graph_context())
        await asyncio.sleep(0)
        get_stream_writer()("first")
        return Command(goto="second")

    def second(state):
        observe("second", get_graph_context())
        get_stream_writer()("second")
        return {"done": True}

    graph = (
        GraphManager().add_node(first).add_node(second)
        .compile_graph(entry_point="first")
    )

    @subscribe(graph.dispatch_name, between_handlers=(None, graph.dispatch_name))
    async def before(event):
        observe("before", event.context)
        chain = handler_chain_context.get()
        assert chain is not None and chain.held
        chains[event.event_id] = chain
        get_stream_writer()("before")

    @subscribe(graph.dispatch_name, between_handlers=(graph.dispatch_name, None))
    async def after(event):
        observe("after", event.context)
        assert handler_chain_context.get() is chains[event.event_id]
        get_stream_writer()("after")

    @subscribe(graph.dispatch_name, background=True)
    async def background(event):
        observe("background", event.context)
        assert handler_chain_context.get() is None
        if not event.context.is_active:
            background_finished[event.context.state["request"]].set()

    # An enclosing graph binding must survive both invoke and stream iteration.
    outer = GraphManager().compile_graph(entry_point=None).create_context({})

    async def run(name):
        token = _request_id.set(name)
        try:
            context = graph.create_context({"request": name})
            with apix_graph_context(outer):
                if mode == "invoke":
                    assert await graph.invoke(graph_context=context) == {
                        "request": name, "done": True,
                    }
                else:
                    chunks = []
                    async for chunk in graph.stream(graph_context=context):
                        assert get_graph_context() is outer
                        assert _request_id.get() == name
                        chunks.append(chunk)
                    assert chunks.count("first") == chunks.count("second") == 1
                    assert "before" in chunks and "after" in chunks
                assert get_graph_context() is outer
                assert _request_id.get() == name
            with pytest.raises(RuntimeError, match="only available"):
                get_graph_context()
            await background_finished[name].wait()
        finally:
            _request_id.reset(token)

    async with asyncio.timeout(3):
        await asyncio.gather(run("left"), run("right"))
        await wait_for_dispatch(get_event_loop())
    for name in ("left", "right"):
        phases = [phase for owner, phase in observations if owner == name]
        assert phases.count("before") == phases.count("after") == 3
        assert phases.count("background") == 3
        assert phases.count("first") == phases.count("second") == 1
    assert len({id(chain) for chain in chains.values()}) == 6
    assert _request_id.get() is None


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_nested_graph_and_interruption_inherit_context(mode, wait_for_dispatch):
    observed = []

    def child_node(state):
        assert _request_id.get() == "request"
        assert get_graph_context().graph_id == child.graph_id
        observed.append("child")
        return {"child": True}

    child = GraphManager().add_node(child_node).compile_graph(entry_point="child_node")

    async def parent_node(state):
        context = get_graph_context()
        assert context.graph_id == parent.graph_id
        assert await child.invoke({}) == {"child": True}
        assert get_graph_context() is context
        assert await interrupt(data="question") == "answer"
        assert get_graph_context() is context
        return {"done": True}

    parent = GraphManager().add_node(parent_node).compile_graph(entry_point="parent_node")

    async def hook(block):
        assert get_graph_context().graph_id == parent.graph_id
        assert get_current_run_id() == block.run_id
        assert _request_id.get() == "request"
        get_stream_writer()("interrupted")
        observed.append("hook")
        block.resolve("answer")

    parent.add_interrupted_hook(hook)
    token = _request_id.set("request")
    try:
        async with asyncio.timeout(3):
            if mode == "invoke":
                assert await parent.invoke({}) == {"done": True}
            else:
                assert [chunk async for chunk in parent.stream({})] == ["interrupted"]
            await wait_for_dispatch(get_event_loop())
        assert observed == ["child", "hook"]
        with pytest.raises(RuntimeError, match="only available"):
            get_graph_context()
    finally:
        _request_id.reset(token)


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("outcome", ["error", "cancel"])
async def test_invocation_restores_enclosing_context_after_failure(mode, outcome):
    started = asyncio.Event()

    async def node(state):
        assert get_graph_context().graph_id == graph.graph_id
        assert _request_id.get() == "request"
        started.set()
        if outcome == "error":
            raise ValueError("node failed")
        await asyncio.Future()

    graph = GraphManager().add_node(node).compile_graph(entry_point="node")
    outer = GraphManager().compile_graph(entry_point=None).create_context({})

    async def run():
        token = _request_id.set("request")
        try:
            with apix_graph_context(outer):
                try:
                    if mode == "invoke":
                        await graph.invoke({})
                    else:
                        async for _ in graph.stream({}):
                            pass
                finally:
                    assert get_graph_context() is outer
                    assert _request_id.get() == "request"
        finally:
            _request_id.reset(token)

    async with asyncio.timeout(3):
        task = asyncio.create_task(run())
        await started.wait()
        if outcome == "cancel":
            task.cancel()
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else ValueError):
            await task
    with pytest.raises(RuntimeError, match="only available"):
        get_graph_context()
    assert _request_id.get() is None
