"""Executable example of extending NodeGraph through event subscriptions."""

import pytest

from apixis.core.graph.base import _END

from apixis.core.event import ApixEvent, unsubscribe, subscribe
from apixis.core.event.factory import get_handler_registry
from apixis.core.event.factory import get_event_pipe
from apixis.core.graph import (
    GLOBALNS,
    GraphManager,
    get_graph_dispatch_name,
)
from apixis.core.graph.utils.namespace import get_graph_interrupted_name


GLOBAL_DISPATCH = get_graph_dispatch_name()


pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest.mark.parametrize("namespace", [None, "", "<global>", "plugin-demo"])
async def test_subscribe_inserts_plugin_before_node_graph_listener(namespace):
    """Use the public dispatch name for both subscription and handler ordering."""
    node_name = "plugin_demo_business_node"

    def business_node(state: dict) -> dict:
        """Represent the application node that receives plugin-enriched state."""
        return {
            "pipeline": [*state["pipeline"], "business-node"],
            "plugin_value_seen": state["plugin_value"],
        }

    graph = (
        GraphManager()
        .add_node(business_node, node_name)
        .compile_graph(entry_point=node_name, using_namespace=namespace)
    )

    # A higher priority makes this handler the left boundary of the plugin
    # insertion range; NodeGraph listeners use the default priority of 1.
    @subscribe(graph.dispatch_name, priority=10)
    async def plugin_demo_authentication(event: ApixEvent) -> None:
        if event.context.target_node_name != node_name:
            return
        event.context.state["pipeline"].append("authentication-plugin")

    # A plugin is just another event subscriber. Function names identify the
    # two existing handlers between which it should be inserted.
    @subscribe(
        graph.dispatch_name,
        between_handlers=(
            plugin_demo_authentication.__name__,
            graph.dispatch_name,
        ),
    )
    async def plugin_demo_enrichment(event: ApixEvent) -> None:
        if event.context.target_node_name != node_name:
            return
        state = event.context.state
        state["pipeline"].append("enrichment-plugin")
        state["plugin_value"] = "injected through event plugin"

    handler_names = get_handler_registry().get_handlers_chain_for_event(
        graph.dispatch_name
    )
    assert handler_names == [
        plugin_demo_authentication.__name__,
        plugin_demo_enrichment.__name__,
        graph.dispatch_name,
    ]

    plugin_meta = get_handler_registry().get_handler(plugin_demo_enrichment.__name__)
    assert plugin_meta.between_handlers == (
        plugin_demo_authentication.__name__,
        graph.dispatch_name,
    )
    assert plugin_meta.priority is None

    assert await graph.invoke({"pipeline": []}) == {
        "pipeline": [
            "authentication-plugin",
            "enrichment-plugin",
            "business-node",
        ],
        "plugin_value": "injected through event plugin",
        "plugin_value_seen": "injected through event plugin",
    }
    graph.decompose()
    unsubscribe(plugin_demo_authentication.__name__)
    unsubscribe(plugin_demo_enrichment.__name__)


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("action", ["error", "timeout", "accept", "accept_and_error"])
@pytest.mark.parametrize("target", ["business", "terminal"])
async def test_upstream_plugin_termination_completes_graph(mode, action, target):
    """Public subscriptions must not leave invoke or stream waiting forever."""
    import asyncio


    from apixis.core.graph.context import get_stream_writer
    from apixis.core.utils.exception import GraphNodeError

    called = []

    def business(state):
        called.append("business")
        get_stream_writer()("business chunk")
        return {"value": "updated"}

    graph = (
        GraphManager()
        .add_node(business)
        .compile_graph(entry_point="business", using_namespace=GLOBALNS)
    )
    target_name = _END if target == "terminal" else target
    captured_events = []

    @subscribe(GLOBAL_DISPATCH, priority=10, time_out=0.01)
    async def termination_plugin(event):
        if event.context.target_node_name != target_name:
            return
        captured_events.append(event)
        if action in ("accept", "accept_and_error"):
            event.accept()
        if action in ("error", "accept_and_error"):
            raise ValueError("plugin rejected dispatch")
        if action == "timeout":
            await asyncio.Future()

    chunks = []

    context = graph.create_context({"value": "initial"})

    async def run():
        if mode == "invoke":
            return await graph.invoke(graph_context=context)
        async for chunk in graph.stream(graph_context=context):
            chunks.append(chunk)

    try:
        async with asyncio.timeout(1):
            if action == "accept":
                result = await run()
                assert context.status == "aborted"
                if mode == "invoke":
                    assert result == {"value": "initial"}
            else:
                with pytest.raises(GraphNodeError) as raised:
                    await run()
                assert context.status == "failed"
                [error] = raised.value.errors
                assert error.handler_name == "termination_plugin"
                assert error.exception_type == (
                    "TimeoutError" if action == "timeout" else "ValueError"
                )
        assert len(captured_events) == 1
        assert context.completion.done()
        assert not context.is_active
        assert not graph._contexts
        assert called == (["business"] if target == "terminal" else [])
        if mode == "stream":
            assert chunks == (["business chunk"] if target == "terminal" else [])
    finally:
        unsubscribe(termination_plugin.__name__)
        graph.decompose()


@pytest.mark.parametrize("mode", ["invoke", "stream"])
async def test_graph_dispatch_handles_its_own_snapshot_failure(mode):
    """A failure before node execution completes through on_error."""
    import asyncio
    from unittest.mock import patch

    from apixis.core.graph.context import GraphContext

    called = []

    def business(state):
        called.append("business")
        return state

    graph = (
        GraphManager()
        .add_node(business)
        .compile_graph(entry_point="business")
    )

    context = graph.create_context({})

    async def run():
        if mode == "invoke":
            return await graph.invoke(graph_context=context)
        return [chunk async for chunk in graph.stream(graph_context=context)]

    try:
        with patch.object(
            GraphContext, "take_a_snapshot", side_effect=ValueError("snapshot failed")
        ):
            async with asyncio.timeout(1):
                with pytest.raises(ValueError, match="snapshot failed"):
                    await run()
        assert context.status == "failed"
        assert context.completion.done()
        assert called == []
        assert not graph._contexts
    finally:
        graph.decompose()


async def test_background_plugin_failure_does_not_fail_graph():
    """A failed background plugin cannot suppress the graph dispatch core."""
    import asyncio

    failed = asyncio.Event()

    def business(state):
        return {"completed": True}

    graph = (
        GraphManager()
        .add_node(business)
        .compile_graph(entry_point="business", using_namespace=GLOBALNS)
    )

    @subscribe(GLOBAL_DISPATCH, priority=20, background=True)
    async def background_plugin(event):
        if event.context.target_node_name == "business":
            failed.set()
            raise ValueError("optional background work failed")

    @subscribe(GLOBAL_DISPATCH, priority=10)
    async def wait_for_background_plugin(event):
        await failed.wait()

    context = graph.create_context({})
    try:
        async with asyncio.timeout(1):
            assert await graph.invoke(graph_context=context) == {"completed": True}
        assert context.status == "finished"
    finally:
        unsubscribe(background_plugin.__name__)
        unsubscribe(wait_for_background_plugin.__name__)
        graph.decompose()


@pytest.mark.parametrize(
    "action", ["error", "accept", "accept_and_error", "hook_error", "hook_timeout"]
)
@pytest.mark.parametrize("timeout", [None, 10])
async def test_interruption_hook_termination_unblocks_node(action, timeout):
    """Interruption notifications release the Block as well as the invocation."""
    import asyncio

    from apixis.core.graph.interrupter.graph_interrupter import interrupt
    from apixis.core.utils.exception import GraphNodeError

    resumed = []
    blocks = []
    hook_called = []

    async def business(state):
        await interrupt(timeout=timeout)
        resumed.append(True)
        return state

    graph = (
        GraphManager()
        .add_node(business)
        .compile_graph(entry_point="business", using_namespace=GLOBALNS)
    )

    @subscribe(get_graph_interrupted_name(GLOBALNS, missing_ok=True), priority=10)
    async def interruption_plugin(event):
        blocks.append(event.context)
        if action in ("accept", "accept_and_error"):
            event.accept()
        if action in ("error", "accept_and_error"):
            raise ValueError("upstream interruption failed")

    async def on_interrupted(block):
        hook_called.append(True)
        if action == "hook_timeout":
            raise TimeoutError("hook timeout")
        raise ValueError("hook failed locally")

    graph.add_interrupted_hook(on_interrupted)
    context = graph.create_context({"value": "initial"})
    try:
        async with asyncio.timeout(1):
            if action == "accept":
                assert await graph.invoke(graph_context=context) == {"value": "initial"}
                assert context.status == "aborted"
            else:
                error_type = {
                    "hook_error": ValueError,
                    "hook_timeout": TimeoutError,
                }.get(action, GraphNodeError)
                with pytest.raises(error_type):
                    await graph.invoke(graph_context=context)
                assert context.status == "failed"
        assert resumed == []
        assert hook_called == (
            [True] if action in ("hook_error", "hook_timeout") else []
        )
        assert len(blocks) == 1 and blocks[0].done
        assert blocks[0].cancelled is (action == "accept")
    finally:
        unsubscribe(interruption_plugin.__name__)
        graph.decompose()


@pytest.mark.parametrize("namespace", [None, "", "<global>", "ordered-plugins"])
async def test_dispatch_name_orders_plugins_on_both_sides_of_graph(namespace):
    """Plugins need only the public graph name to run before and after dispatch."""
    calls = []
    observed_events = []

    def business(state):
        calls.append("node")
        return {"done": True}

    graph = (
        GraphManager()
        .add_node(business)
        .compile_graph(entry_point="business", using_namespace=namespace)
    )

    @subscribe(graph.dispatch_name, between_handlers=(None, graph.dispatch_name))
    async def before_dispatch(event):
        calls.append("before")
        observed_events.append(event.event_name)

    @subscribe(graph.dispatch_name, between_handlers=(graph.dispatch_name, None))
    async def after_dispatch(event):
        calls.append("after")
        observed_events.append(event.event_name)

    try:
        assert await graph.invoke({}) == {"done": True}
        await get_event_pipe().join()
        node_index = calls.index("node")
        assert calls[node_index - 1 : node_index + 2] == ["before", "node", "after"]
        assert calls.count("before") == calls.count("after")
        assert set(observed_events) == {graph.dispatch_name}
    finally:
        unsubscribe(before_dispatch.__name__)
        unsubscribe(after_dispatch.__name__)


async def test_wildcard_plugin_observes_global_and_named_graphs_created_later():
    """Namespace-only subscriptions can be registered before graphs exist."""
    observed = []

    @subscribe(get_graph_dispatch_name("*"), priority=10)
    async def observe_all_graphs(event):
        observed.append(event.event_name)

    def business(state):
        return {"done": True}

    try:
        names = []
        for namespace in (GLOBALNS, "named-plugin-graph"):
            graph = (
                GraphManager()
                .add_node(business)
                .compile_graph(entry_point="business", using_namespace=namespace)
            )
            names.append(graph.dispatch_name)
            assert await graph.invoke({}) == {"done": True}
        assert set(observed) == set(names)
    finally:
        unsubscribe(observe_all_graphs.__name__)
