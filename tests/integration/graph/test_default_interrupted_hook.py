"""Every compiled graph owns interruption cleanup and a missing-hook fallback."""

import asyncio

import pytest

from apixis.core.event.factory import get_event_pipe, get_handler_registry
from apixis.core.event import get_handler, subscribe, unsubscribe
from apixis.core.graph import GLOBALNS, GraphManager
from apixis.core.graph.interrupter import interrupt, interrupted_hook
from apixis.core.graph.utils.namespace import get_graph_interrupted_name
from apixis.core.utils import BlockHookNotRegisteredError, BlockNotResolvedError
from apixis.core.utils.exception import GraphNodeError


async def run_graph(graph, context, mode):
    if mode == "invoke":
        return await graph.invoke(graph_context=context)
    return [chunk async for chunk in graph.stream(graph_context=context)]


@pytest.mark.parametrize("namespace", [None, GLOBALNS, "named-review"])
@pytest.mark.parametrize("name", [None, "custom_review_hook"])
async def test_owned_hook_name_is_public_and_distinct_from_fallback(namespace, name):
    """User hook lookup and cleanup never target the internal fallback."""
    async def review(state):
        return {"answer": await interrupt()}

    async def capture(block):
        block.resolve("approved")

    graph = (GraphManager().add_node(review)
             .compile_graph(entry_point="review", using_namespace=namespace))
    event_name = get_graph_interrupted_name(graph)
    registry = get_handler_registry()
    [fallback_name] = registry.get_handlers_chain_for_event(event_name)
    fallback = get_handler(fallback_name)
    assert fallback_name != event_name
    assert get_handler(event_name) is None

    handler_name = name or event_name
    assert graph.add_interrupted_hook(capture, name=name) is capture
    assert capture.__name__ == "capture"
    assert get_handler(handler_name) is not None
    assert get_handler(handler_name) is not fallback
    assert get_handler(fallback_name) is fallback
    assert registry.get_handlers_chain_for_event(event_name) == [
        handler_name, fallback_name,
    ]

    async with asyncio.timeout(1):
        assert await graph.invoke({}) == {"answer": "approved"}
        await get_event_pipe().join()
    graph.decompose()
    assert get_handler(handler_name) is None
    assert get_handler(fallback_name) is None
    assert registry.get_handlers_chain_for_event(event_name) == []


async def test_default_owned_hook_can_reuse_callback_across_graphs():
    """Default names follow graph namespaces even for a shared callback."""
    async def review(state):
        return {"answer": await interrupt()}

    async def capture(block):
        block.resolve(block.namespace)

    manager = GraphManager().add_node(review)
    first = manager.compile_graph(entry_point="review", using_namespace="first-review")
    second = manager.compile_graph(entry_point="review", using_namespace="second-review")
    first.add_interrupted_hook(capture)
    second.add_interrupted_hook(capture)

    assert capture.__name__ == "capture"
    async with asyncio.timeout(1):
        assert await first.invoke({}) == {"answer": "first-review"}
        await get_event_pipe().join()
    first.decompose()
    assert get_handler(get_graph_interrupted_name(first)) is None
    assert get_handler(get_graph_interrupted_name(second)) is not None
    async with asyncio.timeout(1):
        assert await second.invoke({}) == {"answer": "second-review"}
        await get_event_pipe().join()
    second.decompose()
    assert get_handler(get_graph_interrupted_name(second)) is None


async def test_public_hook_name_can_be_registered_before_graph_compilation():
    """A preexisting user hook does not collide with fallback registration."""
    namespace = "preexisting-review"
    event_name = get_graph_interrupted_name(namespace)

    async def capture(block):
        block.resolve("approved")

    capture.__name__ = event_name
    interrupted_hook(namespace, exist_ok=False)(capture)
    user_handler = get_handler(event_name)

    async def review(state):
        return {"answer": await interrupt()}

    graph = (GraphManager().add_node(review)
             .compile_graph(entry_point="review", using_namespace=namespace))
    assert get_handler(event_name) is user_handler
    async with asyncio.timeout(1):
        assert await graph.invoke({}) == {"answer": "approved"}
        await get_event_pipe().join()
    graph.decompose()
    assert get_handler(event_name) is user_handler
    unsubscribe(event_name)


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("namespace", [None, GLOBALNS, "missing-hook"])
@pytest.mark.parametrize("timeout", [None, 10])
async def test_missing_hook_fails_without_waiting_for_timeout(mode, namespace, timeout):
    continued = []

    async def review(state):
        await interrupt(data="review", timeout=timeout)
        continued.append(True)
        return state

    graph = (GraphManager().add_node(review)
             .compile_graph(entry_point="review", using_namespace=namespace))
    context = graph.create_context({})
    try:
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError, match=graph.namespace):
                await run_graph(graph, context, mode)
            await get_event_pipe().join()
        assert context.status == "failed"
        assert context.completion.done()
        assert continued == []
    finally:
        graph.decompose()


@pytest.mark.parametrize("registration", ["owned", "standalone_before", "standalone_after"])
@pytest.mark.parametrize("accepted", [True, False])
async def test_deferred_hook_must_accept_its_block(registration, accepted):
    blocks = asyncio.Queue()
    namespace = "deferred-review"

    async def capture(block):
        if accepted:
            block.accept()
        await blocks.put(block)

    async def review(state):
        return {"answer": await interrupt()}

    if registration == "standalone_before":
        interrupted_hook(namespace)(capture)
    graph = (GraphManager().add_node(review)
             .compile_graph(entry_point="review", using_namespace=namespace))
    if registration == "owned":
        graph.add_interrupted_hook(capture)
    elif registration == "standalone_after":
        interrupted_hook(namespace)(capture)
    handler_name = (
        get_graph_interrupted_name(graph) if registration == "owned" else capture.__name__
    )

    context = graph.create_context({})
    task = asyncio.create_task(graph.invoke(graph_context=context))
    try:
        async with asyncio.timeout(1):
            block = await blocks.get()
            # Allow the default handler to run after capture() has returned.
            await asyncio.sleep(0)
            assert block.accepted is accepted
            if accepted:
                assert not block.done
                assert not task.done()
                block.resolve("approved")
                assert await task == {"answer": "approved"}
            else:
                with pytest.raises(BlockNotResolvedError, match=namespace):
                    await task
                assert block.done
                assert context.status == "failed"
            await get_event_pipe().join()
        unsubscribe(handler_name)
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError):
                await graph.invoke({})
            await get_event_pipe().join()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        unsubscribe(handler_name)
        graph.decompose()


@pytest.mark.parametrize("subscription", ["exact", "wildcard"])
@pytest.mark.parametrize("unregister_after_entry", [False, True])
@pytest.mark.parametrize("accepted", [True, False])
async def test_plain_observer_must_accept_block_for_deferred_resolution(subscription, unregister_after_entry, accepted):
    """An observer must accept its block even when it unregisters after entry."""
    events = asyncio.Queue()

    async def review(state):
        return {"answer": await interrupt()}

    graph = GraphManager().add_node(review).compile_graph(entry_point="review")

    event_name = get_graph_interrupted_name(graph.namespace, missing_ok=True)
    [fallback_name] = get_handler_registry().get_handlers_chain_for_event(event_name)
    pattern = event_name if subscription == "exact" else get_graph_interrupted_name("*", missing_ok=True)

    @subscribe(pattern, priority=10)
    async def observe(event):
        if accepted:
            event.context.accept()
        await events.put(event)
        if unregister_after_entry:
            unsubscribe(observe.__name__)

    task = asyncio.create_task(graph.invoke({}))
    try:
        async with asyncio.timeout(1):
            event = await events.get()
            # Interruption dispatch finishes while the graph still awaits its Block.
            await asyncio.sleep(0)
            assert event.seen == [observe.__name__, fallback_name]
            assert event.context.accepted is accepted
            if accepted:
                assert not event.context.done
                assert not task.done()
                event.context.resolve("approved")
                assert await task == {"answer": "approved"}
            else:
                with pytest.raises(BlockNotResolvedError, match=graph.namespace):
                    await task
                assert event.context.done
            await get_event_pipe().join()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        unsubscribe(observe.__name__)
        graph.decompose()


async def test_filtered_hook_does_not_suppress_fallback():
    """A matching registration alone is insufficient when its core has not run."""
    called = []

    async def review(state):
        await interrupt()
        return state

    graph = GraphManager().add_node(review).compile_graph(entry_point="review")
    event_name = get_graph_interrupted_name(graph.namespace, missing_ok=True)

    @interrupted_hook(graph.namespace)
    async def capture(block):
        called.append(block)

    handler = get_handler(capture.__name__)
    handler.register(
        event_name,
        priority=10,
        filter_event=[event_name],
    )
    try:
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError):
                await graph.invoke({})
            await get_event_pipe().join()
        assert called == []
    finally:
        handler.unregister()
        graph.decompose()


@pytest.mark.parametrize("priority", [0, -1, -9999])
@pytest.mark.parametrize("register_before_graph", [False, True])
@pytest.mark.parametrize("resolve", [False, True])
async def test_default_validation_runs_after_low_priority_hooks(
    priority, register_before_graph, resolve,
):
    """Public-priority hooks run before validation regardless of registration order."""
    namespace = "low-priority-interruption"
    event_name = get_graph_interrupted_name(namespace, missing_ok=True)
    called = []

    async def review(state):
        return {"answer": await interrupt()}

    async def capture(event):
        called.append(event.context)
        if resolve:
            event.context.resolve("approved")

    if register_before_graph:
        subscribe(event_name, priority=priority)(capture)
    graph = (GraphManager().add_node(review)
             .compile_graph(entry_point="review", using_namespace=namespace))
    if not register_before_graph:
        subscribe(event_name, priority=priority)(capture)

    try:
        async with asyncio.timeout(1):
            if resolve:
                assert await graph.invoke({}) == {"answer": "approved"}
            else:
                with pytest.raises(BlockNotResolvedError):
                    await graph.invoke({})
            await get_event_pipe().join()
        assert len(called) == 1
        assert called[0].done
    finally:
        unsubscribe(capture.__name__)
        graph.decompose()


async def test_simultaneous_graphs_keep_seen_and_fallback_independent():
    """A handled interruption in one graph cannot mask a missing hook in another."""
    async def review(state):
        return {"answer": await interrupt()}

    manager = GraphManager().add_node(review)
    first = manager.compile_graph(entry_point="review", using_namespace="seen-first")
    second = manager.compile_graph(entry_point="review", using_namespace="seen-second")

    @first.add_interrupted_hook
    async def resolve(block):
        block.resolve("approved")

    try:
        async with asyncio.timeout(1):
            results = await asyncio.gather(first.invoke({}), second.invoke({}), return_exceptions=True)
            await get_event_pipe().join()
        assert results[0] == {"answer": "approved"}
        assert isinstance(results[1], BlockHookNotRegisteredError)
    finally:
        first.decompose()
        second.decompose()


@pytest.mark.parametrize("mode", ["invoke", "stream"])
@pytest.mark.parametrize("action", ["error", "accept", "cancel"])
async def test_default_handles_upstream_termination_without_user_hook(mode, action):
    blocks = []
    continued = []

    async def review(state):
        await interrupt()
        continued.append(True)
        return state

    graph = GraphManager().add_node(review).compile_graph(entry_point="review")

    @subscribe(get_graph_interrupted_name(graph.namespace, missing_ok=True), priority=10)
    async def plugin(event):
        blocks.append(event.context)
        if action == "error":
            raise ValueError("plugin failed")
        if action == "cancel":
            raise asyncio.CancelledError()
        event.accept()

    context = graph.create_context({"value": "initial"})
    try:
        async with asyncio.timeout(1):
            if action == "accept":
                result = await run_graph(graph, context, mode)
                assert result == ({"value": "initial"} if mode == "invoke" else [])
            else:
                error = GraphNodeError if action == "error" else asyncio.CancelledError
                with pytest.raises(error):
                    await run_graph(graph, context, mode)
            await get_event_pipe().join()
        assert context.status == ("failed" if action == "error" else "aborted")
        assert context.completion.cancelled() is (action == "cancel")
        assert len(blocks) == 1 and blocks[0].done
        assert continued == []
    finally:
        unsubscribe(plugin.__name__)
        graph.decompose()


async def test_node_can_recover_from_missing_hook_error():
    async def review(state):
        try:
            await interrupt()
        except BlockHookNotRegisteredError:
            return {"review_skipped": True}

    graph = GraphManager().add_node(review).compile_graph(entry_point="review")
    try:
        async with asyncio.timeout(1):
            assert await graph.invoke({}) == {"review_skipped": True}
            await get_event_pipe().join()
    finally:
        graph.decompose()


async def test_default_registration_collision_releases_partial_graph_and_namespace():
    event_name = get_graph_interrupted_name("collision", missing_ok=True)
    manager = GraphManager()
    probe = manager.compile_graph(entry_point=None, using_namespace="collision")
    [fallback_name] = get_handler_registry().get_handlers_chain_for_event(event_name)
    probe.decompose()

    async def unrelated(event):
        pass

    # Occupy the fallback handler's internal name to force the second
    # listener registration to fail after graph dispatch has been registered.
    unrelated.__name__ = fallback_name
    subscribe(event_name)(unrelated)
    try:
        from apixis.core.utils.exception import EventHandlerAlreadyRegisteredError
        with pytest.raises(EventHandlerAlreadyRegisteredError):
            manager.compile_graph(entry_point=None, using_namespace="collision")
        assert get_handler(fallback_name).core_func is unrelated
    finally:
        unsubscribe(fallback_name)

    graph = manager.compile_graph(entry_point=None, using_namespace="collision")
    async with asyncio.timeout(1):
        assert await graph.invoke({}) == {}
    graph.decompose()


async def test_decomposition_removes_default_and_namespace_reuse_restores_it():
    async def review(state):
        await interrupt()
        return state

    manager = GraphManager().add_node(review)
    old = manager.compile_graph(entry_point="review", using_namespace="reused-default")

    @old.add_interrupted_hook
    async def resolve(block):
        block.resolve("old answer")

    old.decompose()
    graph = manager.compile_graph(entry_point="review", using_namespace="reused-default")
    try:
        async with asyncio.timeout(1):
            with pytest.raises(BlockHookNotRegisteredError):
                await graph.invoke({})
            await get_event_pipe().join()
    finally:
        graph.decompose()
