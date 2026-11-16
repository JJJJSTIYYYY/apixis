"""Graph publications are visible only on the core selected by the listener."""

import asyncio
from concurrent.futures import Future

import pytest

from apixis import (
    ApixEventPipe, GraphManager, expect_event, expect_event_threadsafe,
    get_event_registry, get_event_runtime_status, get_thread_bridge, post_and_wait_threadsafe,
    post_event, post_event_threadsafe, subscribe,
)
from apixis.core.event import factory
from apixis.core.graph.base import get_namespace_graphs


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    return 1


@pytest.mark.parametrize("point", ["received", "processed"])
@pytest.mark.parametrize("publication_target, caller, listening_target", [
    (None, "main", "graph"),
    ("receiver", "receiver", None),
    ("receiver", "receiver", "receiver"),
    ("receiver", "receiver", "graph"),
    ("receiver", "receiver", "main"),
    ("receiver", "main", "receiver"),
])
async def test_graph_publication_and_listener_core_must_match(
    thread_cores, point, publication_target, caller, listening_target,
):
    ready, published = Future(), Future()
    payload = {"value": 42}

    def compile_graph():
        async def publish(state):
            if publication_target is None:
                await post_event(event_name="graph.message", context=payload)
            else:
                await post_event_threadsafe(
                    event_name="graph.message", context=payload, thread_ids=publication_target,
                )
            return {"published": True}

        GraphManager().add_node(publish).compile_graph("publish", using_namespace="sender")

    await thread_cores("graph", compile_graph)
    await thread_cores("receiver")
    baseline_listeners = await get_thread_bridge().call_sync(
        lambda: get_event_runtime_status().wait_listeners,
        thread_ids=["main", "graph", "receiver"],
    )
    actual_target = publication_target or "graph"
    listener_core = listening_target or caller
    should_receive = actual_target == listener_core

    async def collect():
        context = (
            expect_event("graph.message", point=point)
            if listening_target is None else
            expect_event_threadsafe("graph.message", point=point, thread_ids=listening_target)
        )
        async with context as waiter:
            ready.set_result(None)
            # Let the graph publish before testing the negative case's deadline.
            await asyncio.wrap_future(published)
            if should_receive:
                return await waiter.wait(time_out=2)
            with pytest.raises(TimeoutError):
                await waiter.wait(time_out=0.01)

    collection = asyncio.create_task(get_thread_bridge().call(
        collect, thread_ids=caller, background=True,
    ))

    async def invoke():
        return await get_namespace_graphs()["sender"].invoke({})

    try:
        await asyncio.wait_for(asyncio.wrap_future(ready), 3)
        assert await asyncio.wait_for(get_thread_bridge().call(
            invoke, thread_ids="graph", background=True,
        ), 3) == {"published": True}
        await post_and_wait_threadsafe(event_name="drained", thread_ids=actual_target)
        observations = await get_thread_bridge().call_sync(
            lambda: "graph.message" in get_event_registry().get_registered_events(),
            thread_ids=["main", "graph", "receiver"],
        )
        assert observations == {
            core: core == actual_target for core in ("main", "graph", "receiver")
        }
        published.set_result(None)
        event = await asyncio.wait_for(collection, 3)
        if should_receive:
            assert event.event_name == "graph.message"
            assert event.context is payload
        assert await get_thread_bridge().call_sync(
            lambda: get_event_runtime_status().wait_listeners,
            thread_ids=["main", "graph", "receiver"],
        ) == baseline_listeners
    finally:
        collection.cancel()
        await asyncio.gather(collection, return_exceptions=True)


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_graph_remote_request_and_reply_progress_at_capacity_one(thread_cores, point):
    def compile_graph():
        async def request(state):
            async with expect_event("graph.reply", point=point) as waiter:
                await post_event_threadsafe(
                    event_name="graph.request", context=state["value"], thread_ids="service",
                )
                reply = await waiter.wait(time_out=2)
            return {"result": reply.context}

        GraphManager().add_node(request).compile_graph("request", using_namespace="client")

    def setup_service():
        @subscribe("graph.request")
        async def respond(event):
            await post_event_threadsafe(
                event_name="graph.reply", context=event.context * 2, thread_ids="graph",
            )

    await thread_cores("graph", compile_graph)
    await thread_cores("service", setup_service)

    async def invoke():
        return await get_namespace_graphs()["client"].invoke({"value": 21})

    assert await asyncio.wait_for(get_thread_bridge().call(
        invoke, thread_ids="graph", background=True,
    ), 3) == {"value": 21, "result": 42}
    # A processed sentinel also ensures the reply's dispatch has released its permit.
    await post_and_wait_threadsafe(event_name="drained", thread_ids=["graph", "service"])
    statuses = await get_thread_bridge().call_sync(
        get_event_runtime_status, thread_ids=["graph", "service"],
    )
    # The compiled graph retains its interruption fallback listener.
    assert statuses["graph"].wait_listeners == 1
    assert statuses["service"].wait_listeners == 0
