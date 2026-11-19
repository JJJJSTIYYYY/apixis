"""Verify event publication and listener routing across named cores."""

import asyncio
from concurrent.futures import Future

import pytest

from apixis import (
    ApixEventPipe, expect_event, expect_event_threadsafe,
    get_event_registry, get_event_runtime_status, get_thread_bridge, post_and_wait_threadsafe,
    post_event, post_event_threadsafe, subscribe,
)
from apixis.core.event import factory


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    return 1


@pytest.mark.parametrize("point", ["received", "processed"])
@pytest.mark.parametrize("publication_target, caller, listening_target", [
    (None, "main", "sender"),
    ("receiver", "receiver", None),
    ("receiver", "receiver", "receiver"),
    ("receiver", "receiver", "sender"),
    ("receiver", "receiver", "main"),
    ("receiver", "main", "receiver"),
])
async def test_publication_and_listener_core_must_match(
    thread_cores, point, publication_target, caller, listening_target,
):
    ready, published = Future(), Future()
    payload = {"value": 42}

    def setup_sender():
        @subscribe("routing.trigger")
        async def publish(event):
            if publication_target is None:
                await post_event(event_name="routing.message", context=payload)
            else:
                await post_event_threadsafe(
                    event_name="routing.message", context=payload, thread_ids=publication_target,
                )

    await thread_cores("sender", setup_sender)
    await thread_cores("receiver")
    cores = ["main", "sender", "receiver"]
    baseline_listeners = await get_thread_bridge().call_sync(
        lambda: get_event_runtime_status().wait_listeners,
        thread_ids=cores,
    )
    actual_target = publication_target or "sender"
    listener_core = listening_target or caller
    should_receive = actual_target == listener_core

    async def collect():
        context = (
            expect_event("routing.message", point=point, time_out=2 if should_receive else 0.1)
            if listening_target is None else
            expect_event_threadsafe("routing.message", point=point, thread_ids=listening_target, time_out=2 if should_receive else 0.1)
        )
        async with context as waiter:
            ready.set_result(None)
            # Publish before testing the negative case's deadline.
            await asyncio.wrap_future(published)
            if should_receive:
                return await waiter.wait()
            with pytest.raises(TimeoutError):
                await waiter.wait()

    collection = asyncio.create_task(get_thread_bridge().call(
        collect, thread_ids=caller, background=True,
    ))

    try:
        await asyncio.wait_for(asyncio.wrap_future(ready), 3)
        trigger = await post_and_wait_threadsafe(event_name="routing.trigger", thread_ids="sender", time_out=3)
        assert not trigger.error_stack
        await post_and_wait_threadsafe(event_name="drained", thread_ids=actual_target)
        observations = await get_thread_bridge().call_sync(
            lambda: "routing.message" in get_event_registry().get_registered_events(),
            thread_ids=cores,
        )
        assert observations == {core: core == actual_target for core in cores}
        published.set_result(None)
        event = await asyncio.wait_for(collection, 3)
        if should_receive:
            assert event.event_name == "routing.message"
            assert event.context is payload
        assert await get_thread_bridge().call_sync(
            lambda: get_event_runtime_status().wait_listeners,
            thread_ids=cores,
        ) == baseline_listeners
    finally:
        collection.cancel()
        await asyncio.gather(collection, return_exceptions=True)


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_remote_request_and_local_reply_wait_at_capacity_one(thread_cores, point):
    def setup_client():
        @subscribe("routing.trigger")
        async def request(event):
            async with expect_event("routing.reply", point=point, time_out=2) as waiter:
                await post_event_threadsafe(
                    event_name="routing.request", context=event.context, thread_ids="service",
                )
                reply = await waiter.wait()
            event.context = reply.context

    def setup_service():
        @subscribe("routing.request")
        async def respond(event):
            await post_event_threadsafe(
                event_name="routing.reply", context=event.context * 2, thread_ids="client",
            )

    await thread_cores("client", setup_client)
    await thread_cores("service", setup_service)

    result = await post_and_wait_threadsafe(
        event_name="routing.trigger", context=21, thread_ids="client", time_out=3,
    )
    assert not result.error_stack
    assert result.context == 42
    # A processed sentinel also ensures the reply's dispatch has released its permit.
    cores = ["client", "service"]
    await post_and_wait_threadsafe(event_name="drained", thread_ids=cores)
    waiters = await get_thread_bridge().call_sync(
        lambda: get_event_runtime_status().wait_listeners, thread_ids=cores,
    )
    assert waiters == {"client": 0, "service": 0}
