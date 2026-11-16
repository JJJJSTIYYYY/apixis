"""Framework permit lending and the limits of application-owned dependencies."""

import asyncio
from concurrent.futures import Future

import pytest

from apixis import (
    ApixEventPipe, expect_event, expect_event_threadsafe, get_event_runtime_status,
    get_thread_bridge, post_and_wait, post_and_wait_threadsafe,
    post_event_threadsafe, subscribe,
    stop_core,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_token_context, suspend_process


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    return 1


@pytest.fixture(params=[False, True], ids=["default", "eager"])
async def task_factory(request):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    selected = asyncio.eager_task_factory if request.param else None
    loop.set_task_factory(selected)
    try:
        yield selected
    finally:
        loop.set_task_factory(previous)


async def test_default_router_call_allows_nested_requests_to_same_target(thread_cores, task_factory):
    def setup():
        asyncio.get_running_loop().set_task_factory(task_factory)

        @subscribe("nested")
        async def respond(event):
            event.context *= 2

    await thread_cores("worker", setup)

    async def nested():
        return await post_and_wait_threadsafe(event_name="nested", context=21, thread_ids="worker")

    event = await asyncio.wait_for(get_thread_bridge().call(nested, thread_ids="worker"), 2)
    assert not event.has_error
    assert event.context == 42


async def test_concurrent_a_b_a_roundtrips_lend_each_cores_permits(thread_cores, task_factory):
    def setup_a():
        asyncio.get_running_loop().set_task_factory(task_factory)

        @subscribe("start")
        async def start(event):
            async with expect_event_threadsafe(
                f"reply.{event.context}", point="processed", thread_ids="a",
            ) as waiter:
                await post_event_threadsafe(event_name="middle", context=event.context, thread_ids="b")
                reply = await waiter.get_result()
            assert handler_chain_token_context.get().held
            event.context = reply.context

        @subscribe("finish")
        async def finish(event):
            await post_event_threadsafe(
                event_name=f"return.{event.context}", context=event.context, thread_ids="b",
            )

    def setup_b():
        asyncio.get_running_loop().set_task_factory(task_factory)

        @subscribe("middle")
        async def middle(event):
            async with expect_event(f"return.{event.context}", point="processed") as waiter:
                await post_event_threadsafe(event_name="finish", context=event.context, thread_ids="a")
                reply = await waiter.get_result()
            assert handler_chain_token_context.get().held
            await post_event_threadsafe(
                event_name=f"reply.{event.context}", context=reply.context * 2, thread_ids="a",
            )

    await thread_cores("a", setup_a)
    await thread_cores("b", setup_b)
    events = await asyncio.wait_for(asyncio.gather(*(
        post_and_wait_threadsafe(event_name="start", context=index, thread_ids="a")
        for index in range(12)
    )), 5)
    assert [event.context for event in events] == [index * 2 for index in range(12)]
    assert all(not event.has_error for event in events)
    assert len({event.event_id for event in events}) == 12
    async with asyncio.timeout(3):
        while True:
            statuses = await get_thread_bridge().call_sync(get_event_runtime_status, thread_ids=["a", "b"])
            if all(status.event_loop.dispatch_tasks == 0 for status in statuses.values()):
                break
            await asyncio.sleep(0)
    for status in statuses.values():
        assert status.wait_listeners == 0
        assert status.event_loop.dispatch_permits_in_use == 0
        assert not status.event_loop.pending_dispatch
        assert status.event_queue_size == 0
        assert status.thread_bridge.queue_size == 0
        assert status.thread_bridge.admission_waiters == 0


@pytest.mark.parametrize("lend_source_permit", [False, True], ids=["application-blocked", "source-suspended"])
async def test_to_thread_reverse_request_needs_source_loop_suspension(
    thread_cores, task_factory, lend_source_permit,
):
    abandon = Future()
    seen = []

    def setup():
        asyncio.get_running_loop().set_task_factory(task_factory)

        @subscribe("child")
        async def child(event):
            seen.append(event.context)
            event.context *= 2

        @subscribe("parent")
        async def parent(event):
            chain = handler_chain_token_context.get()

            def worker():
                async def reverse():
                    assert handler_chain_token_context.get() is (None if lend_source_permit else chain)
                    request = asyncio.create_task(post_and_wait_threadsafe(
                        event_name="child", context=event.context, thread_ids="source",
                    ))
                    if lend_source_permit:
                        return (await asyncio.wait_for(request, 2)).context
                    await asyncio.wrap_future(abandon)
                    request.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await request
                    return "blocked"

                return asyncio.run(reverse())

            if lend_source_permit:
                async with suspend_process():
                    event.context = await asyncio.to_thread(worker)
            else:
                event.context = await asyncio.to_thread(worker)
            assert chain.held

    await thread_cores("source", setup)
    parent = asyncio.create_task(post_and_wait_threadsafe(event_name="parent", context=21, thread_ids="source"))
    try:
        if not lend_source_permit:
            async with asyncio.timeout(3):
                while True:
                    status = await get_thread_bridge().call_sync(get_event_runtime_status, thread_ids="source")
                    if status.event_loop.pending_dispatch:
                        break
                    await asyncio.sleep(0)
            assert status.event_loop.dispatch_permits_in_use == 1
            assert not seen
            assert not parent.done()
            abandon.set_result(None)
        result = await asyncio.wait_for(parent, 3)
        assert not result.has_error
        assert result.context == (42 if lend_source_permit else "blocked")
        # Cancellation removed the reply listener, but an admitted child still runs.
        await post_and_wait_threadsafe(event_name="drained", thread_ids="source")
        assert seen == [21]
    finally:
        if not abandon.done():
            abandon.set_result(None)
        parent.cancel()
        await asyncio.gather(parent, return_exceptions=True)


async def test_framework_lends_permit_but_cannot_release_application_lock(
    cleanup_event_runtime, task_factory,
):
    lock = asyncio.Lock()
    child_entered = asyncio.Event()
    chains = []

    @subscribe("parent")
    async def parent(event):
        chains.append(handler_chain_token_context.get())
        async with lock:
            await post_and_wait(event_name="child")

    @subscribe("child")
    async def child(event):
        chains.append(handler_chain_token_context.get())
        child_entered.set()
        async with lock:
            pytest.fail("The parent still owns the application lock.")

    request = asyncio.create_task(post_and_wait(event_name="parent"))
    try:
        await asyncio.wait_for(child_entered.wait(), 2)
        assert not chains[0].held
        assert lock.locked()
        assert not request.done()
        # Cancellation in the wait body still restores the parent's permit,
        # so the application's resource cycle also delays that cancellation.
        chains[0].owner.cancel()
        async with asyncio.timeout(2):
            while chains[0].resuming_task is None:
                await asyncio.sleep(0)
        assert lock.locked()
        assert not request.done()
        # The application must break the cycle, here by cancelling the child
        # holding the dispatch permit while waiting for its parent's lock.
        child_task = chains[1].owner
        child_task.cancel()
        await asyncio.gather(child_task, return_exceptions=True)
        await asyncio.wait_for(request, 2)
        await asyncio.sleep(0)
        assert not lock.locked()
        assert get_event_runtime_status().wait_listeners == 0
        assert cleanup_event_runtime.event_loop._event_semaphore._value == 1
    finally:
        for chain in reversed(chains):
            if chain.owner is not None:
                chain.owner.cancel()
        request.cancel()
        await asyncio.gather(request, return_exceptions=True)


async def test_stopping_one_target_fails_wait_and_cleans_other_targets(thread_cores):
    await thread_cores("a")
    await thread_cores("b")
    async with expect_event_threadsafe("never", thread_ids=["a", "b"]) as waiter:
        waiting = asyncio.create_task(waiter.get_result())
        with pytest.raises(RuntimeError, match="stopped"):
            await get_thread_bridge().call(stop_core, thread_ids="b")
        with pytest.raises(RuntimeError, match="stopped"):
            await asyncio.wait_for(waiting, 2)
    assert await get_thread_bridge().call_sync(
        lambda: get_event_runtime_status().wait_listeners, thread_ids="a",
    ) == 0


async def test_cancel_multitarget_publication_cleans_blocked_target_without_rolling_back_others(thread_cores):
    entered, release, delivered = Future(), Future(), Future()
    observed = {"a": [], "b": []}

    def setup_a():
        @subscribe("*")
        async def receive(event):
            if event.event_name == "hold":
                entered.set_result(None)
                await asyncio.wrap_future(release)
            observed["a"].append(event.event_name)

    def setup_b():
        @subscribe("broadcast")
        async def receive(event):
            observed["b"].append(event.event_name)
            delivered.set_result(None)

    owner = await thread_cores("a", setup_a)
    await thread_cores("b", setup_b)

    async def snapshot():
        # Test instrumentation bypasses the serial publication already blocked
        # in a's endpoint, but reads all asyncio state on the owning loop.
        result = Future()
        owner.loop.call_soon_threadsafe(lambda: result.set_result(get_event_runtime_status()))
        return await asyncio.wrap_future(result)

    blocked = broadcast = None
    try:
        await post_event_threadsafe(event_name="hold", thread_ids="a")
        await asyncio.wait_for(asyncio.wrap_future(entered), 2)
        await post_event_threadsafe(event_name="pending", thread_ids="a")
        await post_event_threadsafe(event_name="queued", thread_ids="a")
        blocked = asyncio.create_task(post_event_threadsafe(event_name="blocked", thread_ids="a"))
        async with asyncio.timeout(2):
            while (await snapshot()).thread_bridge.active_requests == 0:
                await asyncio.sleep(0)
        broadcast = asyncio.create_task(post_event_threadsafe(event_name="broadcast", thread_ids=["a", "b"]))
        await asyncio.wait_for(asyncio.wrap_future(delivered), 2)
        async with asyncio.timeout(2):
            while (await snapshot()).thread_bridge.queue_size != 1:
                await asyncio.sleep(0)
        broadcast.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(broadcast, 2)
        assert observed["b"] == ["broadcast"]
        assert not blocked.done()
        release.set_result(None)
        await asyncio.wait_for(blocked, 2)
        await post_and_wait_threadsafe(event_name="drained", thread_ids=["a", "b"])
        assert observed["a"] == ["hold", "pending", "queued", "blocked", "drained"]
    finally:
        if not release.done():
            release.set_result(None)
        tasks = [task for task in (blocked, broadcast) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
