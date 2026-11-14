"""Progress at one dispatch permit and one slot in both event queues."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Barrier

import pytest

from apixis import (
    ApixEventPipe, expect_event_threadsafe, post_and_wait,
    post_and_wait_threadsafe, post_event, post_event_threadsafe, subscribe,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_token_context


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    return 1


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0)


@pytest.fixture(params=[False, True], ids=["default", "eager"])
async def runtime(cleanup_event_runtime, request):
    core = cleanup_event_runtime
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    previous_handler = loop.get_exception_handler()
    errors = []
    loop.set_exception_handler(lambda _, context: errors.append(context))
    if request.param:
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        yield core
    finally:
        await core.thread_bridge.close()
        await asyncio.sleep(0)
        loop.set_task_factory(previous_factory)
        loop.set_exception_handler(previous_handler)
        assert not core.thread_bridge._active
        assert not core.thread_bridge._async_admission
        assert not errors, errors


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_managed_get_result_lends_permit_to_worker_reply(runtime, point):
    ready = asyncio.Event()

    @subscribe("managed.parent")
    async def parent(event):
        async with expect_event_threadsafe("worker.reply", point=point) as waiter:
            ready.set()
            reply = await waiter.get_result()
            assert handler_chain_token_context.get().held
            event.context["reply"] = reply.context

    task = asyncio.create_task(post_and_wait(event_name="managed.parent", context={}))
    await asyncio.wait_for(ready.wait(), 3)
    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(
        post_event_threadsafe(event_name="worker.reply", context=42),
    )), 3)
    result = await asyncio.wait_for(task, 3)
    assert not result.has_error
    assert result.context == {"reply": 42}
    await until(lambda: not runtime.event_loop._dispatch_tasks)
    assert runtime.event_loop._event_semaphore._value == 1
    assert set(runtime.handler_registry.registry) == {"parent"}


async def test_managed_context_entry_drains_full_queues(runtime):
    entered, allow_entry = asyncio.Event(), asyncio.Event()

    @subscribe("managed.parent")
    async def parent(event):
        entered.set()
        await allow_entry.wait()
        async with expect_event_threadsafe("unused.reply"):
            assert handler_chain_token_context.get().held
            event.context["registered"] = True

    parent_task = asyncio.create_task(post_and_wait(event_name="managed.parent", context={}))
    await asyncio.wait_for(entered.wait(), 3)

    async def worker():
        for name in ("pending", "queued", "blocked"):
            await post_event_threadsafe(event_name=name)

    worker_task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    try:
        await until(lambda: runtime.event_pipe.full() and runtime.event_pipe._queue._putters
                    and runtime.event_loop._pending_dispatch is not None)
        assert runtime.event_loop._event_semaphore._value == 0
        allow_entry.set()
        result, _ = await asyncio.wait_for(asyncio.gather(parent_task, worker_task), 3)
        assert not result.has_error
        assert result.context == {"registered": True}
        await until(lambda: not runtime.event_loop._dispatch_tasks)
        assert runtime.event_loop._event_semaphore._value == 1
        assert set(runtime.handler_registry.registry) == {"parent"}
    finally:
        allow_entry.set()
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)


@pytest.mark.parametrize("workers", [1, 4])
async def test_threadsafe_calls_progress_with_occupied_default_executor(runtime, monkeypatch, workers):
    loop = asyncio.get_running_loop()
    pool = ThreadPoolExecutor(max_workers=workers)
    monkeypatch.setattr(loop, "_default_executor", pool)
    start = Barrier(workers)

    @subscribe("managed.parent")
    async def parent(event):
        async with expect_event_threadsafe(f"reply.{event.context}") as waiter:
            await post_event_threadsafe(event_name=f"reply.{event.context}")
            assert (await waiter.get_result()).event_name == f"reply.{event.context}"
        assert handler_chain_token_context.get().held

    def worker(index):
        start.wait(3)
        return asyncio.run(post_and_wait_threadsafe(event_name="managed.parent", context=index))

    tasks = [asyncio.create_task(asyncio.to_thread(worker, index)) for index in range(workers)]
    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), 3)
        assert [event.context for event in results] == list(range(workers))
        assert all(not event.has_error for event in results)
        await until(lambda: not runtime.event_loop._dispatch_tasks)
        assert runtime.event_loop._event_semaphore._value == 1
        assert set(runtime.handler_registry.registry) == {"parent"}
    finally:
        # Release foreign callers before joining their executor if a regression
        # leaves a managed handler waiting for an executor-backed submission.
        await runtime.thread_bridge.close()
        await asyncio.gather(*tasks, return_exceptions=True)
        pool.shutdown(wait=True)


async def test_cancel_context_entry_while_reacquiring_cleans_listener(runtime):
    entered, allow_entry = asyncio.Event(), asyncio.Event()
    blocking, release = asyncio.Event(), asyncio.Event()
    owners = []

    @subscribe("managed.parent")
    async def parent(event):
        owners.append(handler_chain_token_context.get())
        entered.set()
        await allow_entry.wait()
        async with expect_event_threadsafe("unused.reply"):
            pytest.fail("Entry must wait for the blocker to return its permit.")

    @subscribe("blocker")
    async def blocker(event):
        blocking.set()
        await release.wait()

    await post_event(event_name="managed.parent")
    await asyncio.wait_for(entered.wait(), 3)
    await post_event(event_name="blocker")
    await until(lambda: runtime.event_loop._pending_dispatch is not None)
    allow_entry.set()
    await asyncio.wait_for(blocking.wait(), 3)
    chain = owners[0]
    try:
        await until(lambda: chain.resuming_task is chain.owner)
        owner = chain.owner
        owner.cancel()
        await asyncio.wait_for(asyncio.gather(owner, return_exceptions=True), 3)
        assert set(runtime.handler_registry.registry) == {"parent", "blocker"}
    finally:
        release.set()
    await until(lambda: not runtime.event_loop._dispatch_tasks)
    assert runtime.event_loop._event_semaphore._value == 1


async def test_cancel_notified_producer_passes_capacity_to_next_waiter(runtime, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    published = []

    async def blocked(event):
        if event.event_name == "first":
            entered.set()
            await release.wait()
        published.append(event.event_name)

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    bridge = runtime.thread_bridge
    first = await bridge.asubmit(partial(post_event, event_name="first"))
    await asyncio.wait_for(entered.wait(), 3)
    second = await bridge.asubmit(partial(post_event, event_name="second"))
    cancelled = asyncio.create_task(post_event_threadsafe(event_name="cancelled"))
    last = asyncio.create_task(post_event_threadsafe(event_name="last"))
    await until(lambda: len(bridge._async_admission) == 2)
    # Cancel exactly after a capacity wakeup, before that producer can enqueue.
    next(iter(bridge._async_admission)).add_done_callback(lambda _: cancelled.cancel())
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await asyncio.wait_for(asyncio.gather(
        asyncio.wrap_future(first.result), asyncio.wrap_future(second.result), last,
    ), 3)
    assert published == ["first", "second", "last"]
    assert not bridge._async_admission
