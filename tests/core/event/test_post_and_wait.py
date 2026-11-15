"""Publication completion, exact event correlation and listener ownership."""

import asyncio
from contextvars import ContextVar
import importlib

import pytest

from apixis import (
    ApixEventPipe, EventType, EventWaiter,
    post_and_wait, post_and_wait_threadsafe, post_event, subscribe,
)
from apixis.core.event.base import handler_chain_token_context
from apixis.core.event.subscription import publication


@pytest.fixture
def event_runtime_capacity(request):
    return getattr(request, "param", None)


@pytest.mark.parametrize("module_name", [
    "apixis", "apixis.core", "apixis.core.event",
    "apixis.core.event.subscription", "apixis.core.event.subscription.publication",
])
def test_public_exports(module_name):
    module = importlib.import_module(module_name)
    assert module.post_and_wait is post_and_wait
    assert "post_and_wait" in module.__all__
    assert module.post_and_wait_threadsafe is post_and_wait_threadsafe
    assert "post_and_wait_threadsafe" in module.__all__


async def test_concurrent_same_name_events_return_their_own_result(cleanup_event_runtime):
    first_entered, release = asyncio.Event(), asyncio.Event()
    seen = {}

    @subscribe("job.same")
    async def process(event):
        seen[event.context["index"]] = event
        if event.context["index"] == 0:
            first_entered.set()
            await release.wait()
        event.context["processed"] = True

    first = asyncio.create_task(post_and_wait(
        event_type=EventType.INFO, event_name="job.same", context={"index": 0},
    ))
    try:
        await asyncio.wait_for(first_entered.wait(), 1)
        second = await post_and_wait(
            event_type=EventType.INFO, event_name="job.same",
            context={"index": 1}, time_out=1,
        )
        assert second is seen[1]
        assert not first.done()
        release.set()
        first_result = await asyncio.wait_for(first, 1)
        assert first_result is seen[0]
        assert first_result.event_id != second.event_id
        assert first_result.context["processed"]
        assert second.context["processed"]
    finally:
        release.set()
        await asyncio.gather(first, return_exceptions=True)


@pytest.mark.parametrize("event_name", ["job[0]", "job?done", "job*done"])
async def test_event_name_is_matched_literally(event_name):
    event = await post_and_wait(event_name=event_name, time_out=1)
    assert event.event_name == event_name
    assert event.event_type is EventType.DEFAULT


@pytest.mark.parametrize("state", ["normal", "accepted", "error", "no_handlers"])
@pytest.mark.parametrize("keep_listening", [False, True])
async def test_returns_foreground_result_and_preserves_context(state, keep_listening):
    publisher_context = ContextVar("post_and_wait_context", default=None)
    payload = {}
    observed = []
    if state != "no_handlers":
        @subscribe("job.process")
        async def process(event):
            observed.append(event)
            assert publisher_context.get() == "publisher"
            publisher_context.set("handler")
            event.context["processed"] = True
            if state == "accepted":
                event.accept()
            elif state == "error":
                raise ValueError("job failed")

    token = publisher_context.set("publisher")
    try:
        event = await post_and_wait(
            event_type=EventType.TRANSIENT, event_name="job.process",
            context=payload, time_out=1, keep_listening=keep_listening,
        )
        assert publisher_context.get() == "publisher"
    finally:
        publisher_context.reset(token)
    assert event.context is payload
    assert event.accepted is (state == "accepted")
    assert event.has_error is (state == "error")
    if observed:
        assert event is observed[0]
        assert payload["processed"]


async def test_background_handlers_continue_after_return():
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    @subscribe("job.background", background=True)
    async def background(event):
        entered.set()
        await release.wait()
        finished.set()

    try:
        await post_and_wait(event_type=EventType.INFO, event_name="job.background", time_out=1)
        await asyncio.wait_for(entered.wait(), 1)
        assert not finished.is_set()
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), 1)


@pytest.mark.parametrize(("cancel", "keep_listening"), [(False, False), (True, False), (True, True)])
async def test_abandoning_wait_cleans_listener_without_cancelling_dispatch(
    cleanup_event_runtime, wait_for_dispatch, cancel, keep_listening,
):
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    registry = cleanup_event_runtime.handler_registry

    @subscribe("job.slow")
    async def process(event):
        entered.set()
        await release.wait()
        finished.set()

    registered = set(registry.registry)
    task = asyncio.create_task(post_and_wait(
        event_type=EventType.INFO, event_name="job.slow",
        time_out=None if cancel else 0.05,
        keep_listening=keep_listening,
    ))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task
        assert set(registry.registry) == registered
        assert not finished.is_set()
    finally:
        release.set()
        await wait_for_dispatch(cleanup_event_runtime.event_loop)
    assert finished.is_set()


@pytest.mark.parametrize("keep_listening", [False, True])
async def test_timeout_covers_queue_admission(cleanup_event_runtime, monkeypatch, keep_listening):
    core = cleanup_event_runtime
    pipe = ApixEventPipe(maxsize=1)
    await pipe.post_event(event_type=EventType.INFO, event_name="queued")
    monkeypatch.setattr(publication, "get_event_pipe", lambda: pipe)
    registered = set(core.handler_registry.registry)
    with pytest.raises(TimeoutError):
        await post_and_wait(
            event_type=EventType.INFO, event_name="blocked", time_out=0.01,
            keep_listening=keep_listening,
        )
    assert pipe.qsize() == 1
    assert pipe.get_nowait().event_name == "queued"
    pipe.task_done()
    assert set(core.handler_registry.registry) == registered


@pytest.mark.parametrize("event_runtime_capacity", [1], indirect=True)
@pytest.mark.parametrize("keep_listening", [False, True])
async def test_nested_publication_lends_and_restores_dispatch_permit(
    event_runtime_capacity, cleanup_event_runtime, keep_listening,
):
    assert cleanup_event_runtime.event_loop._event_semaphore._bounded == 1
    result = []
    finished = asyncio.Event()

    @subscribe("job.child")
    async def child(event):
        event.context["result"] = 42

    @subscribe("job.parent")
    async def parent(event):
        reply = await post_and_wait(
            event_type=EventType.INFO, event_name="job.child", context={},
            time_out=0 if keep_listening else 1, keep_listening=keep_listening,
        )
        if isinstance(reply, EventWaiter):
            reply = await reply.get_result()
        assert handler_chain_token_context.get().held
        result.append(reply.context["result"])
        finished.set()

    await post_event(event_type=EventType.INFO, event_name="job.parent")
    await asyncio.wait_for(finished.wait(), 2)
    assert result == [42]


@pytest.mark.parametrize("resume", ["get_result", "wait", "close"])
async def test_deferred_waiter_retains_exact_event_and_listener_ownership(
    cleanup_event_runtime, wait_for_dispatch, resume,
):
    core = cleanup_event_runtime
    entered, release = asyncio.Event(), asyncio.Event()
    events = []

    @subscribe("job.deferred")
    async def process(event):
        events.append(event)
        if event.context == "target":
            entered.set()
            await release.wait()

    registered = set(core.handler_registry.registry)
    waiter = await post_and_wait(
        event_name="job.deferred", context="target", time_out=0, keep_listening=True,
    )
    assert isinstance(waiter, EventWaiter)
    try:
        await asyncio.wait_for(entered.wait(), 1)
        listeners = set(core.handler_registry.registry) - registered
        assert len(listeners) == 1
        unrelated = await post_and_wait(event_name="job.deferred", context="other", time_out=1)
        assert unrelated.context == "other"
        assert await waiter.wait(time_out=0) is waiter
        assert set(core.handler_registry.registry) - registered == listeners
        if resume == "close":
            waiter.close()
            assert set(core.handler_registry.registry) == registered
        else:
            release.set()
            result = await asyncio.wait_for(getattr(waiter, resume)(), 1)
            assert result is events[0]
            assert result.event_id != unrelated.event_id
            assert set(core.handler_registry.registry) == registered
    finally:
        waiter.close()
        release.set()
        await wait_for_dispatch(core.event_loop)


async def test_async_worker_rejects_retained_main_loop_waiter(cleanup_event_runtime):
    with pytest.raises(ValueError, match="Cross-thread post_and_wait"):
        await asyncio.to_thread(lambda: asyncio.run(post_and_wait(
            event_name="worker.deferred", time_out=0, keep_listening=True,
        )))
    assert not cleanup_event_runtime.handler_registry.registry


async def test_async_worker_does_not_block_bridge_requests(cleanup_event_runtime):
    """Processing may depend on a later publication from another worker."""
    release, entered = asyncio.Event(), asyncio.Event()

    @subscribe("worker.wait")
    async def process(event):
        entered.set()
        await release.wait()
        event.context["processed"] = True

    @subscribe("worker.release")
    async def unblock(event):
        release.set()

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(post_and_wait(
        event_type=EventType.INFO, event_name="worker.wait", context={}, time_out=2,
    ))))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(post_event(
            event_type=EventType.INFO, event_name="worker.release",
        ))), 1)
        result = await asyncio.wait_for(task, 2)
        assert result.context["processed"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("keep_listening", [False, True])
async def test_result_completed_before_publication_returns_is_preserved(
    cleanup_event_runtime, wait_for_dispatch, monkeypatch, keep_listening,
):
    core = cleanup_event_runtime
    put = core.event_pipe.put
    published = []

    async def publish_and_complete(event):
        published.append(event)
        await put(event)
        await wait_for_dispatch(core.event_loop)

    monkeypatch.setattr(core.event_pipe, "put", publish_and_complete)
    result = await post_and_wait(event_name="early.result", time_out=1, keep_listening=keep_listening)
    assert result is published[0]
    assert not core.handler_registry.registry


@pytest.mark.parametrize("keep_listening", [False, True])
async def test_cancelling_queue_admission_removes_listener_and_does_not_publish(
    cleanup_event_runtime, monkeypatch, keep_listening,
):
    core = cleanup_event_runtime
    pipe = ApixEventPipe(maxsize=1)
    await pipe.post_event(event_name="occupied")
    monkeypatch.setattr(publication, "get_event_pipe", lambda: pipe)
    pending = asyncio.create_task(post_and_wait(event_name="blocked", keep_listening=keep_listening))
    try:
        await asyncio.sleep(0)
        assert core.handler_registry.waiter_count == 1
        assert not pending.done()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert not core.handler_registry.registry
        assert pipe.qsize() == 1
        assert pipe.get_nowait().event_name == "occupied"
        pipe.task_done()
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
