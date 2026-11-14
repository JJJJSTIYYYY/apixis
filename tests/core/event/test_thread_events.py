"""Thread bridge contracts, readiness, cancellation and core shutdown."""

import asyncio
from contextvars import ContextVar
import importlib
from functools import partial
from threading import Event, get_ident

import pytest

from apixis import (
    ApixEventPipe, EventType, ThreadEventWaiter,
    wait_event, wait_event_threadsafe, post_event, post_event_threadsafe, start_core, stop_core,
    subscribe, expect_event, expect_event_threadsafe,
    post_and_wait, post_and_wait_threadsafe,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_token_context
from apixis.core.event.subscription.waiter import collect_events


publisher_context = ContextVar("thread_event_test", default=None)


async def _submit_publication(**kwargs):
    return (await factory.get_thread_bridge().asubmit(partial(post_event, **kwargs))).result


async def _submit_collection(event_name, **kwargs):
    return (await factory.get_thread_bridge().asubmit(partial(
        collect_events, event_name, point="received", filter=None, time_out=None, **kwargs,
    ), background=True)).result



@pytest.mark.parametrize("module_name", [
    "apixis",
    "apixis.core",
    "apixis.core.event",
    "apixis.core.event.subscription",
    "apixis.core.event.subscription.waiter",
])
def test_thread_waiting_exports_and_aliases(module_name):
    module = importlib.import_module(module_name)
    assert module.expect_event_threadsafe is module.wait_for_event_threadsafe is expect_event_threadsafe
    assert module.wait_event_threadsafe is module.await_for_threadsafe is wait_event_threadsafe
    assert {
        "expect_event_threadsafe", "wait_event_threadsafe",
        "wait_for_event_threadsafe", "await_for_threadsafe",
    } <= set(module.__all__)


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "_core", None)
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=2))
    return 1


@pytest.fixture
async def runtime(cleanup_event_runtime):
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    errors = []
    loop.set_exception_handler(lambda _, context: errors.append(context))
    core = cleanup_event_runtime
    try:
        yield core
    finally:
        await core.thread_bridge.close()
        await asyncio.sleep(0)
        assert not core.thread_bridge._active
        assert not errors, errors
        loop.set_exception_handler(previous)


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.parametrize("expect", [expect_event, expect_event_threadsafe])
async def test_roundtrip_and_thread_context_snapshot(runtime, expect):
    owner = get_ident()
    seen = []
    processed = Event()

    @subscribe("thread.request")
    async def respond(event):
        assert event.event_type is EventType.DEFAULT
        seen.append((get_ident(), publisher_context.get(), event.context))
        # The dispatch has its own chain, never the publisher's inherited marker.
        assert handler_chain_token_context.get() is not marker
        await runtime.event_pipe.post_event(
            event_type=EventType.INFO, event_name="thread.reply", context=event.context,
        )

    @subscribe("thread.reply")
    async def mark_processed(event):
        event.context["processed"] = True
        processed.set()

    marker = object()

    async def worker():
        publisher_context.set("worker-context")
        handler_chain_token_context.set(marker)
        payload = {"id": 42}
        async with expect("thread.reply", point="processed") as waiter:
            await post_event_threadsafe(
                event_name="thread.request", context=payload,
            )
            publisher_context.set("changed")
            reply = await waiter.wait(time_out=3)
            assert processed.is_set()
            assert reply.context is payload
            assert reply.context["processed"] is True
        return payload

    payload = await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 5)
    assert seen == [(owner, "worker-context", payload)]
    assert len(runtime.handler_registry.registry) == 2
    assert runtime.event_loop._event_semaphore._value == 1


@pytest.mark.parametrize("state", ["normal", "accepted", "error", "no_handlers"])
async def test_post_and_wait_threadsafe_returns_foreground_result(runtime, state):
    seen = []
    if state != "no_handlers":
        @subscribe("thread.process[0]")
        async def process(event):
            seen.append(event)
            assert publisher_context.get() == "worker-context"
            assert handler_chain_token_context.get().owner is asyncio.current_task()
            event.context["processed"] = True
            if state == "accepted":
                event.accept()
            elif state == "error":
                raise ValueError("processing failed")

    async def worker():
        publisher_context.set("worker-context")
        payload = {}
        event = await post_and_wait_threadsafe(event_name="thread.process0", context=payload, time_out=1)
        assert event.context is payload
        return event

    event = await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 5)
    assert event.event_type is EventType.DEFAULT
    assert event.accepted is (state == "accepted")
    assert event.has_error is (state == "error")
    if seen:
        assert event is seen[0]
        assert event.context["processed"]


async def test_post_and_wait_threadsafe_matches_concurrent_same_name_events(runtime):
    entered = asyncio.Event()

    @subscribe("thread.same")
    async def process(event):
        if event.context == "first":
            entered.set()
            await wait_event("thread.release")

    first = asyncio.create_task(post_and_wait_threadsafe(event_name="thread.same", context="first", time_out=2))
    await asyncio.wait_for(entered.wait(), 1)
    try:
        second = await post_and_wait_threadsafe(event_name="thread.same", context="second", time_out=1)
        assert second.context == "second"
        assert not first.done()
    finally:
        await post_event(event_name="thread.release")
    original = await asyncio.wait_for(first, 2)
    assert original.context == "first"
    assert original.event_id != second.event_id


@pytest.mark.parametrize("cancel", [False, True])
async def test_post_and_wait_threadsafe_abandonment_cleans_listener(runtime, cancel):
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    @subscribe("thread.slow")
    async def process(event):
        entered.set()
        await release.wait()
        finished.set()

    registered = set(runtime.handler_registry.registry)
    task = asyncio.create_task(post_and_wait_threadsafe(
        event_name="thread.slow", time_out=None if cancel else 0.05,
    ))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task
        await until(lambda: not runtime.thread_bridge._active)
        assert set(runtime.handler_registry.registry) == registered
        assert not finished.is_set()
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), 1)


async def test_shielded_post_and_wait_threadsafe_can_resume_after_caller_timeout(runtime):
    entered, release = asyncio.Event(), asyncio.Event()

    @subscribe("thread.delayed")
    async def process(event):
        entered.set()
        await release.wait()
        event.context["processed"] = True

    task = asyncio.create_task(post_and_wait_threadsafe(event_name="thread.delayed", context={}))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), 0.01)
        assert not task.done()
        release.set()
        event = await asyncio.wait_for(task, 1)
        assert event.context["processed"]
    finally:
        release.set()


async def test_async_wait_filters_and_collects_all_patterns(runtime):
    task = asyncio.create_task(wait_event_threadsafe(
        ["job.*", "done"], filter="job.internal", point="processed", time_out=2,
    ))
    await until(lambda: len(runtime.handler_registry.registry) == 2)
    for name in ("job.internal", "job.public", "done"):
        await post_event_threadsafe(event_name=name)
    events = await task
    assert {pattern: event.event_name for pattern, event in events.items()} == {
        "job.*": "job.public", "done": "done",
    }
    await until(lambda: not runtime.thread_bridge._active)
    assert not runtime.handler_registry.registry


@pytest.mark.parametrize("use_context", [False, True])
@pytest.mark.parametrize("options, error", [
    ({"event_name": "", "time_out": 1}, ValueError),
    ({"event_name": "never", "point": "invalid"}, ValueError),
    ({"event_name": "never", "time_out": float("nan")}, ValueError),
    ({"event_name": "never", "time_out": 0.01}, TimeoutError),
])
@pytest.mark.parametrize("expect, wait", [
    (expect_event, wait_event), (expect_event_threadsafe, wait_event_threadsafe),
])
async def test_validation_and_timeout_clean_up(runtime, use_context, options, error, expect, wait):
    async def worker():
        with pytest.raises(error):
            if use_context:
                async with expect(**options) as waiter:
                    await waiter.wait(time_out=options.get("time_out"))
            else:
                await wait(**options)

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 5)
    await until(lambda: not runtime.thread_bridge._active)
    assert not runtime.handler_registry.registry


async def test_wait_call_timeout_cleans_listeners_before_return(runtime):
    async def worker():
        async with expect_event_threadsafe("never") as waiter:
            with pytest.raises(TimeoutError):
                await waiter.wait(time_out=0.01)
        with pytest.raises(RuntimeError, match="already been entered"):
            async with waiter:
                pass

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 5)
    assert not runtime.handler_registry.registry


@pytest.mark.parametrize("expect", [expect_event, expect_event_threadsafe])
async def test_wait_deadline_update_progresses_behind_blocked_publication(runtime, monkeypatch, expect):
    registered, start_wait = Event(), Event()
    publishing, release = asyncio.Event(), asyncio.Event()

    async def blocked(event):
        publishing.set()
        await release.wait()

    async def worker():
        async with expect("never") as waiter:
            registered.set()
            while not start_wait.is_set():
                await asyncio.sleep(0)
            with pytest.raises(TimeoutError):
                await waiter.wait(time_out=0.01)

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    await until(registered.is_set)
    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    publication = await _submit_publication(event_name="blocked")
    await asyncio.wait_for(publishing.wait(), 1)
    try:
        start_wait.set()
        await asyncio.wait_for(task, 2)
        assert not runtime.handler_registry.registry
        assert not publication.done()
    finally:
        start_wait.set()
        release.set()
        await asyncio.gather(task, asyncio.wrap_future(publication), return_exceptions=True)


async def test_invalid_wait_deadline_preserves_registered_collection(runtime):
    async def worker():
        async with expect_event_threadsafe("reply") as waiter:
            with pytest.raises(ValueError, match="finite"):
                await waiter.wait(time_out=float("nan"))
            await post_event_threadsafe(event_name="reply", context=42)
            assert (await waiter.get_result()).context == 42

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)
    assert not runtime.handler_registry.registry


async def test_cancel_before_deadline_update_does_not_reactivate_collection(runtime, monkeypatch):
    loop = asyncio.get_running_loop()
    schedule = loop.call_soon_threadsafe
    pending = []

    def delay_update(callback, *args, **kwargs):
        if callback.__qualname__ == "EventWaiter.reset_timeout":
            pending.append(partial(callback, *args))
            return schedule(lambda: None)
        return schedule(callback, *args, **kwargs)

    monkeypatch.setattr(loop, "call_soon_threadsafe", delay_update)

    async def worker():
        async with expect_event_threadsafe("never") as waiter:
            task = asyncio.create_task(waiter.wait(time_out=30))
            while not pending:
                await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)
    pending[0]()
    assert not runtime.handler_registry.registry
    assert not runtime.thread_bridge._active


@pytest.mark.parametrize("expect", [expect_event, expect_event_threadsafe])
async def test_context_body_exception_closes_waiter(runtime, expect):
    async def worker():
        with pytest.raises(LookupError):
            async with expect("never"):
                raise LookupError("body failed")

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 5)
    assert not runtime.handler_registry.registry


@pytest.mark.parametrize("after_registration", [False, True])
async def test_cancel_wait_task(runtime, after_registration):
    task = asyncio.create_task(wait_event_threadsafe("never"))
    if after_registration:
        await until(lambda: bool(runtime.handler_registry.registry))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await until(lambda: not runtime.thread_bridge._active and not runtime.handler_registry.registry)


@pytest.mark.parametrize("operation", ["wait", "post"])
async def test_cancel_task_before_its_coroutine_starts(runtime, monkeypatch, operation):
    loop = asyncio.get_running_loop()
    create_task = loop.create_task
    cancelled = False

    def create_cancelled(coroutine, **kwargs):
        nonlocal cancelled
        task = create_task(coroutine, **kwargs)
        if coroutine.__qualname__ == "EventThreadBridge._execute" and not cancelled:
            cancelled = True
            task.cancel()
        return task

    monkeypatch.setattr(loop, "create_task", create_cancelled)
    with pytest.raises(asyncio.CancelledError):
        if operation == "wait":
            await wait_event_threadsafe("never")
        else:
            await post_event_threadsafe(event_name="cancelled")
    await post_event_threadsafe(event_name="next")
    await until(lambda: not runtime.thread_bridge._active)
    assert not runtime.handler_registry.registry


async def test_bounded_admission_and_cancel_before_publication(runtime):
    first = await _submit_publication(event_type=EventType.INFO, event_name="first")
    second = await _submit_publication(event_type=EventType.INFO, event_name="cancelled")
    assert len(runtime.thread_bridge._requests) == runtime.event_pipe.maxsize
    assert second.cancel()
    received = []

    @subscribe("*")
    async def capture(event):
        received.append(event.event_name)

    await asyncio.wrap_future(first)
    await until(lambda: received == ["first"] and not runtime.thread_bridge._active)
    assert received == ["first"]


async def test_result_timeout_does_not_cancel_publication(runtime, monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked(event):
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    result = await _submit_publication(event_type=EventType.INFO, event_name="blocked")
    await entered.wait()

    async def worker():
        with pytest.raises(TimeoutError):
            result.result(0.01)
        assert not result.done()

    await asyncio.to_thread(lambda: asyncio.run(worker()))
    release.set()
    assert await asyncio.wrap_future(result) is None


async def test_stop_fails_pending_waits_and_blocked_publications(runtime, monkeypatch):
    waiting = await _submit_collection("never")
    await until(lambda: bool(runtime.handler_registry.registry))
    entered = asyncio.Event()

    async def blocked(event):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    publishing = await _submit_publication(event_type=EventType.INFO, event_name="blocked")
    await entered.wait()
    queued = await _submit_publication(event_type=EventType.INFO, event_name="queued")
    await asyncio.wait_for(stop_core(), 3)
    for future in (waiting, publishing, queued):
        with pytest.raises(RuntimeError, match="stopped"):
            future.result()
    assert not runtime.handler_registry.registry
    with pytest.raises(RuntimeError, match="not running"):
        await _submit_collection("never")
    await start_core()
    assert not runtime.thread_bridge.closed


async def test_shutdown_unblocks_context_entry_and_wait(runtime, monkeypatch):
    entered = Event()

    async def worker():
        with pytest.raises(RuntimeError, match="stopped"):
            async with expect_event_threadsafe("never") as waiter:
                entered.set()
                await waiter.wait()

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    await until(entered.is_set)
    await until(lambda: len(runtime.thread_bridge._active) == 1)
    await stop_core()
    await asyncio.wait_for(task, 3)
    assert not runtime.handler_registry.registry


async def test_main_loop_can_use_async_thread_context(runtime):
    async with ThreadEventWaiter("main.reply") as waiter:
        await post_event_threadsafe(event_name="main.reply")
        assert (await waiter.wait(time_out=1)).event_name == "main.reply"
    assert not runtime.handler_registry.registry


async def test_thread_context_wait_replaces_registration_deadline(runtime):
    entered = Event()
    waiters = []

    async def worker():
        async with expect_event_threadsafe("deadline.reply", time_out=0.05) as waiter:
            waiters.append(waiter)
            entered.set()
            return await waiter.wait()

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    await until(entered.is_set)
    await until(lambda: waiters[0]._waiter._deadline is None)
    await asyncio.sleep(0.06)
    assert not task.done()
    await post_event_threadsafe(event_name="deadline.reply")
    assert (await asyncio.wait_for(task, 1)).event_name == "deadline.reply"
    assert not runtime.handler_registry.registry


async def test_thread_context_cancellation_finishes_owner_cleanup(runtime):
    entered, cancel = Event(), Event()

    async def worker():
        async def wait():
            async with expect_event_threadsafe("never") as waiter:
                entered.set()
                await waiter.wait()

        waiting = asyncio.create_task(wait())
        while not cancel.is_set():
            await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    await until(entered.is_set)
    await until(lambda: len(runtime.thread_bridge._active) == 1)
    cancel.set()
    await asyncio.wait_for(task, 2)
    assert not runtime.handler_registry.registry
    assert not runtime.thread_bridge._active


@pytest.mark.parametrize("waiting_for_admission", [False, True])
async def test_cancel_thread_context_entry_keeps_worker_loop_responsive(
    runtime, monkeypatch, waiting_for_admission,
):
    entered, release = asyncio.Event(), asyncio.Event()
    ticked, cancel = Event(), Event()

    async def blocked(event):
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    publications = [await _submit_publication(event_name="blocked")]
    await entered.wait()
    if waiting_for_admission:
        publications.extend([await _submit_publication(event_name="queued") for _ in range(2)])

    async def worker():
        async def enter():
            async with expect_event_threadsafe("never"):
                pytest.fail("Registration must wait behind the blocked publication.")

        task = asyncio.create_task(enter())
        await asyncio.sleep(0.01)
        ticked.set()
        while not cancel.is_set():
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    try:
        await until(ticked.is_set)
        if waiting_for_admission:
            await until(lambda: bool(runtime.thread_bridge._async_admission))
        else:
            await until(lambda: len(runtime.thread_bridge._requests) == 1)
        cancel.set()
        await asyncio.wait_for(task, 2)
        assert not runtime.handler_registry.registry
    finally:
        cancel.set()
        release.set()
        await asyncio.wait_for(task, 2)
        await asyncio.gather(*(asyncio.wrap_future(future) for future in publications))


async def test_threadsafe_awaits_lend_managed_caller_permit(runtime):
    finished = asyncio.Event()
    results = []

    @subscribe("managed.child")
    async def child(event):
        event.context["processed"] = True

    @subscribe("managed.parent")
    async def parent(event):
        child_event = await post_and_wait_threadsafe(event_name="managed.child", context={}, time_out=1)
        async with expect_event_threadsafe("managed.reply") as waiter:
            await post_event_threadsafe(event_name="managed.reply")
            reply = await waiter.wait(time_out=1)
        assert handler_chain_token_context.get().held
        results.append((child_event.context["processed"], reply.event_name))
        finished.set()

    await post_event(event_name="managed.parent")
    await asyncio.wait_for(finished.wait(), 2)
    assert results == [(True, "managed.reply")]


async def test_worker_cannot_start_core_or_access_registry(runtime):
    async def worker():
        with pytest.raises(RuntimeError, match="main thread"):
            factory.get_handler_registry()
        with pytest.raises(RuntimeError, match="main thread"):
            await start_core()

    await asyncio.to_thread(lambda: asyncio.run(worker()))


async def test_uninitialized_bridge_does_not_construct_core(runtime, monkeypatch):
    monkeypatch.setattr(factory, "_core", None)
    with pytest.raises(RuntimeError, match="start_core"):
        await wait_event_threadsafe("never")
    assert factory._core is None


async def test_worker_waits_for_full_bridge_without_retry(runtime, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def blocked(event):
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    first = await _submit_publication(event_type=EventType.INFO, event_name="first")
    await entered.wait()
    queued = [await _submit_publication(event_type=EventType.INFO, event_name="queued") for _ in range(2)]
    started, admitted = Event(), Event()

    async def worker():
        started.set()
        future = await _submit_publication(event_type=EventType.INFO, event_name="last")
        admitted.set()
        future.result(3)

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    await until(started.is_set)
    await until(lambda: bool(runtime.thread_bridge._async_admission))
    assert not admitted.is_set()
    release.set()
    await asyncio.wait_for(task, 3)
    await asyncio.gather(*(asyncio.wrap_future(future) for future in [first, *queued]))


@pytest.mark.parametrize("eager", [False, True])
async def test_consumer_wakeup_coalesces_and_restarts_after_idle(runtime, monkeypatch, eager):
    loop = asyncio.get_running_loop()
    schedule = loop.call_soon_threadsafe
    wakeups = 0

    def count_wakeups(callback, *args, **kwargs):
        nonlocal wakeups
        if callback == runtime.thread_bridge._ensure_consumer:
            wakeups += 1
        return schedule(callback, *args, **kwargs)

    monkeypatch.setattr(loop, "call_soon_threadsafe", count_wakeups)
    previous = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    entered, release = asyncio.Event(), asyncio.Event()
    published = []

    async def blocked(event):
        published.append(event.event_name)
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    try:
        for burst in range(4):
            entered.clear()
            release.clear()
            first = await _submit_publication(event_type=EventType.INFO, event_name=f"{burst}.first")
            await entered.wait()
            queued = [await _submit_publication(event_type=EventType.INFO, event_name=f"{burst}.{i}") for i in range(2)]
            assert wakeups == burst + 1
            release.set()
            await asyncio.gather(*(asyncio.wrap_future(future) for future in [first, *queued]))
            await until(lambda: runtime.thread_bridge._consumer.done())
            assert not runtime.thread_bridge._consumer_scheduled
        assert published == [name for burst in range(4) for name in (f"{burst}.first", f"{burst}.0", f"{burst}.1")]
    finally:
        release.set()
        loop.set_task_factory(previous)


@pytest.mark.parametrize("workers", [1, 4])
async def test_stop_wakes_worker_waiting_for_admission(runtime, monkeypatch, workers):
    entered = asyncio.Event()

    async def blocked(event):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    first = await _submit_publication(event_type=EventType.INFO, event_name="first")
    await entered.wait()
    queued = [await _submit_publication(event_type=EventType.INFO, event_name="queued") for _ in range(2)]
    started = Event()

    async def worker():
        started.set()
        with pytest.raises(RuntimeError, match="not running"):
            await _submit_publication(event_type=EventType.INFO, event_name="last")

    tasks = [asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker()))) for _ in range(workers)]
    await until(started.is_set)
    await until(lambda: len(runtime.thread_bridge._async_admission) == workers)
    await stop_core()
    await asyncio.wait_for(asyncio.gather(*tasks), 3)
    for future in [first, *queued]:
        with pytest.raises(RuntimeError, match="stopped"):
            future.result()


@pytest.mark.parametrize("cancel", [False, True])
async def test_async_worker_admission_keeps_its_loop_responsive(runtime, monkeypatch, cancel):
    entered, release = asyncio.Event(), asyncio.Event()
    worker_waiting, worker_ticked = Event(), Event()
    cancel_now = Event()
    published = []

    async def blocked(event):
        published.append(event.event_name)
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    first = await _submit_publication(event_type=EventType.INFO, event_name="first")
    await entered.wait()
    queued = [await _submit_publication(event_type=EventType.INFO, event_name="queued") for _ in range(2)]

    async def async_worker():
        task = asyncio.create_task(post_event(event_type=EventType.INFO, event_name="async"))
        worker_waiting.set()
        await asyncio.sleep(0.01)
        worker_ticked.set()
        if cancel:
            while not cancel_now.is_set():
                await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(async_worker())))
    await until(worker_waiting.is_set)
    await until(worker_ticked.is_set)
    if cancel:
        await until(lambda: bool(runtime.thread_bridge._async_admission))
        cancel_now.set()
        await asyncio.wait_for(task, 3)
        assert len(runtime.thread_bridge._requests) == 2
    release.set()
    await asyncio.wait_for(task, 3)
    await asyncio.gather(*(asyncio.wrap_future(future) for future in [first, *queued]))
    assert published == ["first", "queued", "queued"] + ([] if cancel else ["async"])


@pytest.mark.parametrize("wait", [wait_event, wait_event_threadsafe])
async def test_async_worker_waits_on_owner_loop(runtime, wait):
    def worker():
        return asyncio.run(wait("foreign.reply", point="processed", time_out=3))

    task = asyncio.create_task(asyncio.to_thread(worker))
    await until(lambda: bool(runtime.handler_registry.registry))
    await post_event(event_type=EventType.INFO, event_name="foreign.reply", context=42)
    result = await asyncio.wait_for(task, 3)
    assert result.context == 42
    assert not runtime.handler_registry.registry


@pytest.mark.parametrize("waiting_for_admission", [False, True])
@pytest.mark.parametrize("expect", [expect_event, expect_event_threadsafe])
async def test_stop_unblocks_context_before_registration(runtime, monkeypatch, waiting_for_admission, expect):
    entered = asyncio.Event()

    async def blocked(event):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    futures = [await _submit_publication(event_type=EventType.INFO, event_name="blocked")]
    await entered.wait()
    if waiting_for_admission:
        futures.extend([await _submit_publication(event_type=EventType.INFO, event_name="queued") for _ in range(2)])

    async def worker():
        with pytest.raises(RuntimeError, match="not running|stopped"):
            async with expect("never"):
                pytest.fail("The listener must not register behind a blocked publisher.")

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    if waiting_for_admission:
        await until(lambda: bool(runtime.thread_bridge._async_admission))
    else:
        await until(lambda: len(runtime.thread_bridge._requests) == 1)
    await stop_core()
    await asyncio.wait_for(task, 3)
    for future in futures:
        with pytest.raises(RuntimeError, match="stopped"):
            future.result()
    assert not runtime.handler_registry.registry


async def test_cancel_async_worker_after_registration(runtime):
    listening, cancelled = Event(), Event()

    async def async_worker():
        task = asyncio.create_task(wait_event("never"))
        listening.set()
        while not cancelled.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(async_worker())))
    await until(listening.is_set)
    await until(lambda: bool(runtime.handler_registry.registry))
    cancelled.set()
    await asyncio.wait_for(task, 3)
    await until(lambda: not runtime.thread_bridge._active)
    assert not runtime.handler_registry.registry


async def test_different_owner_loop_is_rejected(runtime, monkeypatch):
    other = asyncio.new_event_loop()
    try:
        monkeypatch.setattr(runtime.thread_bridge, "loop", other)
        with pytest.raises(RuntimeError, match="different asyncio loop"):
            factory.get_event_pipe()
        with pytest.raises(RuntimeError, match="different asyncio loop"):
            await start_core()
        with pytest.raises(RuntimeError, match="different asyncio loop"):
            await stop_core()
    finally:
        monkeypatch.setattr(runtime.thread_bridge, "loop", asyncio.get_running_loop())
        other.close()


@pytest.mark.parametrize("expect", [expect_event, expect_event_threadsafe])
async def test_eager_tasks_register_before_publication(runtime, monkeypatch, expect):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        async def worker():
            async with expect("eager", point="processed", time_out=3) as waiter:
                await post_event_threadsafe(event_type=EventType.INFO, event_name="eager")
                assert (await waiter.wait()).event_name == "eager"

        await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)
        assert not runtime.handler_registry.registry
    finally:
        loop.set_task_factory(previous)


@pytest.mark.parametrize("point", ["received", "processed"])
@pytest.mark.parametrize("event_names", ["reply.*", ["reply.*"], {"reply.*", "reply.public"}])
async def test_auto_context_retains_early_matches_and_result_shape(runtime, point, event_names):
    async def worker():
        async with expect_event(
            event_names, point=point, filter="reply.internal", time_out=3,
        ) as waiter:
            assert isinstance(waiter, ThreadEventWaiter)
            await post_and_wait(event_name="reply.internal")
            event = await post_and_wait(event_name="reply.public")
            result = await waiter.wait()
            if isinstance(event_names, str):
                assert result is event
            else:
                assert set(result) == set(event_names)
                assert all(match is event for match in result.values())

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 5)
    assert not runtime.handler_registry.registry
    assert not runtime.thread_bridge._active


@pytest.mark.parametrize("use_context", [False, True])
async def test_auto_worker_wait_rejects_deferred_listener_ownership(runtime, use_context):
    async def worker():
        with pytest.raises(ValueError, match="Cross-thread.*keep_listening"):
            if use_context:
                async with expect_event("never", keep_listening=True):
                    pytest.fail("Deferred ownership must be rejected before registration.")
            else:
                await wait_event("never", keep_listening=True)

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)
    assert not runtime.handler_registry.registry
    assert not runtime.thread_bridge._active


async def test_auto_context_selects_entry_thread_and_cleans_cancelled_wait(runtime):
    context = expect_event("never")
    registered, cancel_now = Event(), Event()

    async def worker():
        async def collect():
            async with context as waiter:
                assert isinstance(waiter, ThreadEventWaiter)
                registered.set()
                await waiter.get_result()

        task = asyncio.create_task(collect())
        while not cancel_now.is_set():
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    try:
        await until(registered.is_set)
        assert runtime.handler_registry.registry
    finally:
        cancel_now.set()
        await asyncio.wait_for(task, 3)
    assert not runtime.handler_registry.registry
    assert not runtime.thread_bridge._active
