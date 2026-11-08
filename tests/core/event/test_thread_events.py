"""Thread bridge contracts, readiness, cancellation and core shutdown."""

import asyncio
from concurrent.futures import CancelledError
from contextvars import ContextVar
from threading import Event, get_ident

import pytest

from apixis import (
    ApixEventPipe, BuiltinChannel, EventType, ThreadEventWaiter,
    await_for, await_for_threadsafe, post_event, post_event_threadsafe, start_core, stop_core,
    subscribe, wait_for_event_threadsafe,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_context


publisher_context = ContextVar("thread_event_test", default=None)


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "_core", None)
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(builtin=BuiltinChannel(maxsize=2)))
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


async def test_roundtrip_and_thread_context_snapshot(runtime):
    owner = get_ident()
    seen = []
    processed = Event()

    @subscribe("thread.request")
    async def respond(event):
        seen.append((get_ident(), publisher_context.get(), event.context))
        # The dispatch has its own chain, never the publisher's inherited marker.
        assert handler_chain_context.get() is not marker
        await runtime.event_pipe.post_event(
            event_type=EventType.INFO, event_name="thread.reply", context=event.context,
        )

    @subscribe("thread.reply")
    async def mark_processed(event):
        event.context["processed"] = True
        processed.set()

    marker = object()

    def worker():
        publisher_context.set("worker-context")
        handler_chain_context.set(marker)
        payload = {"id": 42}
        with wait_for_event_threadsafe("thread.reply", point="processed") as waiter:
            post_event_threadsafe(
                event_type=EventType.INFO, event_name="thread.request", context=payload,
            ).result(3)
            publisher_context.set("changed")
            assert processed.wait(3)
            reply = waiter.wait(time_out=3)
            assert reply.context is payload
            assert reply.context["processed"] is True
        return payload

    payload = await asyncio.wait_for(asyncio.to_thread(worker), 5)
    assert seen == [(owner, "worker-context", payload)]
    assert len(runtime.handler_registry.registry) == 2
    assert runtime.event_loop._event_semaphore._value == 1


async def test_direct_future_filters_and_collects_all_patterns(runtime):
    result = await_for_threadsafe(
        ["job.*", "done"], filter="job.internal", point="processed", time_out=2,
    )
    await until(lambda: len(runtime.handler_registry.registry) == 2)
    for name in ("job.internal", "job.public", "done"):
        await asyncio.wrap_future(post_event_threadsafe(event_type=EventType.INFO, event_name=name))
    events = await asyncio.wrap_future(result)
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
async def test_validation_and_timeout_clean_up(runtime, use_context, options, error):
    def worker():
        with pytest.raises(error):
            if use_context:
                with wait_for_event_threadsafe(**options) as waiter:
                    waiter.wait()
            else:
                await_for_threadsafe(**options).result(3)

    await asyncio.wait_for(asyncio.to_thread(worker), 5)
    await until(lambda: not runtime.thread_bridge._active)
    assert not runtime.handler_registry.registry


async def test_wait_call_timeout_cleans_listeners_before_return(runtime):
    def worker():
        with wait_for_event_threadsafe("never") as waiter:
            with pytest.raises(TimeoutError):
                waiter.wait(time_out=0.01)
        with pytest.raises(RuntimeError, match="already been entered"):
            with waiter:
                pass

    await asyncio.wait_for(asyncio.to_thread(worker), 5)
    assert not runtime.handler_registry.registry


async def test_context_body_exception_closes_waiter(runtime):
    def worker():
        with pytest.raises(LookupError):
            with wait_for_event_threadsafe("never"):
                raise LookupError("body failed")

    await asyncio.wait_for(asyncio.to_thread(worker), 5)
    assert not runtime.handler_registry.registry


@pytest.mark.parametrize("after_registration", [False, True])
async def test_cancel_wait_future(runtime, after_registration):
    result = await_for_threadsafe("never")
    if after_registration:
        await until(lambda: bool(runtime.handler_registry.registry))
    assert result.cancel()
    with pytest.raises(CancelledError):
        result.result()
    await until(lambda: not runtime.thread_bridge._active and not runtime.handler_registry.registry)


async def test_cancel_task_before_its_coroutine_starts(runtime, monkeypatch):
    loop = asyncio.get_running_loop()
    create_task = loop.create_task

    def create_cancelled(coroutine, **kwargs):
        task = create_task(coroutine, **kwargs)
        if coroutine.__qualname__ == "EventThreadBridge._execute":
            task.cancel()
        return task

    monkeypatch.setattr(loop, "create_task", create_cancelled)
    result = await_for_threadsafe("never")
    await until(result.done)
    await until(lambda: not runtime.thread_bridge._active)
    assert result.cancelled()
    assert not runtime.handler_registry.registry


async def test_bounded_admission_and_cancel_before_publication(runtime):
    first = post_event_threadsafe(event_type=EventType.INFO, event_name="first")
    second = post_event_threadsafe(event_type=EventType.INFO, event_name="cancelled")
    with pytest.raises(RuntimeError, match="blocking the main thread"):
        post_event_threadsafe(event_type=EventType.INFO, event_name="overflow")
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
    result = post_event_threadsafe(event_type=EventType.INFO, event_name="blocked")
    await entered.wait()

    def worker():
        with pytest.raises(TimeoutError):
            result.result(0.01)
        assert not result.done()

    await asyncio.to_thread(worker)
    release.set()
    assert await asyncio.wrap_future(result) is None


async def test_stop_fails_pending_waits_and_blocked_publications(runtime, monkeypatch):
    waiting = await_for_threadsafe("never")
    await until(lambda: bool(runtime.handler_registry.registry))
    entered = asyncio.Event()

    async def blocked(event):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    publishing = post_event_threadsafe(event_type=EventType.INFO, event_name="blocked")
    await entered.wait()
    queued = post_event_threadsafe(event_type=EventType.INFO, event_name="queued")
    await asyncio.wait_for(stop_core(), 3)
    for future in (waiting, publishing, queued):
        with pytest.raises(RuntimeError, match="stopped"):
            future.result()
    assert not runtime.handler_registry.registry
    with pytest.raises(RuntimeError, match="not running"):
        await_for_threadsafe("never")
    await start_core()
    assert not runtime.thread_bridge.closed


async def test_shutdown_unblocks_context_entry_and_wait(runtime, monkeypatch):
    entered = Event()

    def worker():
        with pytest.raises(RuntimeError, match="stopped"):
            with wait_for_event_threadsafe("never") as waiter:
                entered.set()
                waiter.wait()

    task = asyncio.create_task(asyncio.to_thread(worker))
    await until(entered.is_set)
    await stop_core()
    await asyncio.wait_for(task, 3)
    assert not runtime.handler_registry.registry


async def test_main_thread_cannot_block_on_thread_waiter(runtime):
    waiter = ThreadEventWaiter("never")
    with pytest.raises(RuntimeError, match="worker thread"):
        with waiter:
            pass
    with pytest.raises(RuntimeError, match="worker thread"):
        waiter.wait()


async def test_worker_cannot_start_core_or_access_registry(runtime):
    def worker():
        with pytest.raises(RuntimeError, match="main thread"):
            factory.get_handler_registry()
        with pytest.raises(RuntimeError, match="main thread"):
            asyncio.run(start_core())

    await asyncio.to_thread(worker)


async def test_uninitialized_bridge_does_not_construct_core(runtime, monkeypatch):
    monkeypatch.setattr(factory, "_core", None)
    with pytest.raises(RuntimeError, match="start_core"):
        await_for_threadsafe("never")
    assert factory._core is None


async def test_worker_waits_for_full_bridge_without_retry(runtime, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def blocked(event):
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    first = post_event_threadsafe(event_type=EventType.INFO, event_name="first")
    await entered.wait()
    queued = [post_event_threadsafe(event_type=EventType.INFO, event_name="queued") for _ in range(2)]
    started, admitted = Event(), Event()

    def worker():
        started.set()
        future = post_event_threadsafe(event_type=EventType.INFO, event_name="last")
        admitted.set()
        future.result(3)

    task = asyncio.create_task(asyncio.to_thread(worker))
    await until(started.is_set)
    await until(lambda: bool(runtime.thread_bridge._admission._waiters))
    assert not admitted.is_set()
    release.set()
    await asyncio.wait_for(task, 3)
    await asyncio.gather(*(asyncio.wrap_future(future) for future in [first, *queued]))


async def test_stop_wakes_worker_waiting_for_admission(runtime, monkeypatch):
    entered = asyncio.Event()

    async def blocked(event):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    first = post_event_threadsafe(event_type=EventType.INFO, event_name="first")
    await entered.wait()
    queued = [post_event_threadsafe(event_type=EventType.INFO, event_name="queued") for _ in range(2)]
    started = Event()

    def worker():
        started.set()
        with pytest.raises(RuntimeError, match="not running"):
            post_event_threadsafe(event_type=EventType.INFO, event_name="last")

    task = asyncio.create_task(asyncio.to_thread(worker))
    await until(started.is_set)
    await until(lambda: bool(runtime.thread_bridge._admission._waiters))
    await stop_core()
    await asyncio.wait_for(task, 3)
    for future in [first, *queued]:
        with pytest.raises(RuntimeError, match="stopped"):
            future.result()


@pytest.mark.parametrize("cancel", [False, True])
async def test_async_worker_admission_keeps_its_loop_responsive(runtime, monkeypatch, cancel):
    entered, release = asyncio.Event(), asyncio.Event()
    worker_waiting, worker_ticked = Event(), Event()
    published = []

    async def blocked(event):
        published.append(event.event_name)
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    first = post_event_threadsafe(event_type=EventType.INFO, event_name="first")
    await entered.wait()
    queued = [post_event_threadsafe(event_type=EventType.INFO, event_name="queued") for _ in range(2)]

    async def async_worker():
        task = asyncio.create_task(post_event(event_type=EventType.INFO, event_name="async"))
        worker_waiting.set()
        await asyncio.sleep(0.01)
        worker_ticked.set()
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(async_worker())))
    await until(worker_waiting.is_set)
    await until(worker_ticked.is_set)
    if cancel:
        await asyncio.wait_for(task, 3)
        assert runtime.thread_bridge._requests.qsize() == 2
    release.set()
    await asyncio.wait_for(task, 3)
    await asyncio.gather(*(asyncio.wrap_future(future) for future in [first, *queued]))
    assert published == ["first", "queued", "queued"] + ([] if cancel else ["async"])


async def test_async_worker_waits_on_owner_loop(runtime):
    def worker():
        return asyncio.run(await_for("foreign.reply", point="processed", time_out=3))

    task = asyncio.create_task(asyncio.to_thread(worker))
    await until(lambda: bool(runtime.handler_registry.registry))
    await post_event(event_type=EventType.INFO, event_name="foreign.reply", context=42)
    result = await asyncio.wait_for(task, 3)
    assert result.context == 42
    assert not runtime.handler_registry.registry


@pytest.mark.parametrize("waiting_for_admission", [False, True])
async def test_stop_unblocks_context_before_registration(runtime, monkeypatch, waiting_for_admission):
    entered = asyncio.Event()

    async def blocked(event):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    futures = [post_event_threadsafe(event_type=EventType.INFO, event_name="blocked")]
    await entered.wait()
    if waiting_for_admission:
        futures.extend(post_event_threadsafe(event_type=EventType.INFO, event_name="queued") for _ in range(2))

    def worker():
        with pytest.raises(RuntimeError, match="not running|stopped"):
            with wait_for_event_threadsafe("never"):
                pytest.fail("The listener must not register behind a blocked publisher.")

    task = asyncio.create_task(asyncio.to_thread(worker))
    if waiting_for_admission:
        await until(lambda: bool(runtime.thread_bridge._admission._waiters))
    else:
        await until(lambda: runtime.thread_bridge._requests.qsize() == 1)
    await stop_core()
    await asyncio.wait_for(task, 3)
    for future in futures:
        with pytest.raises(RuntimeError, match="stopped"):
            future.result()
    assert not runtime.handler_registry.registry


async def test_cancel_async_worker_after_registration(runtime):
    listening, cancelled = Event(), Event()

    async def async_worker():
        task = asyncio.create_task(await_for("never"))
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


async def test_eager_tasks_register_before_publication(runtime, monkeypatch):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        def worker():
            with wait_for_event_threadsafe("eager", point="processed", time_out=3) as waiter:
                post_event_threadsafe(event_type=EventType.INFO, event_name="eager").result(3)
                assert waiter.wait().event_name == "eager"

        await asyncio.wait_for(asyncio.to_thread(worker), 3)
        assert not runtime.handler_registry.registry
    finally:
        loop.set_task_factory(previous)
