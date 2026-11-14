"""Real OS-thread stress at bounded and unbounded event dispatch capacities."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import importlib
from pathlib import Path
import platform
from threading import Barrier, get_ident
from time import perf_counter

import pytest

from apixis import (
    ApixEventPipe, EventType, wait_event,
    post_event, post_event_threadsafe, subscribe, expect_event,
)
from apixis.core.event import factory


pytestmark = pytest.mark.parametrize("event_runtime_capacity", [1, 128, None], indirect=True)


@pytest.fixture
def event_runtime_capacity(request, monkeypatch):
    loop_module = importlib.import_module("apixis.core.event.event_loop")
    monkeypatch.setattr(loop_module, "EVENT_LOOP_BACKPRESSURE", request.param)
    monkeypatch.setattr(factory, "_core", None)
    queue_size = request.config.getoption("--thread-queue-size")
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=queue_size))
    return request.param


@pytest.fixture
async def thread_runtime(cleanup_event_runtime, request):
    core = cleanup_event_runtime
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    errors = []
    loop.set_exception_handler(lambda _, context: errors.append(context))
    baseline = set(core.handler_registry.registry)
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.001)

    pulse = asyncio.create_task(heartbeat())
    started = perf_counter()
    metrics = {}
    try:
        yield core, metrics
        async with asyncio.timeout(10):
            while core.thread_bridge._active or core.thread_bridge._consumer_scheduled:
                await asyncio.sleep(0)
            await core.event_pipe.join()
            while core.event_loop._dispatch_tasks:
                await asyncio.sleep(0)
        assert set(core.handler_registry.registry) == baseline | metrics.pop("handlers", set())
        assert not core.thread_bridge._active
        assert not core.thread_bridge._requests
        assert not core.thread_bridge._async_admission
        assert not core.thread_bridge._consumer_scheduled
        assert not core.event_loop._background_handler_tasks
        assert core.event_loop._pending_dispatch is None
        semaphore = core.event_loop._event_semaphore
        expected = semaphore._bounded
        if expected is None:
            assert semaphore._unbounded_value == 0
        else:
            assert semaphore._value == expected
        assert not errors, errors
        assert ticks > 0
        metrics.update(
            scenario=request.node.name,
            dispatch_capacity=expected,
            queue_capacity=core.event_pipe.maxsize,
            asyncio_debug=loop.get_debug(),
            seconds=perf_counter() - started,
            heartbeat_ticks=ticks,
            callback_errors=len(errors),
            leftover_requests=len(core.thread_bridge._requests),
            leftover_waiters=len(set(core.handler_registry.registry) - baseline - set(metrics.get("handler_names", []))),
        )
        print(json.dumps(metrics, sort_keys=True))
        destination = request.config.getoption("--thread-report")
        if destination:
            reports = getattr(request.config, "_thread_metrics", [])
            reports.append(metrics)
            request.config._thread_metrics = reports
            path = Path(destination)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({
                "python": platform.python_version(), "platform": platform.system(),
                "results": reports,
            }, indent=2) + "\n", encoding="utf-8")
    finally:
        pulse.cancel()
        await asyncio.gather(pulse, return_exceptions=True)
        await core.thread_bridge.close()
        loop.set_exception_handler(previous)


async def run_workers(count, worker):
    async def run_one(index):
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=2))
        return await worker(index)

    def run():
        with ThreadPoolExecutor(max_workers=count) as pool:
            return list(pool.map(lambda index: asyncio.run(run_one(index)), range(count)))

    return await asyncio.wait_for(asyncio.to_thread(run), 120)


async def test_thread_publication_flood(thread_runtime, request):
    core, metrics = thread_runtime
    workers = request.config.getoption("--thread-workers") or 256
    events = request.config.getoption("--thread-iterations") or 128
    start = Barrier(workers)
    main_thread = get_ident()
    last = [-1] * workers
    count = 0

    @subscribe("thread.flood")
    async def receive(event):
        nonlocal count
        assert get_ident() == main_thread
        producer, sequence = event.context
        assert sequence == last[producer] + 1
        last[producer] = sequence
        count += 1

    async def worker(producer):
        await asyncio.to_thread(start.wait, 60)
        for sequence in range(events):
            await post_event_threadsafe(
                event_type=EventType.TRANSIENT, event_name="thread.flood", context=(producer, sequence),
            )

    await run_workers(workers, worker)
    async with asyncio.timeout(10):
        while count != workers * events:
            await asyncio.sleep(0)
    assert last == [events - 1] * workers
    metrics.update(workers=workers, events=count,
                   handlers={receive.__name__}, handler_names=[receive.__name__])


async def test_thread_request_reply_stress(thread_runtime, request):
    core, metrics = thread_runtime
    workers = request.config.getoption("--thread-workers") or 128
    rounds = request.config.getoption("--thread-iterations") or 64
    start = Barrier(workers)
    requests = 0
    main_thread = get_ident()

    @subscribe("thread.request")
    async def respond(event):
        nonlocal requests
        assert get_ident() == main_thread
        requests += 1
        await core.event_pipe.post_event(
            event_type=EventType.TRANSIENT, event_name=event.context["reply"], context=event.context,
        )

    async def worker(producer):
        await asyncio.to_thread(start.wait, 60)
        for sequence in range(rounds):
            reply = f"thread.reply.{producer}.{sequence}"
            async with expect_event(reply, point="processed", time_out=60) as waiter:
                await post_event_threadsafe(
                    event_type=EventType.TRANSIENT, event_name="thread.request",
                    context={"reply": reply, "producer": producer, "sequence": sequence},
                )
                event = await waiter.wait(time_out=60)
                assert event.event_name == reply
                assert event.context["producer"] == producer
                assert event.context["sequence"] == sequence

    await run_workers(workers, worker)
    assert requests == workers * rounds
    metrics.update(workers=workers, roundtrips=requests, events=requests * 2,
                   handlers={respond.__name__}, handler_names=[respond.__name__])


async def test_thread_broadcast_and_cancellation_storm(thread_runtime, request):
    core, metrics = thread_runtime
    workers = request.config.getoption("--thread-workers") or 256
    cancellations = request.config.getoption("--thread-iterations") or 32
    registered = Barrier(workers)

    async def worker(producer):
        async with expect_event("thread.broadcast", time_out=60) as waiter:
            leader = await asyncio.to_thread(registered.wait, 60)
            if leader == 0:
                await post_event_threadsafe(
                    event_type=EventType.TRANSIENT, event_name="thread.broadcast", context=42,
                )
            event = await waiter.wait(time_out=60)
            assert event.context == 42
        for _ in range(cancellations):
            task = asyncio.create_task(wait_event("never"))
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return event.event_id

    results = await run_workers(workers, worker)
    assert len(set(results)) == 1
    metrics.update(workers=workers, broadcast_recipients=workers, cancelled_waits=workers * cancellations,
                   events=1)


async def test_async_worker_publication_backpressure(thread_runtime, request):
    core, metrics = thread_runtime
    workers = request.config.getoption("--thread-workers") or 16
    operations = request.config.getoption("--thread-iterations") or 128
    start = Barrier(workers)
    received = set()
    count = 0

    @subscribe("thread.async")
    async def receive(event):
        nonlocal count
        received.add(event.context)
        count += 1

    async def worker(producer):
        await asyncio.to_thread(start.wait, 60)
        await asyncio.gather(*(post_event(
            event_type=EventType.TRANSIENT, event_name="thread.async", context=(producer, sequence),
        ) for sequence in range(operations)))

    await run_workers(workers, worker)
    async with asyncio.timeout(10):
        while count != workers * operations:
            await asyncio.sleep(0)
    assert len(received) == count
    metrics.update(workers=workers, async_operations_per_worker=operations, events=count,
                   handlers={receive.__name__}, handler_names=[receive.__name__])
