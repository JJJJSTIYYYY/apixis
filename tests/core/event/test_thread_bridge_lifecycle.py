"""Ownership of bridge admission, queued work and routed shutdown."""

import asyncio
import gc
from weakref import ref

import pytest

from apixis import get_thread_bridge, post_event, stop_core, subscribe
from apixis.core.event.thread_bridge import ThreadEndpoint


async def test_completed_result_does_not_retain_endpoint():
    async def operation():
        return "completed"

    endpoint = ThreadEndpoint(asyncio.get_running_loop(), maxsize=1)
    result = (await endpoint.asubmit(operation)).result
    async with asyncio.timeout(2):
        assert await asyncio.wrap_future(result) == "completed"
        await endpoint.close()
    endpoint_ref = ref(endpoint)
    del endpoint
    gc.collect()
    assert endpoint_ref() is None
    assert result.result() == "completed"


async def test_cancelling_queued_request_returns_capacity_before_consumer_resumes():
    endpoint = ThreadEndpoint(asyncio.get_running_loop(), maxsize=1)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked():
        entered.set()
        await release.wait()

    async def should_not_run():
        pytest.fail("A cancelled queued operation must not execute.")

    async def following():
        return "completed"

    try:
        await endpoint.asubmit(blocked)
        async with asyncio.timeout(2):
            await entered.wait()
            cancelled = await endpoint.asubmit(should_not_run)
            waiting = asyncio.create_task(endpoint.asubmit(following))
            while not endpoint.get_status().admission_waiters:
                await asyncio.sleep(0)
            cancelled.result.cancel()
            admitted = await waiting
            assert cancelled.closed.done()
            assert not admitted.result.done()
            release.set()
            assert await asyncio.wrap_future(admitted.result) == "completed"
            assert admitted.closed.done()
    finally:
        await endpoint.close()


async def test_cancelling_admission_does_not_submit_receiver_cleanup(monkeypatch):
    endpoint = ThreadEndpoint(asyncio.get_running_loop(), maxsize=1)
    cancellations = []
    monkeypatch.setattr(endpoint, "_cancel", cancellations.append)

    async def blocked():
        await asyncio.Event().wait()

    try:
        async with asyncio.timeout(2):
            await endpoint.asubmit(blocked)
            while not endpoint.get_status().active_requests:
                await asyncio.sleep(0)
            await endpoint.asubmit(blocked)
            waiting = asyncio.create_task(endpoint.asubmit(blocked))
            while not endpoint.get_status().admission_waiters:
                await asyncio.sleep(0)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            assert not cancellations
            assert endpoint.get_status().admission_waiters == 0
    finally:
        await endpoint.close()


@pytest.mark.parametrize("eager", [False, True])
async def test_routed_shutdown_returns_its_result_and_releases_other_requests(eager):
    loop = asyncio.get_running_loop()
    endpoint = ThreadEndpoint(loop, maxsize=1)
    previous = loop.get_task_factory()
    entered = asyncio.Event()
    released = asyncio.Event()

    async def blocked():
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            await endpoint.close()
            released.set()

    async def shutdown():
        await asyncio.create_task(endpoint.close())
        return "stopped"

    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        async with asyncio.timeout(2):
            active = await endpoint.asubmit(blocked, background=True)
            await entered.wait()
            assert await endpoint.call(shutdown) == "stopped"
            assert released.is_set()
            assert active.closed.done()
            with pytest.raises(RuntimeError, match="stopped"):
                await asyncio.wrap_future(active.result)
            assert endpoint.get_status().active_requests == 0
    finally:
        loop.set_task_factory(previous)
        await endpoint.close()


@pytest.mark.parametrize("caller_cancels", [False, True])
async def test_cancelling_stop_does_not_interrupt_request_cleanup(caller_cancels):
    endpoint = ThreadEndpoint(asyncio.get_running_loop(), maxsize=1)
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    cleaned = asyncio.Event()

    async def blocked():
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            cleaned.set()

    try:
        async with asyncio.timeout(2):
            request = await endpoint.asubmit(blocked)
            await entered.wait()
            if caller_cancels:
                request.result.cancel()
                await cleaning.wait()
            stop = asyncio.create_task(endpoint.close())
            while not endpoint.closed:
                await asyncio.sleep(0)
            await cleaning.wait()
            stop.cancel()
            with pytest.raises(asyncio.CancelledError):
                await stop
            release.set()
            await asyncio.wrap_future(request.closed)
            assert cleaned.is_set()
    finally:
        release.set()
        await endpoint.close()


async def test_request_task_creation_failure_releases_request():
    loop = asyncio.get_running_loop()
    endpoint = ThreadEndpoint(loop, maxsize=1)
    fail = True

    def task_factory(loop, coroutine, *, context=None):
        nonlocal fail
        if coroutine.__qualname__ == "ThreadEndpoint._execute" and fail:
            fail = False
            raise RuntimeError("cannot start operation")
        return asyncio.Task(coroutine, loop=loop, context=context)

    async def operation():
        return "completed"

    previous = loop.get_task_factory()
    loop.set_task_factory(task_factory)
    try:
        async with asyncio.timeout(2):
            with pytest.raises(RuntimeError, match="cannot start operation"):
                await endpoint.call(operation)
            assert endpoint.get_status().active_requests == 0
            assert await endpoint.call(operation) == "completed"
    finally:
        loop.set_task_factory(previous)
        await endpoint.close()


async def test_event_handler_shutdown_does_not_inherit_the_publisher_request():
    stopped = asyncio.Event()

    @subscribe("stop.from.handler")
    async def shutdown(event):
        await stop_core()
        stopped.set()

    async def publish_then_wait():
        await post_event(event_name="stop.from.handler")
        await asyncio.Event().wait()

    async with asyncio.timeout(2):
        with pytest.raises(RuntimeError, match="stopped"):
            await get_thread_bridge().call(publish_then_wait)
        await stopped.wait()
