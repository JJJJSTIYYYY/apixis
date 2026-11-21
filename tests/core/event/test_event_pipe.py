"""Tests for local event queues and publication."""

import asyncio
from contextvars import ContextVar
import gc
import time
import weakref

import pytest

from apixis.core.event import EventType, subscribe
from apixis.core.event.factory import get_event_pipe
from apixis.core.event.event_pipe import ApixEventPipe


_publication_context = ContextVar("event_pipe_test_context", default=None)


class TestApixEventPipeQueue:
    @pytest.mark.asyncio
    async def test_queue_compatible_methods(self):
        pipe = ApixEventPipe(maxsize=2)

        await pipe.put("first")
        pipe.put_nowait("second")

        assert pipe.maxsize == 2
        assert pipe.qsize() == 2
        assert pipe.full() is True
        assert await pipe.get() == "first"
        assert pipe.get_nowait() == "second"
        pipe.task_done()
        pipe.task_done()
        await asyncio.wait_for(pipe.join(), timeout=0.1)
        assert pipe.empty() is True

    @pytest.mark.asyncio
    async def test_global_pipe_is_apix_event_pipe_singleton(self):
        from apixis.core.event.factory import get_event_pipe as second_import

        assert isinstance(get_event_pipe(), ApixEventPipe)
        assert get_event_pipe() is second_import()
        from apixis.core.config.core_config import EVENT_PIPE_MAX_LEN
        assert get_event_pipe().maxsize == EVENT_PIPE_MAX_LEN

        while not get_event_pipe().empty():
            get_event_pipe().get_nowait()
            get_event_pipe().task_done()
        await get_event_pipe().put("event")
        assert await get_event_pipe().get() == "event"
        get_event_pipe().task_done()


class TestApixEventPipeEvents:
    def test_unbounded_pipe_is_rejected(self):
        """Local event pipes must preserve bounded admission."""
        with pytest.raises(ValueError, match="maxsize"):
            ApixEventPipe(maxsize=0)

    @pytest.mark.asyncio
    async def test_post_event_builds_event_with_current_timestamp(self):
        pipe = ApixEventPipe()
        before = time.time()

        await pipe.post_event(
            event_name="workflow.started",
            context={"run_id": "run-1"},
        )
        event = await pipe.get()

        assert event.event_id.startswith("event-")
        assert event.event_type is EventType.DEFAULT
        assert event.event_name == "workflow.started"
        assert event.context == {"run_id": "run-1"}
        assert before <= event.timestamp <= time.time()
        assert event.accepted is False
        pipe.task_done()

    @pytest.mark.asyncio
    async def test_post_event_preserves_fifo_order(self):
        pipe = ApixEventPipe()
        for name in ("event.1", "event.2", "event.3"):
            await pipe.post_event(
                event_type=EventType.INFO,
                event_name=name,
            )

        events = [await pipe.get() for _ in range(3)]
        assert [event.event_name for event in events] == [
            "event.1",
            "event.2",
            "event.3",
        ]
        for _ in events:
            pipe.task_done()

    async def test_publication_captures_independent_context_bindings(
        self, cleanup_event_runtime, wait_for_dispatch,
    ):
        core = cleanup_event_runtime
        pipe = core.event_pipe
        await core.event_loop.stop()
        observed = {}

        @subscribe("context.*", priority=10)
        async def observe_publication(event):
            observed[event.event_name] = [_publication_context.get()]
            _publication_context.set("handler")

        @subscribe("context.*")
        async def observe_handler_change(event):
            observed[event.event_name].append(_publication_context.get())

        token = _publication_context.set("first")
        try:
            await pipe.post_event(event_type=EventType.INFO, event_name="context.first")
            _publication_context.set("second")
            await pipe.post_event(event_type=EventType.INFO, event_name="context.second")
        finally:
            _publication_context.reset(token)

        await core.event_loop.start()
        await wait_for_dispatch(core.event_loop)
        assert observed == {
            "context.first": ["first", "handler"],
            "context.second": ["second", "handler"],
        }
        assert _publication_context.get() is None

    async def test_retained_event_releases_publisher_context_after_dispatch(
        self, cleanup_event_runtime, wait_for_dispatch,
    ):
        class Request:
            pass

        events = []

        @subscribe("context.retained")
        async def retain_event(event):
            assert isinstance(_publication_context.get(), Request)
            events.append(event)

        request = Request()
        reference = weakref.ref(request)
        token = _publication_context.set(request)
        try:
            await cleanup_event_runtime.event_pipe.post_event(
                event_type=EventType.INFO, event_name="context.retained",
            )
        finally:
            _publication_context.reset(token)
        del request

        await wait_for_dispatch(cleanup_event_runtime.event_loop)
        gc.collect()
        assert len(events) == 1
        assert reference() is None

    @pytest.mark.asyncio
    async def test_clear_acknowledges_all_queued_events(self):
        pipe = ApixEventPipe()
        await pipe.post_event(event_type=EventType.INFO, event_name="event.1")
        await pipe.post_event(event_type=EventType.INFO, event_name="event.2")

        assert await pipe.clear() == 2
        assert pipe.empty()
        await asyncio.wait_for(pipe.join(), timeout=0.1)
        assert await pipe.clear() == 0



async def test_pipe_close_rejects_all_admission_and_wakes_full_queue_publishers():
    from apixis import CoreClosedError

    pipe = ApixEventPipe(maxsize=1)
    accepted = object()
    pipe.put_nowait(accepted)
    publishers = [asyncio.create_task(pipe.put(object())) for _ in range(8)]
    await asyncio.sleep(0)
    pipe.close()
    pipe.close()
    for publisher in publishers:
        with pytest.raises(CoreClosedError):
            await asyncio.wait_for(publisher, 1)
    with pytest.raises(CoreClosedError):
        pipe.put_nowait(object())
    with pytest.raises(CoreClosedError):
        await pipe.put(object())
    with pytest.raises(CoreClosedError):
        await pipe.post_event(event_name="closed")
    assert pipe.qsize() == 1
    assert pipe.get_nowait() is accepted
    pipe.task_done()
    await asyncio.wait_for(pipe.join(), 1)
    assert not pipe._blocked_puts


async def test_cancelled_publisher_keeps_application_cancellation_semantics():
    pipe = ApixEventPipe(maxsize=1)
    pipe.put_nowait(object())
    publisher = asyncio.create_task(pipe.put(object()))
    await asyncio.sleep(0)
    publisher.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publisher
    assert not pipe._blocked_puts
    assert pipe.qsize() == 1
