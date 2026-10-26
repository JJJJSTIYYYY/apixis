"""Tests for local event queues and publication."""

import asyncio
import time

import pytest

from apixis.core.event import BuiltinChannel, EventType
from apixis.core.event.factory import get_event_pipe
from apixis.core.event.event_pipe import ApixEventPipe


class TestBuiltinChannel:
    @pytest.mark.asyncio
    async def test_queue_compatible_methods(self):
        channel = BuiltinChannel(maxsize=2)

        await channel.put("first")
        channel.put_nowait("second")

        assert channel.maxsize == 2
        assert channel.qsize() == 2
        assert channel.full() is True
        assert await channel.get() == "first"
        assert channel.get_nowait() == "second"
        channel.task_done()
        channel.task_done()
        await asyncio.wait_for(channel.join(), timeout=0.1)
        assert channel.empty() is True

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
    def test_unbounded_builtin_is_rejected(self):
        """Custom local channels must preserve bounded admission."""
        with pytest.raises(ValueError, match="builtin channel maxsize"):
            ApixEventPipe(builtin=BuiltinChannel(maxsize=0))

    @pytest.mark.asyncio
    async def test_post_event_builds_event_with_current_timestamp(self):
        pipe = ApixEventPipe()
        before = time.time()

        await pipe.post_event(
            event_type=EventType.WORKFLOW,
            event_name="workflow.started",
            context={"run_id": "run-1"},
        )
        event = await pipe.get()

        assert event.event_id.startswith("event-")
        assert event.event_type is EventType.WORKFLOW
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

    @pytest.mark.asyncio
    async def test_clear_acknowledges_all_queued_events(self):
        pipe = ApixEventPipe()
        await pipe.post_event(event_type=EventType.INFO, event_name="event.1")
        await pipe.post_event(event_type=EventType.INFO, event_name="event.2")

        assert await pipe.clear() == 2
        assert pipe.empty()
        await asyncio.wait_for(pipe.join(), timeout=0.1)
        assert await pipe.clear() == 0



async def test_pipe_lifecycle_preserves_pending_events_and_acknowledgements():
    """Repeated local lifecycle transitions retain queue ownership and order."""
    channel = BuiltinChannel(maxsize=2)
    pipe = ApixEventPipe(builtin=channel)
    first, second = object(), object()
    pipe.put_nowait(first)
    pipe.put_nowait(second)
    assert pipe.get_channel() is channel
    assert not pipe.is_running
    await asyncio.gather(*(pipe.start() for _ in range(8)))
    assert pipe.is_running
    await asyncio.wait_for(asyncio.gather(*(pipe.stop() for _ in range(8))), 1)
    assert not pipe.is_running
    assert pipe.full()
    await pipe.start()
    assert await pipe.get() is first
    pipe.task_done()
    assert pipe.get_nowait() is second
    pipe.task_done()
    await asyncio.wait_for(pipe.join(), 1)
    await pipe.stop()


async def test_local_context_preserves_arbitrary_python_objects():
    """Local event payloads are passed by reference without serialization."""
    context = {"object": object(), "values": {1, 2}}
    pipe = ApixEventPipe()
    await pipe.post_event(event_type=EventType.INFO, event_name="local.context", context=context)
    event = await pipe.get()
    assert event.context is context
    assert event.context["object"] is context["object"]
    pipe.task_done()
    await pipe.join()


async def test_nowait_queue_errors_and_join_wait_for_acknowledgement():
    pipe = ApixEventPipe(builtin=BuiltinChannel(maxsize=1))
    pipe.put_nowait("first")
    with pytest.raises(asyncio.QueueFull):
        pipe.put_nowait("second")
    assert pipe.get_nowait() == "first"
    with pytest.raises(asyncio.QueueEmpty):
        pipe.get_nowait()
    joined = asyncio.create_task(pipe.join())
    await asyncio.sleep(0)
    assert not joined.done()
    pipe.task_done()
    await asyncio.wait_for(joined, 1)
