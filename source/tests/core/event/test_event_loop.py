"""
Tests for event_loop module.

Covers ApixEventLoop: start/stop lifecycle, event consumption,
dispatch logic, background handlers, error handling, and timeouts.
"""

import asyncio
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from apixis.core.event import ApixEventPipe, get_event_registry

from apixis.core.event.base import ApixEvent, EventType, ApixEventHandler
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.config.core_config import EVENT_LOOP_BACKPRESSURE


# ============================
# Helpers
# ============================


def _reset_registry(registry: ApixHandlerRegistry):
    """Reset registry to clean state."""
    registry.registry.clear()
    registry.priority_buckets.clear()
    registry.cached_chain.clear()
    registry._register_order = 0


def _make_handler_entry(
    name="test_handler",
    subscribe=None,
    callback=None,
    priority=1.0,
    register_order=0,
    stop_when_error=True,
    time_out=30.0,
    background=False,
):
    """Create a ApixEventHandler."""
    if callback is None:
        callback = AsyncMock()

    entry = ApixEventHandler(
        callback,
        stop_when_error=stop_when_error,
        time_out=time_out,
        background=background,
    )
    entry.id = f"id_{name}"
    entry.name = name
    entry.subscribe = list(subscribe or ["test.event"])
    entry.priority = priority
    entry._register_order = register_order
    return entry


def _make_event(event_name="test.event", accepted=False):
    """Create a test ApixEvent."""
    return ApixEvent(
        event_id="event-"+uuid4().hex,
        event_type=EventType.WORKFLOW,
        event_name=event_name,
        context=None,
        timestamp=0.0,
        accepted=accepted,
    )


# ============================
# Tests: start / stop
# ============================


class TestStartStop:
    """Tests for start() and stop() lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_consumer_task(self):
        """start() should create a consumer asyncio.Task."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        with patch.object(
            handler, "_event_consumer_loop", AsyncMock()
        ) as mock_loop:
            await handler.start()
            assert handler._event_consumer_task is not None
            mock_loop.assert_called_once()

        await handler.stop()

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        """Calling start() multiple times should not create multiple tasks."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        with patch.object(handler, "_event_consumer_loop", AsyncMock()):
            await handler.start()
            task1 = handler._event_consumer_task
            await handler.start()
            task2 = handler._event_consumer_task
            assert task1 is task2

        await handler.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_consumer_task(self):
        """stop() should cancel the consumer task."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def block_forever():
            try:
                while True:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                raise

        handler._event_consumer_loop = block_forever
        await handler.start()
        assert handler._event_consumer_task is not None

        await handler.stop()
        assert handler._event_consumer_task is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started_is_safe(self):
        """Calling stop() when not started should not raise."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        await handler.stop()

    @pytest.mark.asyncio
    async def test_stop_preserves_dispatch_tasks(self):
        """stop() should leave pending dispatch tasks running."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def slow_dispatch():
            await asyncio.sleep(10)

        task = asyncio.create_task(slow_dispatch())
        handler._dispatch_tasks.add(task)

        await handler.stop()
        assert not task.done()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_stop_preserves_background_tasks(self):
        """stop() should leave pending background handler tasks running."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def slow_background():
            await asyncio.sleep(10)

        task = asyncio.create_task(slow_background())
        handler._background_handler_tasks.add(task)

        await handler.stop()
        assert not task.done()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# ============================
# Tests: _dispatch_event
# ============================


class TestDispatchEvent:
    """Tests for _dispatch_event method."""

    @pytest.mark.asyncio
    async def test_dispatch_empty_event_name_returns_none(self):
        """Event with empty event_name should return None."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        event = _make_event(event_name="")
        result = await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_dispatch_no_handlers_returns_event(self):
        """
        When no handlers are registered, event is returned as-is.
        Dispatch does not implicitly accept events.
        """
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        event = _make_event()
        result = await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )
        assert result is event
        # Dispatch preserves the explicit acceptance state.
        assert result.accepted is False

    @pytest.mark.asyncio
    async def test_dispatch_calls_handler(self):
        """Registered handler should be called with the event."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        mock_callback = AsyncMock()
        entry = _make_handler_entry(callback=mock_callback)
        registry.register_handler(entry)

        event = _make_event()
        result = await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )

        mock_callback.assert_awaited_once_with(event)
        assert result.accepted is False

    @pytest.mark.asyncio
    async def test_dispatch_multiple_handlers_called_in_order(self):
        """Multiple handlers should be called in registry order."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        call_order = []

        async def h1(event):
            call_order.append("h1")

        async def h2(event):
            call_order.append("h2")

        entry1 = _make_handler_entry(name="h1", callback=h1, priority=10.0)
        entry2 = _make_handler_entry(name="h2", callback=h2, priority=5.0)
        registry.register_handler(entry1)
        registry.register_handler(entry2)

        event = _make_event()
        await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )

        assert call_order == ["h1", "h2"]

    @pytest.mark.asyncio
    async def test_dispatch_event_accepted_skips_remaining_cores(self):
        """When event.accepted is True, remaining core functions are skipped."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        called = []

        async def h1(event):
            called.append("h1")
            event.accept()

        async def h2(event):
            called.append("h2")

        entry1 = _make_handler_entry(name="h1", callback=h1, priority=10.0)
        entry2 = _make_handler_entry(name="h2", callback=h2, priority=5.0)
        registry.register_handler(entry1)
        registry.register_handler(entry2)

        event = _make_event()
        await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )

        assert called == ["h1"]

    @pytest.mark.asyncio
    async def test_dispatch_event_already_accepted_skips_core(self):
        """If event is already accepted, its core function is not called."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        mock_callback = AsyncMock()
        entry = _make_handler_entry(callback=mock_callback)
        registry.register_handler(entry)

        event = _make_event(accepted=True)
        result = await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )

        mock_callback.assert_not_awaited()
        assert result.accepted is True

    @pytest.mark.asyncio
    async def test_dispatch_handler_timeout_logs_error(self):
        """Handler timeout should log an error and continue."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def slow_handler(event):
            await asyncio.sleep(10)

        entry = _make_handler_entry(callback=slow_handler, time_out=0.001)
        registry.register_handler(entry)

        event = _make_event()

        with patch("apixis.core.event.base.logger") as mock_logger:
            result = await handler._dispatch_event(
                event,
                handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
            )

            error_calls = [
                c for c in mock_logger.error.call_args_list
                if "timeout" in str(c).lower()
            ]
            assert len(error_calls) >= 1
            assert result.accepted is False

    @pytest.mark.asyncio
    async def test_dispatch_handler_exception_logs_error(self):
        """Handler exception should log an error."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def failing_handler(event):
            raise ValueError("test error")

        entry = _make_handler_entry(callback=failing_handler)
        registry.register_handler(entry)

        event = _make_event()

        with patch("apixis.core.event.base.logger") as mock_logger:
            result = await handler._dispatch_event(
                event,
                handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
            )
            assert result.accepted is False
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_upstream_stop_flag_does_not_control_next_handler(self):
        """The next handler uses its own stop_when_error setting."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        called = []

        async def h1(event):
            called.append("h1")
            raise ValueError("error")

        async def h2(event):
            called.append("h2")

        entry1 = _make_handler_entry(name="h1", callback=h1, stop_when_error=True)
        entry2 = _make_handler_entry(name="h2", callback=h2, stop_when_error=False)
        registry.register_handler(entry1)
        registry.register_handler(entry2)

        event = _make_event()

        with patch("apixis.core.event.base.logger"):
            await handler._dispatch_event(
                event,
                handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
            )

        assert called == ["h1", "h2"]

    @pytest.mark.asyncio
    async def test_dispatch_handler_stop_when_error_false_continues(self):
        """A later handler with stop_when_error=False runs despite upstream errors."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        called = []

        async def h1(event):
            called.append("h1")
            raise ValueError("error")

        async def h2(event):
            called.append("h2")

        entry1 = _make_handler_entry(name="h1", callback=h1, stop_when_error=False)
        entry2 = _make_handler_entry(name="h2", callback=h2, stop_when_error=False)
        registry.register_handler(entry1)
        registry.register_handler(entry2)

        event = _make_event()

        with patch("apixis.core.event.base.logger"):
            await handler._dispatch_event(
                event,
                handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
            )

        assert called == ["h1", "h2"]

    @pytest.mark.asyncio
    async def test_dispatch_background_handler_creates_task(self):
        """Background handlers should create background tasks."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def bg_handler(event):
            await asyncio.sleep(0.01)

        entry = _make_handler_entry(callback=bg_handler, background=True)
        registry.register_handler(entry)

        event = _make_event()

        result = await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )
        assert result.accepted is False

        # Allow a short time for background task to complete
        await asyncio.sleep(0.05)

    @pytest.mark.asyncio
    async def test_direct_dispatch_uses_captured_handlers_without_registry_lookups(self):
        """Direct dispatch needs no registry access or consumer capacity."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        core = AsyncMock()
        entry = ApixEventHandler(core)
        with (
            patch.object(registry, "get_handler") as lookup,
            patch.object(registry, "_matches_handler") as matches,
        ):
            event = _make_event()
            await handler._dispatch_event(event, [entry])
            lookup.assert_not_called()
            matches.assert_not_called()

        core.assert_awaited_once_with(event)
        assert handler._event_semaphore._value == EVENT_LOOP_BACKPRESSURE


# ============================
# Tests: _run_background_handler
# ============================


class TestRunBackgroundHandler:
    """Tests for _run_background_handler method."""

    @pytest.mark.asyncio
    async def test_background_handler_no_timeout(self):
        """Background handler with time_out=None should run normally."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        mock_callback = AsyncMock()
        entry = _make_handler_entry(callback=mock_callback, time_out=None)
        registry.register_handler(entry)
        event = _make_event()

        await handler._run_background_handler(entry, event)
        mock_callback.assert_awaited_once_with(event)

    @pytest.mark.asyncio
    async def test_background_handler_with_timeout(self):
        """Background handler with a timeout should use wait_for."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        mock_callback = AsyncMock()
        entry = _make_handler_entry(callback=mock_callback, time_out=5.0)
        registry.register_handler(entry)
        event = _make_event()

        await handler._run_background_handler(entry, event)
        mock_callback.assert_awaited_once_with(event)

    @pytest.mark.asyncio
    async def test_background_handler_timeout_error(self):
        """Background handler timeout should log error."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def slow_handler(event):
            await asyncio.sleep(10)

        entry = _make_handler_entry(callback=slow_handler, time_out=0.001)
        registry.register_handler(entry)
        event = _make_event()

        with patch("apixis.core.event.base.logger") as mock_logger:
            await handler._run_background_handler(entry, event)
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_background_handler_cancelled_error_propagates(self):
        """CancelledError should be re-raised in background handler."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def cancelled_handler(event):
            raise asyncio.CancelledError()

        entry = _make_handler_entry(callback=cancelled_handler)
        registry.register_handler(entry)
        event = _make_event()

        with pytest.raises(asyncio.CancelledError):
            await handler._run_background_handler(entry, event)

    @pytest.mark.asyncio
    async def test_background_handler_exception_logs(self):
        """Background handler exception should be logged."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        async def error_handler(event):
            raise ValueError("background error")

        entry = _make_handler_entry(callback=error_handler)
        registry.register_handler(entry)
        event = _make_event()

        with patch("apixis.core.event.base.logger") as mock_logger:
            await handler._run_background_handler(entry, event)
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_background_execution_does_not_acquire_event_capacity(self):
        """Background execution remains possible without an available event permit."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
        handler._event_semaphore = asyncio.BoundedSemaphore(0)
        callback = AsyncMock()
        entry = _make_handler_entry(callback=callback, background=True, time_out=None)
        registry.register_handler(entry)
        event = _make_event()

        await asyncio.wait_for(handler._run_background_handler(entry, event), 1)
        callback.assert_awaited_once_with(event)
        assert handler._event_semaphore._value == 0


# ============================
# Tests: _create_background_handler_task
# ============================


class TestCreateBackgroundHandlerTask:
    """Tests for _create_background_handler_task method."""

    @pytest.mark.asyncio
    async def test_creates_task_adds_to_set(self):
        """Should create a task and add it to _background_handler_tasks."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        mock_callback = AsyncMock()
        entry = _make_handler_entry(callback=mock_callback, background=True, time_out=None)
        registry.register_handler(entry)
        event = _make_event()

        initial_count = len(handler._background_handler_tasks)
        handler._create_background_handler_task(entry, event)

        assert len(handler._background_handler_tasks) == initial_count + 1

        # Wait for task to complete
        pending = list(handler._background_handler_tasks)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        mock_callback.assert_awaited_once_with(event)


# ============================
# Tests: Consumer Loop
# ============================


class TestEventConsumerLoop:
    """Integration tests for the event dispatch flow."""

    @pytest.mark.asyncio
    async def test_consumer_dispatch_flow(self):
        """End-to-end: event -> dispatch -> handler called without implicit acceptance."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())

        mock_callback = AsyncMock()
        entry = _make_handler_entry(callback=mock_callback)
        registry.register_handler(entry)

        event = _make_event()
        result = await handler._dispatch_event(
            event,
            handler._registry.get_handlers_for_event(event.event_name) if event.event_name else [],
        )

        assert result is not None
        assert not result.accepted
        mock_callback.assert_awaited_once_with(event)

    @pytest.mark.asyncio
    async def test_dispatch_acknowledges_event_even_when_dispatch_fails(self, wait_for_dispatch):
        """The real consumer acquires and releases exactly one slot on failure."""
        registry = ApixHandlerRegistry(get_event_registry())
        _reset_registry(registry)
        handler = ApixEventLoop(registry, ApixEventPipe(), get_event_registry())
        handler._event_semaphore = asyncio.BoundedSemaphore(1)
        event = _make_event()

        with (
            patch.object(handler, "_dispatch_event", AsyncMock(side_effect=RuntimeError("dispatch failed"))),
            patch.object(handler._event_pipe, "task_done", wraps=handler._event_pipe.task_done) as task_done,
            patch("apixis.core.event.event_loop.logger") as logger,
        ):
            await handler.start()
            try:
                await handler._event_pipe.put(event)
                await wait_for_dispatch(handler)
                assert handler._event_semaphore._value == 1
                assert not handler._dispatch_tasks
                task_done.assert_called_once_with()
                logger.error.assert_called_once()
            finally:
                await handler.stop()


# ============================
# Tests: Factory-managed runtime
# ============================


class TestModuleSingleton:
    """Tests for the factory-managed event loop."""

    def test_pipe_event_handler_is_PipeEventHandler_instance(self):
        """Factory-managed event loop should be a ApixEventLoop."""
        from apixis.core.event.factory import get_event_loop

        assert isinstance(get_event_loop(), ApixEventLoop)
