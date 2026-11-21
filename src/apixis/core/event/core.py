"""Ownership and explicit lifecycle of one complete asyncio event core."""

import asyncio
from collections.abc import Callable
from contextvars import Context
from typing import Self

from apixis.core.config.core_config import HANDLER_CHAIN_CACHE_SIZE
from apixis.core.event.base.status import CoreState
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.utils.exception import CoreClosedError


class ApixisCore:
    """Own components through create, start, running, stop and close.

    Creation only constructs components. Start and stop control consumption;
    neither replaces components nor restricts already scheduled handler chains.
    Close seals admission, cancels in-flight work, delivers cancellation hooks
    and releases registrations. A closed instance cannot restart.
    """

    _transitions = {
        CoreState.CREATED: {CoreState.STARTING, CoreState.STOPPING, CoreState.CLOSING},
        CoreState.STARTING: {
            CoreState.STARTED, CoreState.STOPPED, CoreState.STOPPING, CoreState.CLOSING,
        },
        CoreState.STARTED: {
            CoreState.RUNNING, CoreState.STOPPING, CoreState.STOPPED, CoreState.CLOSING,
        },
        CoreState.RUNNING: {CoreState.STOPPING, CoreState.STOPPED, CoreState.CLOSING},
        CoreState.STOPPING: {CoreState.STOPPED, CoreState.CLOSING},
        CoreState.STOPPED: {CoreState.STARTING, CoreState.CLOSING},
        CoreState.CLOSING: {CoreState.CLOSED},
        CoreState.CLOSED: set(),
    }

    def __init__(
        self,
        event_registry: ApixEventRegistry,
        event_pipe: ApixEventPipe,
        handler_registry: ApixHandlerRegistry,
        event_loop: ApixEventLoop,
    ) -> None:
        self.event_registry = event_registry
        self.event_pipe = event_pipe
        self.handler_registry = handler_registry
        self.event_loop = event_loop
        self._state = CoreState.CREATED
        self._start_task: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._close_task_done_callbacks: list[Callable[[asyncio.Task[None]], None]] = []

    @classmethod
    def create_core(cls) -> Self:
        """Construct one dependency graph without starting tasks or consumers."""
        event_registry = ApixEventRegistry()
        event_pipe = ApixEventPipe()
        handler_registry = ApixHandlerRegistry(
            event_registry,
            cache_size=HANDLER_CHAIN_CACHE_SIZE,
        )
        event_loop = ApixEventLoop(handler_registry, event_pipe, event_registry)
        return cls(event_registry, event_pipe, handler_registry, event_loop)

    @property
    def state(self) -> CoreState:
        """Current state; CLOSING rejects work while cancellation hooks finish."""
        return self._state

    def _transition(self, state: CoreState) -> None:
        if state not in self._transitions[self._state]:
            raise RuntimeError(
                f"Invalid core transition: {self._state.value} -> {state.value}."
            )
        self._state = state

    def add_close_task_done_callback(
        self, callback: Callable[[asyncio.Task[None]], None],
    ) -> None:
        """Register a synchronous callback receiving the completed close Task.

        Callbacks receive the completed Task in registration order, including
        failed closure. Registration before close waits for that operation;
        during CLOSING it attaches to the existing Task. If that Task already
        failed, the callback is scheduled with call_soon. Duplicate registrations
        are independent; repeated close calls do not notify again.
        Return values are ignored and exceptions go to the asyncio loop's
        exception handler without preventing other callbacks from running.

        Raises TypeError for a non-callable, or CoreClosedError after CLOSED.
        Remove registrations with remove_close_task_done_callback().
        """
        if not callable(callback):
            raise TypeError("Task completion callback must be callable.")
        if self._state is CoreState.CLOSED:
            raise CoreClosedError("The event core is closed.")
        self._close_task_done_callbacks.append(callback)
        if self._close_task is not None:
            self._close_task.add_done_callback(callback)

    def remove_close_task_done_callback(
        self, callback: Callable[[asyncio.Task[None]], None],
    ) -> int:
        """Remove all matching close registrations and return their count.

        Pending callbacks are detached from the current Task as well. Callbacks
        already scheduled by a completed Task follow native asyncio semantics
        and may still run. Removal remains available after closure.
        """
        previous_count = len(self._close_task_done_callbacks)
        self._close_task_done_callbacks[:] = [
            item for item in self._close_task_done_callbacks if item != callback
        ]
        if self._close_task is not None:
            self._close_task.remove_done_callback(callback)
        return previous_count - len(self._close_task_done_callbacks)

    def _on_consumer_stopped(self, _task: asyncio.Task[None]) -> None:
        if self._state is CoreState.RUNNING and not self.event_loop.is_running:
            self._transition(CoreState.STOPPED)

    async def start_core(self) -> None:
        """Start or resume consumption using the existing components.

        Concurrent callers share one startup task. Cancelling a caller only
        cancels its wait; startup continues. A start during STOPPING waits for
        that stop before starting again. Close permanently rejects new starts.
        STARTED records consumer creation; RUNNING confirms its first turn.
        Startup failures clean up the consumer and remain available to callers.
        """
        while self._state is CoreState.STOPPING:
            await asyncio.shield(self._stop_task)
        if self._state in (CoreState.CLOSING, CoreState.CLOSED):
            raise CoreClosedError("Create a new core after closing this instance.")
        if self._state is CoreState.RUNNING:
            if self.event_loop.is_running:
                return
            # Observe an exited consumer even before its done callback runs.
            self._transition(CoreState.STOPPED)
        if self._state not in (CoreState.STARTING, CoreState.STARTED):
            self._transition(CoreState.STARTING)
            self._start_task = asyncio.create_task(
                self._start(), name="apixis-core-start", context=Context(),
            )
        await asyncio.shield(self._start_task)

    async def _start(self) -> None:
        # Publish the task handle before eager consumers can reenter core.
        await asyncio.sleep(0)
        try:
            consumer = await self.event_loop.start()
            consumer.add_done_callback(self._on_consumer_stopped)
            if self._state is CoreState.STARTING:
                self._transition(CoreState.STARTED)
            await asyncio.sleep(0)
            if self._state is CoreState.STARTED:
                self._transition(
                    CoreState.STOPPED if consumer.done() else CoreState.RUNNING
                )
        except BaseException:
            try:
                await self.event_loop.stop()
            finally:
                if self._state in (CoreState.STARTING, CoreState.STARTED):
                    self._transition(CoreState.STOPPED)
            raise

    async def stop_core(self) -> None:
        """Pause only queue consumption; preserve admission and scheduled work.

        Already scheduled foreground chains and background handlers continue.
        Queued and dequeued-but-unassigned events are retained for start_core.
        Concurrent stops share a task, which first joins pending startup.
        Cancelling a caller does not cancel that stop. Stops after closure do
        nothing; close owns the terminal cleanup.
        """
        if self._state in (CoreState.STOPPED, CoreState.CLOSING, CoreState.CLOSED):
            return
        if self._state is not CoreState.STOPPING:
            self._transition(CoreState.STOPPING)
            self._stop_task = asyncio.create_task(
                self._stop(), name="apixis-core-stop", context=Context(),
            )
        await asyncio.shield(self._stop_task)

    async def _stop(self) -> None:
        # Publish the task before logging can eagerly request another start.
        await asyncio.sleep(0)
        if self._start_task is not None:
            await asyncio.gather(self._start_task, return_exceptions=True)
        try:
            await self.event_loop.stop()
        finally:
            if self._state is CoreState.STOPPING:
                self._transition(CoreState.STOPPED)

    async def close_core(self) -> None:
        """Permanently close admission and cancel all owned event work.

        Running handler chains deliver their own on_cancelled notifications.
        Queued events and pending dispatch are discarded without notifications.
        Remaining active waiters finish directly with None, including waits
        for events never produced. No supplemental event is created. Concurrent callers share
        one cleanup task; cancelling a caller does not interrupt core cleanup.

        Pending startup and stop operations finish before terminal cleanup.
        Consumer failures are reported by the consumer's completion callback.
        Cleanup failures propagate through the shared close task with state
        remaining CLOSING. Repeated calls after successful closure do nothing.
        """
        if self._state is CoreState.CLOSED:
            return
        if self._state is not CoreState.CLOSING:
            self._transition(CoreState.CLOSING)
            self.event_pipe.close()
            self.handler_registry.close_admission()
            self._close_task = asyncio.create_task(
                self._close(),
                name="apixis-core-close",
                context=Context(),
            )
            for callback in self._close_task_done_callbacks:
                self._close_task.add_done_callback(callback)
        elif self.event_loop.owns_task(asyncio.current_task()):
            # A cancellation hook cannot wait for the cleanup that awaits it.
            return
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        # Publish the task and attach callbacks before eager cleanup can finish.
        await asyncio.sleep(0)
        if self._start_task is not None:
            await asyncio.gather(self._start_task, return_exceptions=True)
        if self._stop_task is not None:
            await asyncio.gather(self._stop_task, return_exceptions=True)
        try:
            await self.event_loop.close()
        finally:
            await self.handler_registry.close()
            self.event_registry.clear()
        self._transition(CoreState.CLOSED)
        self._start_task = self._stop_task = self._close_task = None
        self._close_task_done_callbacks.clear()


__all__ = ["ApixisCore"]
