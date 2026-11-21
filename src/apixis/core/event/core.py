"""Ownership and explicit lifecycle of one complete asyncio event core."""

import asyncio
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
        CoreState.CREATED: {CoreState.STARTING, CoreState.STOPPED, CoreState.CLOSING},
        CoreState.STARTING: {CoreState.RUNNING, CoreState.STOPPED, CoreState.CLOSING},
        CoreState.RUNNING: {CoreState.STOPPED, CoreState.CLOSING},
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
        self._lifecycle_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self.event_loop.on_stopped = self._on_consumer_stopped

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

    def _on_consumer_stopped(self) -> None:
        if self._state is CoreState.RUNNING:
            self._transition(CoreState.STOPPED)

    async def start_core(self) -> None:
        """Start or resume consumption using the existing components.

        Concurrent lifecycle operations are serialized. Repeated starts while
        running do nothing. A failed start preserves components for retry.
        Raises CoreClosedError once close has begun.
        """
        if self._state in (CoreState.CLOSING, CoreState.CLOSED):
            raise CoreClosedError("Create a new core after closing this instance.")
        async with self._lifecycle_lock:
            if self._state in (CoreState.CLOSING, CoreState.CLOSED):
                raise CoreClosedError("The event core is closed.")
            if self._state is CoreState.RUNNING:
                return
            self._transition(CoreState.STARTING)
            try:
                await self.event_loop.start()
            except BaseException:
                await self.event_loop.stop()
                if self._state is CoreState.STARTING:
                    self._transition(CoreState.STOPPED)
                raise
            if self._state is CoreState.STARTING:
                self._transition(CoreState.RUNNING)

    async def stop_core(self) -> None:
        """Pause only queue consumption; preserve admission and scheduled work.

        Already scheduled foreground chains and background handlers continue.
        Queued and dequeued-but-unassigned events are retained for start_core.
        Stops are idempotent, including after closure.
        """
        if self._state in (CoreState.CLOSING, CoreState.CLOSED):
            return
        async with self._lifecycle_lock:
            if self._state in (CoreState.CLOSING, CoreState.CLOSED):
                return
            try:
                await self.event_loop.stop()
            finally:
                if self._state not in (CoreState.STOPPED, CoreState.CLOSING):
                    self._transition(CoreState.STOPPED)

    async def close_core(self) -> None:
        """Permanently close admission and cancel all owned event work.

        Running handler chains deliver their own on_cancelled notifications.
        Queued events and pending dispatch are discarded without notifications.
        Remaining active waiters finish directly with None, including waits
        for events never produced. No supplemental event is created. Concurrent callers share
        one cleanup task; cancelling a caller does not interrupt core cleanup.
        """
        if self._close_task is None:
            self._transition(CoreState.CLOSING)
            self.event_pipe.close()
            self.handler_registry.close_admission()
            self.event_loop.cancel_in_flight()
            self._close_task = asyncio.create_task(
                self._close(),
                name="apixis-core-close",
                context=Context(),
            )
        elif self.event_loop.owns_task(asyncio.current_task()):
            # A cancellation hook cannot wait for the cleanup that awaits it.
            return
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        async with self._lifecycle_lock:
            try:
                await self.event_loop.close()
            finally:
                try:
                    await self.handler_registry.close()
                    self.event_registry.clear()
                finally:
                    self._transition(CoreState.CLOSED)


__all__ = ["ApixisCore"]
