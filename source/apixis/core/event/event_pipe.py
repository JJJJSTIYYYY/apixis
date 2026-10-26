"""Bounded local event publication for the asyncio event core."""

import asyncio
import time
from typing import Any
from uuid import uuid4

from apixis.core.event.base import ApixEvent, EventType, suspend_process
from apixis.core.event.pipe_channel import BuiltinChannel
from apixis.core.utils.logger import logger
from apixis.core.utils.validation import validate_int


class ApixEventPipe:
    """Publish local events through one bounded builtin channel.

    Publication waits when the queue is full. post_event() temporarily returns
    the current handler's permit so follow-up events can continue to drain.
    """

    def __init__(self, *, builtin: BuiltinChannel | None = None) -> None:
        self._channel = builtin if builtin is not None else BuiltinChannel()
        validate_int(self._channel.maxsize, "builtin channel maxsize", minimum=1)
        self._started = False

    @property
    def is_running(self) -> bool:
        """Return whether the pipe has received its startup signal."""
        return self._started

    @property
    def maxsize(self) -> int:
        return self._channel.maxsize

    def get_channel(self) -> BuiltinChannel:
        """Return the pipe's local queue channel."""
        return self._channel

    async def put(self, event: Any) -> None:
        await self._channel.put(event)

    async def post_event(
        self,
        *,
        event_type: EventType,
        event_name: str,
        context: Any = None,
    ) -> None:
        """Create and publish a local event.

        Publication waits for queue capacity, not event completion.
        Within a managed handler, return its permit during publication and
        reacquire it before continuing, including after errors or cancellation.

        Args:
            event_type: Event category used by handlers.
            event_name: Name used by the registry to select handlers.
            context: Optional event payload or runtime context.
        """
        event = ApixEvent(
            event_id="event-" + uuid4().hex,
            event_type=event_type,
            event_name=event_name,
            context=context,
            timestamp=time.time(),
            accepted=False,
        )
        async with suspend_process():
            await self.put(event)

    def put_nowait(self, event: Any) -> None:
        """Push an event without waiting, raising QueueFull if at capacity."""
        self._channel.put_nowait(event)

    async def get(self) -> Any:
        return await self._channel.get()

    def get_nowait(self) -> Any:
        return self._channel.get_nowait()

    def empty(self) -> bool:
        return self._channel.empty()

    def full(self) -> bool:
        return self._channel.full()

    def qsize(self) -> int:
        return self._channel.qsize()

    def task_done(self) -> None:
        self._channel.task_done()

    async def join(self) -> None:
        await self._channel.join()

    async def clear(self) -> int:
        """Remove and acknowledge all currently queued events."""
        count = 0
        while True:
            try:
                self.get_nowait()
            except asyncio.QueueEmpty:
                break
            self.task_done()
            count += 1
        logger.info(f"Cleaned {count} in event pipe.")
        return count

    async def start(self) -> None:
        """Mark the pipe started; local queue access requires no worker."""
        self._started = True

    async def stop(self) -> None:
        """Reset startup state while preserving queued events for reuse."""
        self._started = False


__all__ = ["ApixEventPipe"]
