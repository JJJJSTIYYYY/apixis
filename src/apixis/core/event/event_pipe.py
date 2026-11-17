"""Bounded local event publication for the asyncio event core."""

import asyncio
from typing import Any

from apixis.core.config.core_config import EVENT_PIPE_MAX_LEN
from apixis.core.event.base.event import EventType, create_event
from apixis.core.event.base.process import suspend_process
from apixis.core.utils.logger import Logger
from apixis.core.utils.validation import validate_int


class ApixEventPipe:
    """Publish local events through one bounded asyncio queue.

    Publication waits when the queue is full. post_event() temporarily returns
    the current handler's permit so follow-up events can continue to drain.
    """

    def __init__(self, *, maxsize: int = EVENT_PIPE_MAX_LEN) -> None:
        maxsize = validate_int(maxsize, "maxsize", minimum=1)
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
        self._started = False

    @property
    def is_running(self) -> bool:
        """Return whether the pipe has received its startup signal."""
        return self._started

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    async def put(self, event: Any) -> None:
        await self._queue.put(event)

    async def post_event(
        self,
        *,
        event_type: EventType = EventType.DEFAULT,
        event_name: str,
        context: Any = None,
    ) -> None:
        """Create and publish a local event.

        Publication waits for queue capacity, not event completion.
        Handlers inherit a snapshot of the publisher's context variables.
        Within a managed handler, return its permit during publication and
        reacquire it before continuing, including after errors or cancellation.

        Args:
            event_type: Event category used by handlers; defaults to ``EventType.DEFAULT``.
            event_name: Name used by the registry to select handlers.
            context: Optional event payload or runtime context.
        """
        event = create_event(
            event_type=event_type,
            event_name=event_name,
            context=context,
        )
        async with suspend_process():
            await self.put(event)

    def put_nowait(self, event: Any) -> None:
        """Push an event without waiting, raising QueueFull if at capacity."""
        self._queue.put_nowait(event)

    async def get(self) -> Any:
        return await self._queue.get()

    def get_nowait(self) -> Any:
        return self._queue.get_nowait()

    def empty(self) -> bool:
        return self._queue.empty()

    def full(self) -> bool:
        return self._queue.full()

    def qsize(self) -> int:
        return self._queue.qsize()

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

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
        Logger.info(f"Cleaned {count} in event pipe.")
        return count

    async def start(self) -> None:
        """Mark the pipe started; local queue access requires no worker."""
        self._started = True

    async def stop(self) -> None:
        """Reset startup state while preserving queued events for reuse."""
        self._started = False


__all__ = ["ApixEventPipe"]
