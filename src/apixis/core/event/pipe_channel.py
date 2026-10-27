"""In-process event channel backed by asyncio.Queue."""

import asyncio
from typing import Any

from apixis.core.config.core_config import EVENT_PIPE_MAX_LEN
from apixis.core.utils.validation import validate_int


class BuiltinChannel:
    """In-process event channel backed by :class:`asyncio.Queue`."""

    def __init__(self, maxsize: int = EVENT_PIPE_MAX_LEN) -> None:
        maxsize = validate_int(maxsize, "maxsize", minimum=0)
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    async def put(self, event: Any) -> None:
        await self._queue.put(event)

    def put_nowait(self, event: Any) -> None:
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


__all__ = ["BuiltinChannel"]
