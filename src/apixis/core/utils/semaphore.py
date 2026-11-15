"""Optional concurrency bounds with shared permit accounting."""

import asyncio

from apixis.core.utils.validation import validate_int


class ApixisSemaphore(asyncio.BoundedSemaphore):
    """Use bounded permits, or count acquisitions without blocking for None.

    In unbounded mode, ``_unbounded_value`` starts at zero: each acquisition
    subtracts one and each release adds one without accessing base counters.
    Balanced operations return it to zero.
    Async context management follows the standard asyncio semaphore contract.
    """

    def __init__(self, bounded: None | int = None) -> None:
        if bounded is not None:
            validate_int(bounded, "bounded", minimum=1)
        super().__init__(0 if bounded is None else bounded)
        self._bounded = bounded
        self._unbounded_value = 0

    @property
    def capacity(self) -> int | None:
        """Return the configured bound, or None for unlimited dispatch."""
        return self._bounded

    @property
    def in_use(self) -> int:
        """Return acquired permits, including reservations for waking waiters."""
        return -self._unbounded_value if self._bounded is None else self._bounded - self._value

    async def acquire(self) -> bool:
        if self._bounded is None:
            self._unbounded_value -= 1
            return True
        return await super().acquire()

    def release(self) -> None:
        if self._bounded is None:
            self._unbounded_value += 1
        else:
            super().release()

    def locked(self) -> bool:
        return self._bounded is not None and super().locked()
