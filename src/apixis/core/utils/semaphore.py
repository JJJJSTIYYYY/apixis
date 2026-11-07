"""Optional concurrency bounds with shared permit accounting."""

import asyncio

from apixis.core.utils.validation import validate_int


class ApixisSemaphore(asyncio.BoundedSemaphore):
    """Use bounded permits, or count acquisitions without blocking for None.

    In unbounded mode, ``_value`` starts at zero: each acquisition subtracts
    one and each release adds one. Balanced operations return it to zero.
    Async context management follows the standard asyncio semaphore contract.
    """

    def __init__(self, bounded: None | int = None) -> None:
        if bounded is not None:
            validate_int(bounded, "bounded", minimum=1)
        super().__init__(0 if bounded is None else bounded)
        self._bound_value = bounded

    async def acquire(self) -> bool:
        if self._bound_value is None:
            self._value -= 1
            return True
        return await super().acquire()

    def release(self) -> None:
        if self._bound_value is None:
            self._value += 1
        else:
            super().release()

    def locked(self) -> bool:
        return self._bound_value is not None and super().locked()
