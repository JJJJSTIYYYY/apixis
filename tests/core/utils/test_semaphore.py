"""Contracts for optional semaphore bounds and permit accounting."""

import asyncio

import pytest

from apixis.core.utils.semaphore import ApixisSemaphore


async def test_unbounded_operations_only_count(monkeypatch):
    semaphore = ApixisSemaphore()
    # Unbounded operation must survive changes to the base counter layout.
    del semaphore._value
    del semaphore._bound_value

    def unexpected(*args, **kwargs):
        pytest.fail("Unbounded operations must not use the asyncio semaphore.")

    for method in ("acquire", "release", "locked"):
        monkeypatch.setattr(asyncio.BoundedSemaphore, method, unexpected)

    for _ in range(1025):
        assert await semaphore.acquire() is True
    assert semaphore._unbounded_value == -1025
    assert not semaphore.locked()
    assert semaphore._waiters is None
    for _ in range(1025):
        semaphore.release()
    assert semaphore._unbounded_value == 0
    async with semaphore:
        assert semaphore._unbounded_value == -1
    assert semaphore._unbounded_value == 0


@pytest.mark.parametrize("bounded", [None, 1, 2])
async def test_context_manager_releases_on_exception(bounded):
    semaphore = ApixisSemaphore(bounded)
    with pytest.raises(RuntimeError, match="failed"):
        async with semaphore:
            assert semaphore._unbounded_value == (-1 if bounded is None else 0)
            assert semaphore._value == (0 if bounded is None else bounded - 1)
            raise RuntimeError("failed")
    assert semaphore._unbounded_value == 0
    assert semaphore._value == (0 if bounded is None else bounded)


async def test_bounded_waiters_cancellation_and_overrelease():
    semaphore = ApixisSemaphore(1)
    assert await semaphore.acquire() is True
    assert semaphore.locked()
    cancelled = asyncio.create_task(semaphore.acquire())
    waiter = asyncio.create_task(semaphore.acquire())
    await asyncio.sleep(0)
    assert not waiter.done()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    semaphore.release()
    assert await asyncio.wait_for(waiter, 1) is True
    assert semaphore._value == 0
    semaphore.release()
    assert not semaphore.locked()
    with pytest.raises(ValueError):
        semaphore.release()
    assert semaphore._value == 1


@pytest.mark.parametrize("bounded", [0, -1, True, False, "1", 1.5])
def test_invalid_bound(bounded):
    with pytest.raises(ValueError if type(bounded) is int else TypeError):
        ApixisSemaphore(bounded)
