"""Concurrent operations with deterministic sibling cleanup on failure."""

import asyncio
from collections.abc import Awaitable, Iterable


async def gather_results[T](operations: Iterable[Awaitable[T]]) -> list[T]:
    """Gather results in input order and cancel unfinished siblings on error."""
    tasks = [asyncio.ensure_future(operation) for operation in operations]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
