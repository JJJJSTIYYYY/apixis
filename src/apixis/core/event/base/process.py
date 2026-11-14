"""Foreground handler-chain ownership and temporary permit suspension."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from apixis.core.utils.semaphore import ApixisSemaphore


@dataclass(slots=True)
class HandlerChainToken:
    """Shared ownership markers for one foreground handler chain.

    The consumer creates this token after acquiring a permit. Suspension and
    dispatch completion update it; the token itself never operates on the
    semaphore. Child tasks share the token and serialize permit reacquisition.
    """

    semaphore: ApixisSemaphore
    held: bool = True
    closed: bool = False
    suspended: int = 0
    resume_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    owner: asyncio.Task | None = field(default=None, repr=False)
    resuming_task: asyncio.Task | None = field(default=None, repr=False)


handler_chain_token_context: ContextVar[HandlerChainToken | None] = ContextVar(
    "apixis_handler_chain_token", default=None,
)


@asynccontextmanager
async def suspend_process():
    """Suspend the current event process chain (handler chain).

    While the ``async with`` block is running, the current handler chain
    is paused so that other chains can proceed. The chain is resumed
    when the block exits.

    Nested calls share the outer suspension, including child tasks created
    inside it. Independently suspended branches share one permit: the last
    branch to resume reacquires it. Background handlers have no chain token.
    Child cancellation is deferred during reacquisition while its dispatch
    remains live. Cancelling the dispatch itself still propagates promptly.

    Yields:
        None

    Example:
        ```python
        async with suspend_process():
            await do_something()
        ```
    """
    chain = handler_chain_token_context.get()

    if chain is None or chain.closed:
        yield
        return

    owner = chain.owner
    token = handler_chain_token_context.set(None)
    try:
        chain.suspended += 1
        if chain.held:
            chain.semaphore.release()
            chain.held = False
        try:
            yield
        finally:
            chain.suspended -= 1
            if chain.suspended == 0 and not chain.closed and not owner.done():
                # A sibling may suspend while acquisition waits for capacity.
                task = asyncio.current_task()
                if task is not owner and owner.cancelling():
                    # The dispatch cannot finish cancelling a gather while a
                    # cancelled child waits for another event's permit.
                    raise asyncio.CancelledError
                cancelled = None
                while True:
                    try:
                        async with chain.resume_lock:
                            if not chain.closed and not chain.held and chain.suspended == 0:
                                chain.resuming_task = task
                                try:
                                    await chain.semaphore.acquire()
                                finally:
                                    chain.resuming_task = None
                                if chain.closed or chain.suspended:
                                    # The dispatch ended or a sibling suspended.
                                    chain.semaphore.release()
                                else:
                                    chain.held = True
                        break
                    except asyncio.CancelledError as exc:
                        if (
                            task is owner
                            or chain.closed
                            or owner.done()
                            or owner.cancelling()
                        ):
                            raise
                        # The owner may catch this child's cancellation and
                        # continue. Restore its permit before returning to it.
                        cancelled = exc
                if cancelled is not None:
                    raise cancelled
    finally:
        handler_chain_token_context.reset(token)


__all__ = [
    "HandlerChainToken",
    "handler_chain_token_context",
    "suspend_process",
]
