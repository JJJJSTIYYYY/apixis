"""Global event publication from asyncio callers and worker threads."""

import asyncio
from functools import partial
from glob import escape
from threading import current_thread, main_thread
from typing import Any

from apixis.core.event.base.event import ApixEvent, EventType, create_event
from apixis.core.event.base.process import suspend_process
from apixis.core.event.factory import get_event_pipe, get_thread_bridge
from apixis.core.event.subscription.waiter import EventWaiter, expect_event
from apixis.core.utils.validation import validate_number


async def post_event(
    *,
    event_type: EventType = EventType.DEFAULT,
    event_name: str,
    context: Any = None,
) -> None:
    """Create and publish an event through the process-global event pipe.

    Publication waits for queue capacity, not handler completion. The shared
    pipe captures the publisher's context variables and temporarily returns
    the current managed handler's dispatch permit during publication.
    Async callers on another thread wait for bridge capacity and publication
    without blocking their own loop; the core must already run on the main loop.

    Args:
        event_type: Event category used by handlers; defaults to ``EventType.DEFAULT``.
        event_name: Name used by the registry to select handlers.
        context: Optional event payload or runtime context.
    """
    if current_thread() is not main_thread():
        await post_event_threadsafe(event_type=event_type, event_name=event_name, context=context)
        return
    await get_event_pipe().post_event(
        event_type=event_type,
        event_name=event_name,
        context=context,
    )


async def post_and_wait(
    *,
    event_type: EventType = EventType.DEFAULT,
    event_name: str,
    context: Any = None,
    time_out: float | None = None,
    keep_listening: bool = False,
) -> ApixEvent | EventWaiter:
    """Publish one event and return it after its foreground handlers finish.

    The event category defaults to ``EventType.DEFAULT``.
    Register before publishing and match the generated ``event_id`` so
    concurrent events with the same name cannot supply another call's result.
    Completion follows ``point="processed"``: background handlers and events
    published by handlers are not awaited. Handler failures remain available
    in the returned event's ``error_stack``.

    ``time_out`` bounds queue admission and processing together, starting on
    the owner loop. ``None`` waits indefinitely. With ``keep_listening=True``,
    processing timeout returns the waiter while collection continues; use
    ``await waiter.get_result()`` to resume or ``waiter.close()`` to abandon it.
    Otherwise expiry raises ``TimeoutError`` and removes the listener. Queue
    admission timeout always raises because the event has not been published.
    Cancellation removes the listener. Neither timeout nor cancellation retracts
    an admitted event or cancels its handlers. Managed callers lend their dispatch
    permit throughout publication and waiting. Async worker threads bridge
    to the main loop without blocking subsequent bridge requests;
    ``keep_listening=True`` is not supported across threads.
    """
    if time_out is not None:
        time_out = validate_number(time_out, "time_out")
    if current_thread() is not main_thread():
        if keep_listening:
            raise ValueError("Cross-thread post_and_wait does not support keep_listening.")
        return await post_and_wait_threadsafe(
            event_type=event_type, event_name=event_name,
            context=context, time_out=time_out,
        )
    pipe = get_event_pipe()
    event = create_event(event_type=event_type, event_name=event_name, context=context)
    loop = asyncio.get_running_loop()
    deadline = None if time_out is None else loop.time() + time_out
    async with expect_event(
        escape(event_name), point="processed", keep_listening=keep_listening,
    ) as waiter:
        waiter.set_target_event_exactly(event.event_id)
        async with suspend_process():
            async with asyncio.timeout_at(deadline):
                await pipe.put(event)
            remaining = None if deadline is None else deadline - loop.time()
            return await waiter.wait(time_out=remaining)


async def post_and_wait_threadsafe(
    *,
    event_type: EventType = EventType.DEFAULT,
    event_name: str,
    context: Any = None,
    time_out: float | None = None,
) -> ApixEvent:
    """Await publication and foreground completion on the owner loop.

    Reuse ``post_and_wait`` on the owner loop, including exact ``event_id``
    matching and the default ``EventType.DEFAULT``. Call ``await start_core()``
    on the main thread first. Bridge admission waits asynchronously.
    Processing runs independently of subsequent bridge requests.

    ``time_out`` covers event-pipe admission and processing once the request
    starts on the owner loop. Expiry removes the listener and raises
    ``TimeoutError``. Task cancellation waits for listener cleanup; an admitted
    event continues processing. No asyncio waiter is transferred across threads.
    """
    return await get_thread_bridge().call(partial(
        post_and_wait, event_type=event_type, event_name=event_name,
        context=context, time_out=time_out,
    ), background=True)


async def post_event_threadsafe(
    *, event_type: EventType = EventType.DEFAULT, event_name: str, context: Any = None,
) -> None:
    """Await publication from any caller loop; completion means pipe admission.

    The event category defaults to ``EventType.DEFAULT``.
    Call ``await start_core()`` on the main thread first. Bridge admission
    waits asynchronously. Cancel the task to abandon a pending publication;
    an event already admitted cannot be retracted.
    Business context variables are captured at submission;
    internal handler-chain ownership is not transferred.
    """
    await get_thread_bridge().call(partial(
        post_event, event_type=event_type, event_name=event_name, context=context,
    ))


__all__ = [
    "post_event",
    "post_and_wait",
    "post_and_wait_threadsafe",
    "post_event_threadsafe",
]
