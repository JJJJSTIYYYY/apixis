"""Global event publication from asyncio callers and worker threads."""

import asyncio
from concurrent.futures import Future
from functools import partial
from threading import current_thread, main_thread
from typing import Any

from apixis.core.event.base.event import EventType
from apixis.core.event.factory import get_event_pipe, get_thread_bridge


async def post_event(
    *,
    event_type: EventType,
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
        event_type: Event category used by handlers.
        event_name: Name used by the registry to select handlers.
        context: Optional event payload or runtime context.
    """
    if current_thread() is not main_thread():
        request = await get_thread_bridge().asubmit(partial(
            post_event, event_type=event_type, event_name=event_name, context=context,
        ))
        await asyncio.wrap_future(request.result)
        return
    await get_event_pipe().post_event(
        event_type=event_type,
        event_name=event_name,
        context=context,
    )


def post_event_threadsafe(
    *, event_type: EventType, event_name: str, context: Any = None,
) -> Future[None]:
    """Publish from a worker thread; completion means admission to the event pipe.

    Call ``await start_core()`` on the main thread first. When the bounded
    bridge is full, this call waits in the worker thread for capacity. Use
    ``await post_event()`` from an asyncio worker to wait asynchronously. Cancel
    the returned Future to abandon a pending publication; an event already
    admitted cannot be retracted. ``Future.result(timeout=...)`` alone does
    not cancel publication. Business context variables are captured at submission;
    internal handler-chain ownership is not transferred.
    """
    return get_thread_bridge().submit(partial(
        post_event, event_type=event_type, event_name=event_name, context=context,
    )).result


__all__ = [
    "post_event",
    "post_event_threadsafe",
]
