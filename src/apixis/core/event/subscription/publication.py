"""Thread-local event publication and explicit cross-thread forwarding."""

import asyncio
from functools import partial
from glob import escape
from typing import Any

from apixis.core.event.base.event import ApixEvent, EventType, create_event
from apixis.core.event.base.process import suspend_process
from apixis.core.event.factory import get_event_pipe
from apixis.core.event.thread_bridge import ThreadIds, get_thread_bridge
from apixis.core.event.subscription.waiter import expect_event
from apixis.core.utils.validation import validate_number


async def post_event(
    *,
    event_type: EventType = EventType.DEFAULT,
    event_name: str,
    context: Any = None,
) -> None:
    """Create and publish an event through the current thread event pipe.

    Publication waits for queue capacity, not handler completion. The local
    pipe captures the publisher's context variables and temporarily returns
    the current managed handler's dispatch permit during publication.
    This interface never forwards to another thread. Worker callers first
    register their local core with create_core(thread_id).

    Args:
        event_type: Event category used by handlers; defaults to ``EventType.DEFAULT``.
        event_name: Name used by the registry to select handlers.
        context: Optional event payload or runtime context.
    """
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
) -> ApixEvent:
    """Publish one event and return it after its foreground handlers finish.

    The event category defaults to ``EventType.DEFAULT``.
    Register before publishing and match the generated ``event_id`` so
    concurrent events with the same name cannot supply another call's result.
    Completion follows ``point="processed"``: background handlers and events
    published by handlers are not awaited. Handler failures remain available
    in the returned event's ``error_stack``.

    ``time_out`` bounds queue admission and processing together, starting on
    the owner loop. ``None`` waits indefinitely. Expiry raises ``TimeoutError``
    and removes the listener, including while waiting for queue admission.
    Cancellation removes the listener. Neither timeout nor cancellation retracts
    an admitted event or cancels its handlers. Managed callers lend their dispatch
    permit throughout publication and waiting. Publication and collection
    stay on the current thread, including worker cores.
    """
    if time_out is not None:
        time_out = validate_number(time_out, "time_out")
    pipe = get_event_pipe()
    event = create_event(event_type=event_type, event_name=event_name, context=context)
    event_id = event.event_id
    loop = asyncio.get_running_loop()
    deadline = None if time_out is None else loop.time() + time_out
    async with expect_event(
        escape(event_name), point="processed", time_out=time_out,
        predicate=lambda event: event.event_id == event_id,
    ) as waiter:
        async with suspend_process():
            async with asyncio.timeout_at(deadline):
                await pipe.put(event)
            return await waiter.wait()


async def post_and_wait_threadsafe(
    *,
    event_type: EventType = EventType.DEFAULT,
    event_name: str,
    context: Any = None,
    time_out: float | None = None,
    thread_ids: ThreadIds = None,
) -> ApixEvent | dict[str, ApixEvent]:
    """Await publication and foreground completion on the owner loop.

    Reuse ``post_and_wait`` on the owner loop, including exact ``event_id``
    matching and the default ``EventType.DEFAULT``. Call ``await start_core()``
    on each target first. None selects the main core; a string selects one
    core, and a collection returns results keyed by thread ID. Admission waits asynchronously.
    Processing runs independently of subsequent bridge requests.

    ``time_out`` covers event-pipe admission and processing once the request
    starts on the owner loop. Expiry removes the listener and raises
    ``TimeoutError``. Task cancellation waits for listener cleanup; an admitted
    event continues processing. No asyncio waiter is transferred across threads.
    """
    return await get_thread_bridge().call(partial(
        post_and_wait, event_type=event_type, event_name=event_name,
        context=context, time_out=time_out,
    ), thread_ids=thread_ids, background=True)


async def post_event_threadsafe(
    *, event_type: EventType = EventType.DEFAULT, event_name: str, context: Any = None,
    thread_ids: ThreadIds = None,
) -> None:
    """Await publication from any caller loop; completion means pipe admission.

    The event category defaults to ``EventType.DEFAULT``.
    None selects the main core. A string or a non-empty collection of thread
    IDs selects explicit targets; call start_core on each target first.
    Completion confirms admission to every selected event pipe. Each target
    creates its own event; payloads remain shared references. Admission waits
    asynchronously. Cancel the task to abandon a pending publication;
    an event already admitted cannot be retracted.
    Business context variables are captured at submission;
    internal handler-chain ownership is not transferred.
    """
    await get_thread_bridge().call(partial(
        post_event, event_type=event_type, event_name=event_name, context=context,
    ), thread_ids=thread_ids, background=False)


__all__ = [
    "post_event",
    "post_and_wait",
    "post_and_wait_threadsafe",
    "post_event_threadsafe",
]
