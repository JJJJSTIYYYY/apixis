"""Explicit lifecycle and synchronous access to the shared ApixisCore."""

from apixis.core.event.base.status import CoreState, EventRuntimeStatus
from apixis.core.event.core import ApixisCore
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry import ApixHandlerRegistry


_core: ApixisCore | None = None


def create_core() -> ApixisCore:
    """Create components without starting consumption, returning the shared core.

    Repeated calls preserve an existing live core and its registrations. After
    await close_core() completes, create a new instance; references to the old
    pipe and registry remain closed. Creation during closure raises RuntimeError.
    """
    global _core
    if _core is not None and _core.state is CoreState.CLOSING:
        raise RuntimeError("Wait for close_core() before creating another core.")
    if _core is None or _core.state is CoreState.CLOSED:
        _core = ApixisCore._create_core()
    return _core


def get_core() -> ApixisCore:
    """Return the existing core synchronously, without creating or starting it.

    Raises RuntimeError if create_core() has not been called. A closed instance
    remains inspectable until the application explicitly creates a replacement.
    The returned handle exposes state, lifecycle methods and close completion
    callback registration. Its components and their construction are internal;
    use publication, subscription and status helpers for application work.
    """
    if _core is None:
        raise RuntimeError("Call create_core() before accessing the event core.")
    return _core


async def start_core() -> None:
    """Start or resume consumption using the explicitly created shared core.

    Never construct components or replace existing registrations. Repeated
    calls share the startup task and do not duplicate the consumer. Cancelling
    a caller only cancels its wait. Returns
    after consumer startup, without waiting for event processing.

    Raises:
        RuntimeError: If create_core() has not been called.
        CoreClosedError: If terminal closure has begun; explicitly create a
            replacement after awaiting close_core() instead of restarting it.
    """
    await get_core().start_core()


async def stop_core() -> None:
    """Stop queue consumption only, leaving admission and scheduled chains intact.

    Safe before creation and on repeated calls. Resume using start_core().
    Concurrent callers share the stop operation; cancelling a caller's wait
    does not cancel that operation. An interrupted internal operation is retried
    on the next call without rolling back completed steps.
    """
    if _core is not None:
        await _core.stop_core()


async def close_core() -> None:
    """Cancel and close the shared core, including every active event waiter.

    Seal the pipe and registration, join startup/stop, then cancel foreground/background
    tasks. Running handler chains deliver on_cancelled; queued and pending
    events are discarded directly. Complete remaining event waiters before
    joining cleanup, so cleanup can itself await events.
    Notification failures do not abort the other cancellations.

    Safe before creation and on repeated calls. Concurrent callers await one
    protected cleanup task. Await completion before calling create_core() to
    construct a replacement; start_core() cannot reopen the closed instance.
    A failed or cancelled internal task is replaced on the next close call.
    Calling close on a CLOSED instance repeats cleanup with admission sealed.
    """
    if _core is not None:
        await _core.close_core()


def get_event_runtime_status() -> EventRuntimeStatus:
    """Read lifecycle and work counters without creating or starting components."""
    if _core is None:
        return EventRuntimeStatus()
    return EventRuntimeStatus(
        initialized=True,
        state=_core.state,
        running=_core.state is CoreState.RUNNING,
        event_queue_size=_core._event_pipe.qsize(),
        event_queue_capacity=_core._event_pipe.maxsize,
        wait_listeners=_core._handler_registry.waiter_count,
        event_loop=_core._event_loop.get_status(),
    )


def get_event_registry() -> ApixEventRegistry:
    """Return the existing name registry; call create_core() first."""
    return get_core()._event_registry


def get_event_pipe() -> ApixEventPipe:
    """Return the existing pipe without creating components or starting consumption."""
    return get_core()._event_pipe


def get_handler_registry() -> ApixHandlerRegistry:
    """Return the existing handler registry without changing core state."""
    return get_core()._handler_registry


def get_event_loop() -> ApixEventLoop:
    """Return the existing dispatcher without creating or starting its consumer."""
    return get_core()._event_loop


__all__ = [
    "create_core",
    "get_core",
    "start_core",
    "stop_core",
    "close_core",
    "get_event_runtime_status",
]
