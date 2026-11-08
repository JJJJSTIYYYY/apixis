"""Async event collection, readiness, deadlines, and listener ownership."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from threading import current_thread, main_thread
from typing import Literal, Self, cast

from apixis.core.event.base.event import ApixEvent
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.base.process import suspend_process
from apixis.core.event.factory import get_handler_registry, get_thread_bridge
from apixis.core.utils.validation import validate_number


class EventWaiter:
    """Own subscriptions, first-match collection and the waiting deadline.

    String input returns one event; list/set input returns a dictionary keyed
    by pattern. Registration is synchronous. Listeners close automatically on
    completion, ordinary timeout or cancellation. A deferred timeout transfers
    ownership to the caller until collection finishes or ``close()`` is called.
    """

    def __init__(
        self,
        event_names: str | list[str] | set[str],
        *,
        point: Literal['received', 'processed'] = 'received',
        filter: list[str] | str | None = None,
        time_out: float | None = None,
        keep_listening: bool = False,
    ) -> None:
        if point not in ('received', 'processed'):
            raise ValueError("point must be 'received' or 'processed'.")
        if time_out is not None:
            time_out = validate_number(time_out, "time_out")
        if not isinstance(event_names, (str, list, set)):
            raise TypeError("event_names must be a string, list or set of patterns.")
        patterns = [event_names] if isinstance(event_names, str) else event_names
        if not patterns or any(not isinstance(name, str) or not name for name in patterns):
            raise ValueError("event_name must contain at least one non-empty string pattern.")
        if filter is not None:
            filters = [filter] if isinstance(filter, str) else filter
            if not isinstance(filters, list):
                raise TypeError("filter must be a string or list of patterns.")
            if any(not isinstance(name, str) or not name for name in filters):
                raise ValueError("filter must contain non-empty string patterns.")
        self._single_event = isinstance(event_names, str)
        event_names = set(patterns)
        self._events: dict[str, ApixEvent | None] = dict.fromkeys(event_names)
        self._remaining = len(self._events)
        loop = asyncio.get_running_loop()
        self._future: asyncio.Future[dict[str, ApixEvent]] = loop.create_future()
        self._timeout_future: asyncio.Future[None] | None = None
        self._deadline: float | None = None
        self._timeout_handle: asyncio.TimerHandle | None = None
        self._keep_listening = keep_listening
        self._deferred = False
        self._closed = False
        self._context_exited = False
        self._registry = get_handler_registry()
        self._handlers: list[ApixEventHandler] = []
        try:
            self._register_wait_handler(point, filter)
            self._start_timeout(time_out)
        except BaseException:
            self.close()
            raise

    async def __aenter__(self) -> Self:
        if self._context_exited:
            raise RuntimeError("The event waiter context has already exited.")
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        if exc_type is not None or not self._deferred:
            self._context_exited = True
            self.close()

    async def wait(
        self,
        time_out: float | None = None,
        *,
        keep_listening: bool | None = None,
    ) -> ApixEvent | dict[str, ApixEvent] | Self:
        """Wait for all matches, optionally returning this waiter on timeout.

        Every call replaces the active deadline: a numeric ``time_out`` starts
        a new budget from this call; omit it or pass ``None`` to wait indefinitely.
        The listening policy defaults to the current configuration.
        ``keep_listening=True`` returns this waiter on expiry while collection
        continues. Otherwise
        expiry closes subscriptions and raises ``TimeoutError``. A closed
        waiter cannot be restarted. Concurrent waits share the same budget and
        policy. Cancellation always closes the waiter. Waiting lends the
        handler's dispatch permit.
        """
        if time_out is not None:
            time_out = validate_number(time_out, "time_out")
        if self._context_exited:
            raise RuntimeError("The event waiter context has already exited.")
        if self._future.cancelled():
            raise RuntimeError("The event waiter is closed.")
        if keep_listening is not None:
            self._keep_listening = keep_listening
        if not self._future.done():
            self._start_timeout(time_out)
            if self._timeout_future is None:
                self._timeout_future = asyncio.get_running_loop().create_future()
        try:
            async with suspend_process():
                while not self._future.done():
                    timeout = self._timeout_future
                    await asyncio.wait(
                        (self._future, timeout), return_when=asyncio.FIRST_COMPLETED,
                    )
                    if timeout is not self._timeout_future:
                        continue
                    if not self._future.done() and not timeout.cancelled() and timeout.done():
                        if self._keep_listening:
                            self._deferred = True
                            return self
                        self._expire()
                if self._future.cancelled():
                    raise asyncio.CancelledError
                return await self.get_result()
        except asyncio.CancelledError:
            self.close()
            raise

    async def get_result(self) -> ApixEvent | dict[str, ApixEvent]:
        """Wait for the complete result without reapplying the waiting budget.

        Use after a deferred timeout. Results remain readable after automatic
        closure. Cancellation closes the waiter; a pending explicitly closed
        waiter raises ``RuntimeError``. Partial results are never returned.
        """
        if self._future.cancelled():
            raise RuntimeError("The event waiter is closed.")
        try:
            async with suspend_process():
                # Cancelling any result wait closes the whole collection, so
                # shielding its Future only adds a Future and two callbacks.
                events = await self._future
        except asyncio.CancelledError:
            self.close()
            raise
        return next(iter(events.values())) if self._single_event else events

    def _start_timeout(self, time_out: float | None) -> None:
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
        loop = asyncio.get_running_loop()
        if self._timeout_future is not None:
            self._timeout_future.cancel()
            self._timeout_future = loop.create_future()
        self._deadline = None if time_out is None else loop.time() + time_out
        self._timeout_handle = (
            None if self._deadline is None else loop.call_at(self._deadline, self._expire)
        )

    def _expire(self) -> None:
        if self._future.done():
            return
        self._deadline = None
        if self._keep_listening:
            if self._timeout_future is not None and not self._timeout_future.done():
                self._timeout_future.set_result(None)
        else:
            self._future.set_exception(TimeoutError("Timed out waiting for an event."))
            self.close()

    async def _resolve(self, event_name: str, event: ApixEvent, *args, **kwargs) -> None:
        if self._future.done() or self._events[event_name] is not None:
            return
        if (
            self._deadline is not None
            and asyncio.get_running_loop().time() >= self._deadline
        ):
            self._expire()
            if self._closed:
                return
        self._events[event_name] = event
        self._remaining -= 1
        if self._remaining == 0:
            self._future.set_result(cast(dict[str, ApixEvent], self._events))
            # Captured handlers cover overlapping patterns even as close()
            # removes listeners from the separate waiting cache.
            self.close()

    def _register_wait_handler(
        self,
        point: Literal['received', 'processed'],
        filter: list[str] | str | None,
    ) -> None:
        """Install every handler without yielding; constructor failure closes all."""
        if isinstance(filter, str):
            filter = [filter]
        filters = list(dict.fromkeys(filter)) if filter else []
        for event_name in self._events:
            resolve = partial(self._resolve, event_name)
            handler = ApixEventHandler(
                core_func=resolve,
                on_accepted=resolve,
                on_cancelled=resolve,
                on_has_error=resolve,
                on_error=resolve,
                stop_when_error=False,
            )
            handler.name = 'resolve_future-' + handler.id
            handler.subscribe = [event_name]
            handler.filter_event = filters
            self._registry.register_wait_handler(handler, point=point)
            self._handlers.append(handler)

    def close(self) -> None:
        """Idempotently unregister listeners and cancel any incomplete collection."""
        if self._closed:
            return
        self._closed = True
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
        if self._timeout_future is not None:
            self._timeout_future.cancel()
        for handler in self._handlers:
            self._registry.unregister_handler(handler.name, missing_ok=True)
        self._handlers.clear()
        if not self._future.done():
            self._future.cancel()
        elif not self._future.cancelled():
            # The context may exit without ever retrieving a timeout exception.
            self._future.exception()


@asynccontextmanager
async def wait_for_event(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    time_out: float | None = None,
    keep_listening: bool = False,
) -> AsyncIterator[EventWaiter]:
    """Register an event waiter before running the context body.

    Unlike :func:`await_for`, this interface separates subscription readiness
    from waiting for the result. Entering the context registers synchronously,
    without yielding to other tasks before registration, then yields a waiter.
    Publish the request inside the context and call
    ``await waiter.wait()`` to get its reply. No task or ``sleep(0)`` is needed;
    a reply received before ``wait()`` is retained.

    Merely constructing this context manager does not register anything. Neither
    interface replays events whose handler chains were captured before
    registration. Each pattern retains its first matching event independently.

    Args:
        event_name: Case-sensitive glob pattern or non-empty list/set of patterns.
            A string returns one event; a collection returns a dictionary keyed
            by pattern after all patterns match, even for a one-item collection.
            Duplicate patterns are merged. One event may satisfy overlapping
            patterns. Later matches never replace an already collected event.
        point: ``'received'`` resolves before ordinary foreground handlers;
            ``'processed'`` resolves after them. Background handlers are not
            awaited. Both return the original mutable ``ApixEvent``.
        filter: Glob pattern or list of patterns to exclude.
        time_out: Seconds from registration until every requested pattern has
            matched at the selected point. ``None`` means no deadline;
            non-positive values expire immediately. The deadline includes time
            spent publishing or doing other work before ``wait()``. It expires
            the result, not the context body. A live ``wait(time_out=...)``
            replaces this deadline; ``wait()`` or ``wait(time_out=None)`` clears
            it. A timed-out closed waiter cannot be restarted. Timely results
            remain available afterwards.
        keep_listening: If true, expiry makes ``wait()`` return its waiter
            instead of raising. Listeners remain until collection completes
            or the caller invokes ``close()``. Defaults to false.

    The waiter owns registration and cleanup. All handlers are registered
    without yielding, and close on completion, ordinary timeout or cancellation.
    A normal context exit preserves listeners only after ``wait()`` has actually
    returned the waiter on timeout. Otherwise exit closes it, including when
    the body raises. Use ``await waiter.get_result()`` for a deferred result.
    Waiting lends the current dispatch permit; context entry does not suspend it.

    Example::

        async with wait_for_event(reply, point="processed") as waiter:
            await pipe.post_event(
                event_type=EventType.WORKFLOW,
                event_name="tool.request",
                context={"reply": reply},
            )
            event = await waiter.wait(time_out=40)
            result = event.context["result"]
    """
    async with EventWaiter(
        event_name, point=point, filter=filter,
        time_out=time_out, keep_listening=keep_listening,
    ) as waiter:
        yield waiter


async def await_for(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    time_out: float | None = None,
    keep_listening: bool = False,
) -> ApixEvent | dict[str, ApixEvent] | EventWaiter:
    """Register all patterns and immediately wait for their first matches.

    Subscription starts when this coroutine actually executes. It registers
    without yielding before awaiting the result. Calling ``await_for(...)``
    only creates a coroutine; ``asyncio.create_task(await_for(...))`` schedules
    it but does not guarantee registration when ``create_task`` returns. Events
    dequeued before registration are not replayed. This is normal subscription
    ordering, not an event-loop scheduling guarantee.

    Use ``await await_for(...)`` to subscribe and immediately wait for an event
    produced elsewhere. Use :func:`wait_for_event` for request/reply flows that
    must register first, publish a request, then wait: its context entry is the
    explicit readiness barrier and retains replies arriving before ``wait()``.

    Args:
        event_name: Case-sensitive glob pattern or non-empty list/set of patterns.
            A string returns one event; a collection returns a dictionary keyed
            by pattern after all patterns match, even for a one-item collection.
            Duplicate patterns are merged. One event may satisfy overlapping
            patterns. Later matches never replace an already collected event.
        point: ``'received'`` resolves before ordinary foreground handlers;
            ``'processed'`` resolves after them, without awaiting background
            handlers. The returned ``ApixEvent`` is mutable, not a snapshot.
        filter: Glob pattern or list of patterns to exclude.
        time_out: Maximum seconds spent waiting after registration, or ``None``
            for no limit. Time before this coroutine starts is not included.
            Expiry raises ``TimeoutError`` unless ``keep_listening`` is true.
        keep_listening: Return an ``EventWaiter`` on timeout and keep collecting.
            Retrieve the eventual result with ``await waiter.get_result()``;
            call ``waiter.close()`` if abandoning it. Defaults to false.

    Waiting lends the current handler's dispatch permit via ``suspend_process``.
    The waiter owns all handlers and closes them on completion, ordinary timeout
    or cancellation. Only a deferred timeout preserves its listeners.

    Async callers on another thread are bridged to the main loop; admission
    waits asynchronously and ``keep_listening=True`` is not supported there.

    Example::

        event = await await_for("job.finished", point="processed", time_out=40)
        result = event.context["result"]
    """
    if current_thread() is not main_thread():
        if keep_listening:
            raise ValueError("Cross-thread await_for does not support keep_listening.")
        request = await get_thread_bridge().asubmit(partial(
            collect_events, event_name, point=point, filter=filter, time_out=time_out,
        ), background=True)
        return await asyncio.wrap_future(request.result)
    async with wait_for_event(
        event_name, point=point, filter=filter,
        time_out=time_out, keep_listening=keep_listening,
    ) as waiter:
        return await waiter.wait(time_out=time_out)


async def collect_events(
    event_name: str | list[str] | set[str], *,
    point: Literal['received', 'processed'], filter: list[str] | str | None,
    time_out: float | None,
) -> ApixEvent | dict[str, ApixEvent]:
    """Collect on the owner loop using the deadline set at registration."""
    async with EventWaiter(event_name, point=point, filter=filter, time_out=time_out) as waiter:
        return await waiter.get_result()


__all__ = [
    "EventWaiter",
    "wait_for_event",
    "await_for",
]
