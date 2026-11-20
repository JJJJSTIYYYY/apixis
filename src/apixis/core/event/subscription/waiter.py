"""Local event collection, fixed deadlines, and single-Future cleanup."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from inspect import iscoroutinefunction
from typing import Literal, Self, cast

from apixis.core.event.base.event import ApixEvent, EventPredicate
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.base.process import suspend_process
from apixis.core.event.factory import get_handler_registry
from apixis.core.utils.validation import validate_number


type _EventMatches = ApixEvent | dict[str, ApixEvent]


class EventWaiter:
    """Own subscriptions, first-match collection and the waiting deadline.

    String input returns one event; list/set input returns a dictionary keyed
    by pattern. Registration is synchronous. Listeners close automatically on
    completion, timeout or cancellation of its single Future. The deadline is
    fixed at registration. An optional synchronous ``predicate`` filters events
    by their contents. Completed results remain readable after cleanup.
    """

    def __init__(
        self,
        event_names: str | list[str] | set[str],
        *,
        point: Literal['received', 'processed'] = 'received',
        filter: list[str] | str | None = None,
        predicate: EventPredicate | None = None,
        time_out: float | None = None,
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
        if predicate is not None:
            if not callable(predicate):
                raise TypeError("predicate must be callable or None.")
            if iscoroutinefunction(predicate) or iscoroutinefunction(predicate.__call__):
                raise TypeError("predicate must be synchronous.")
        self._single_event = isinstance(event_names, str)
        self._predicate = predicate
        event_names = set(patterns)
        self._events: dict[str, ApixEvent | None] = dict.fromkeys(event_names)
        self._remaining = len(self._events)
        loop = asyncio.get_running_loop()
        self._future: asyncio.Future[dict[str, ApixEvent]] = loop.create_future()
        self._deadline: float | None = None
        self._timeout_handle: asyncio.TimerHandle | None = None
        self._entered = False
        self._registry = get_handler_registry()
        self._handlers: list[ApixEventHandler] = []
        self._future.add_done_callback(self._on_done)
        try:
            self._register_wait_handler(point, filter)
            if time_out is not None:
                self._deadline = loop.time() + time_out
                self._timeout_handle = loop.call_at(self._deadline, self._expire)
        except BaseException:
            self.close()
            raise

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("The event waiter context has already been entered.")
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self.close()

    async def wait(self) -> _EventMatches:
        """Await the single collection with its registration deadline.

        Return every requested pattern's first match. Timeout raises
        ``TimeoutError``. Cancelling a pending wait cancels the shared Future
        and removes its listeners, including for concurrent readers. Explicit
        ``close()`` also cancels an incomplete Future. Subsequent reads preserve
        its result, exception or cancellation. Waiting lends the current
        handler's dispatch permit. Listener cleanup precedes permit recovery.
        """
        if self._future.done():
            events = self._future.result()
        else:
            async with suspend_process():
                try:
                    events = await self._future
                finally:
                    self.close()
        return next(iter(events.values())) if self._single_event else events

    get_result = wait

    def _expire(self) -> None:
        if not self._future.done():
            self._future.set_exception(TimeoutError("Timed out waiting for an event."))
            self._on_done(self._future)

    async def _resolve(self, event_name: str, event: ApixEvent, *args, **kwargs) -> None:
        if self._future.done() or self._events[event_name] is not None:
            return
        if (
            self._deadline is not None
            and asyncio.get_running_loop().time() >= self._deadline
        ):
            self._expire()
            return
        if self._predicate is not None:
            try:
                if not self._predicate(event):
                    return
            except Exception as error:
                self._future.set_exception(error)
                self.close()
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
        """Cancel an incomplete Future and release listeners and its timer.

        This operation is synchronous on the owner loop and idempotent.
        A completed result or exception stays available for subsequent reads.
        """
        self._future.cancel()
        self._on_done(self._future)

    def _on_done(self, future: asyncio.Future[dict[str, ApixEvent]]) -> None:
        """Release collection resources when the Future reaches a terminal state."""
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None
        for handler in self._handlers:
            self._registry.unregister_handler(handler.name, missing_ok=True)
        self._handlers.clear()
        if not future.cancelled():
            # A timeout may finish before the caller starts reading the result.
            future.exception()


@asynccontextmanager
async def expect_event(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    predicate: EventPredicate | None = None,
    time_out: float | None = None,
) -> AsyncIterator[EventWaiter]:
    """Register an event waiter before running the context body.

    Unlike :func:`wait_event`, this interface separates subscription readiness
    from waiting for the result. Context entry registers synchronously on the
    shared runtime without yielding.
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
        predicate: Optional synchronous ``EventPredicate`` evaluated on the owner
            loop after name filtering, at the selected point. False ignores the
            event; an exception fails this collection and removes its listeners.
            Overlapping patterns may evaluate it more than once per event.
        time_out: Seconds from registration until every requested pattern has
            matched at the selected point. ``None`` means no deadline;
            non-positive values expire immediately. The deadline includes time
            spent publishing or doing other work before ``wait()``. It expires
            the result while the context body continues. ``wait()`` and
            ``get_result()`` use this fixed deadline. Timely results remain
            available afterwards. Expiry raises ``TimeoutError``.

    The waiter owns registration and cleanup. All handlers are registered
    without yielding. Completion, timeout, cancellation and context exit
    release every listener and timer. Each context is single-use. Repeated
    reads observe the same Future's terminal state. Waiting lends the current
    handler's dispatch permit. Local context entry keeps that permit.

    Example::

        async with expect_event(reply, point="processed", time_out=40) as waiter:
            await pipe.post_event(
                event_type=EventType.WORKFLOW,
                event_name="tool.request",
                context={"reply": reply},
            )
            event = await waiter.wait()
            result = event.context["result"]
    """
    async with EventWaiter(
        event_name, point=point, filter=filter, predicate=predicate,
        time_out=time_out,
    ) as waiter:
        yield waiter


async def wait_event(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    predicate: EventPredicate | None = None,
    time_out: float | None = None,
) -> ApixEvent | dict[str, ApixEvent]:
    """Register all patterns and immediately wait for their first matches.

    Subscription starts when this coroutine executes. It registers on the
    shared runtime without yielding before awaiting the result.
    Calling ``wait_event(...)`` only creates a coroutine; ``asyncio.create_task(wait_event(...))`` schedules
    it but does not guarantee registration when ``create_task`` returns. Events
    dequeued before registration are not replayed. This is normal subscription
    ordering, not an event-loop scheduling guarantee.

    Use ``await wait_event(...)`` to subscribe and immediately wait for an event
    produced elsewhere. Use :func:`expect_event` for request/reply flows that
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
        predicate: Optional synchronous ``EventPredicate`` evaluated on the owner
            loop after name filtering, at the selected point. False ignores the
            event; an exception fails this collection and removes its listeners.
            Overlapping patterns may evaluate it more than once per event.
        time_out: Maximum seconds spent waiting after registration, or ``None``
            for no limit. Time before this coroutine starts is not included.
            Expiry raises ``TimeoutError`` and removes the listeners.

    Waiting lends the current handler's dispatch permit via ``suspend_process``.
    The waiter owns all handlers and closes them on completion, ordinary timeout
    or cancellation.

    Example::

        event = await wait_event("job.finished", point="processed", time_out=40)
        result = event.context["result"]
    """
    async with expect_event(
        event_name, point=point, filter=filter, predicate=predicate,
        time_out=time_out,
    ) as waiter:
        return await waiter.wait()


# Preserve the original public names as aliases.
wait_for_event = expect_event
await_for = wait_event


__all__ = [
    "EventWaiter",
    "expect_event",
    "wait_event",
    "wait_for_event",
    "await_for",
]
