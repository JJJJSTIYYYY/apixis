"""Event collection, readiness, deadlines, and cleanup across caller threads."""

import asyncio
from collections.abc import AsyncIterator
from concurrent.futures import Future
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import Context
from functools import partial
from inspect import iscoroutinefunction
from typing import Literal, Self, cast

from apixis.core.event.base.event import ApixEvent, EventPredicate
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.base.process import suspend_process
from apixis.core.event.factory import get_handler_registry
from apixis.core.event.thread_bridge import (
    ThreadEndpoint, ThreadIds, ThreadSubmission, complete_future, get_thread_bridge,
)
from apixis.core.utils.tasks import gather_results
from apixis.core.utils.validation import validate_number


type _EventMatches = ApixEvent | dict[str, ApixEvent]
type _ThreadEventMatches = _EventMatches | dict[str, _EventMatches]


class EventWaiter:
    """Own subscriptions, first-match collection and the waiting deadline.

    String input returns one event; list/set input returns a dictionary keyed
    by pattern. Registration is synchronous. Listeners close automatically on
    completion, ordinary timeout or cancellation. A deferred timeout transfers
    ownership to the caller until collection finishes or ``close()`` is called.
    An optional synchronous ``predicate`` filters events by their contents.
    """

    def __init__(
        self,
        event_names: str | list[str] | set[str],
        *,
        point: Literal['received', 'processed'] = 'received',
        filter: list[str] | str | None = None,
        predicate: EventPredicate | None = None,
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

        Completed results remain available after context exit. Reading them
        does not restart listeners or apply a new deadline.
        """
        if time_out is not None:
            time_out = validate_number(time_out, "time_out")
        if self._future.done():
            return await self.get_result()
        if keep_listening is not None:
            self._keep_listening = keep_listening
        self.reset_timeout(time_out)
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
        closure or context exit. Cancellation closes the waiter; a pending
        explicitly closed waiter raises ``RuntimeError``. Partial results are
        never returned.
        """
        if self._future.cancelled():
            raise RuntimeError("The event waiter is closed.")
        if self._future.done():
            events = self._future.result()
        else:
            try:
                async with suspend_process():
                    # Cancelling any result wait closes the whole collection, so
                    # shielding its Future only adds a Future and two callbacks.
                    events = await self._future
            except asyncio.CancelledError:
                self.close()
                raise
        return next(iter(events.values())) if self._single_event else events

    def reset_timeout(self, time_out: float | None = None) -> None:
        """Apply a validated waiting budget to a live collection."""
        if not self._future.done():
            self._start_timeout(time_out)

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


class _TargetEventWaiter:
    """Async waiting context that confirms owner-loop registration on entry.

    Use ``async with`` and ``await waiter.wait()``. Each wait replaces the
    registration deadline, as for ``EventWaiter.wait``. ``close()`` requests
    cancellation without blocking; context exit waits for owner-loop cleanup.
    A context is single-use. Asyncio state is accessed only on the owner loop.
    The enclosing ThreadEventWaiter owns caller-permit suspension.
    """

    def __init__(
        self, endpoint: ThreadEndpoint, event_name: str | list[str] | set[str], *,
        point: Literal['received', 'processed'] = 'received',
        filter: list[str] | str | None = None,
        predicate: EventPredicate | None = None,
        time_out: float | None = None,
    ) -> None:
        self._make_waiter = partial(
            EventWaiter, event_name, point=point, filter=filter, predicate=predicate, time_out=time_out,
        )
        self._submission: ThreadSubmission[ApixEvent | dict[str, ApixEvent]] | None = None
        self._bridge = endpoint
        self._waiter: EventWaiter | None = None
        self._entered = False

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("The thread event waiter context has already been entered.")
        self._entered = True
        ready: Future[None] = Future()

        async def collect() -> ApixEvent | dict[str, ApixEvent]:
            waiter = self._make_waiter()
            self._waiter = waiter
            try:
                complete_future(ready)
                return await waiter.get_result()
            finally:
                waiter.close()

        def on_done(future: Future) -> None:
            if not ready.done():
                error = (
                    RuntimeError("Event collection was cancelled before registration.")
                    if future.cancelled() else future.exception()
                )
                complete_future(ready, error=error)

        try:
            self._submission = await self._bridge.asubmit(collect, background=True)
            self._submission.result.add_done_callback(on_done)
            await asyncio.wrap_future(ready)
        except BaseException:
            self.close()
            await self._wait_closed()
            raise
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self.close()
        await self._wait_closed()

    async def wait(self, time_out: float | None = None) -> ApixEvent | dict[str, ApixEvent]:
        """Await matches; a numeric budget starts now and None waits indefinitely."""
        if self._waiter is None:
            raise RuntimeError("Enter the thread event waiter context before waiting.")
        if time_out is not None:
            time_out = validate_number(time_out, "time_out")
        result = self._submission.result
        if result.done() and not result.cancelled():
            return await self.get_result()
        if self._bridge.closed or not self._bridge.loop.is_running():
            raise RuntimeError("The event thread bridge is not running; await start_core() first.")
        if self._submission.result.cancelled():
            raise RuntimeError("The event waiter is closed.")
        if asyncio.get_running_loop() is self._bridge.loop:
            self._waiter.reset_timeout(time_out)
        else:
            self._bridge.loop.call_soon_threadsafe(
                self._waiter.reset_timeout, time_out, context=Context(),
            )
        return await self.get_result()

    async def get_result(self) -> ApixEvent | dict[str, ApixEvent]:
        """Await the result using the currently configured owner-loop deadline."""
        if self._submission is None:
            raise RuntimeError("Enter the thread event waiter context before waiting.")
        result = self._submission.result
        if result.done() and not result.cancelled():
            return result.result()
        try:
            return await asyncio.wrap_future(self._submission.result)
        except asyncio.CancelledError:
            self.close()
            await self._wait_closed()
            raise

    def close(self) -> None:
        """Request cancellation; async context exit waits for listener cleanup."""
        if self._submission is not None:
            self._submission.result.cancel()

    async def _wait_closed(self) -> None:
        if self._submission is not None and not self._submission.closed.done():
            await asyncio.shield(asyncio.wrap_future(self._submission.closed))


class ThreadEventWaiter:
    """Collect events from explicitly selected cores with confirmed registration.

    None selects the main core. A string returns that core's event or pattern
    dictionary; a non-empty collection returns a dictionary keyed by thread ID.
    Enter with async with, publish only after entry, and await wait/get_result.
    Every target owns its listeners and deadline. Context exit or cancellation
    awaits cleanup on all targets. Payloads are shared references, not copies.
    """

    def __init__(
        self, event_name: str | list[str] | set[str], *,
        point: Literal['received', 'processed'] = 'received',
        filter: list[str] | str | None = None,
        predicate: EventPredicate | None = None,
        time_out: float | None = None,
        thread_ids: ThreadIds = None,
    ) -> None:
        self._make_target = partial(
            _TargetEventWaiter, event_name=event_name, point=point,
            filter=filter, predicate=predicate, time_out=time_out,
        )
        self._thread_ids = thread_ids
        self._single = thread_ids is None or isinstance(thread_ids, str)
        self._targets: dict[str, _TargetEventWaiter] = {}
        self._stack = None if self._single else AsyncExitStack()
        self._entered = False

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("The thread event waiter context has already been entered.")
        self._entered = True
        targets = get_thread_bridge().targets(self._thread_ids)
        try:
            async with suspend_process():
                for thread_id, endpoint in targets.items():
                    target = self._make_target(endpoint=endpoint)
                    self._targets[thread_id] = target
                    if self._single:
                        await target.__aenter__()
                    else:
                        await self._stack.enter_async_context(target)
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self.close()
        if self._single:
            for target in self._targets.values():
                await target._wait_closed()
        else:
            await self._stack.aclose()

    async def _collect(self, time_out: float | None, *, reset_deadline: bool) -> _ThreadEventMatches:
        if not self._targets:
            raise RuntimeError("Enter the thread event waiter context before waiting.")
        try:
            async with suspend_process():
                if self._single:
                    target = next(iter(self._targets.values()))
                    return await target.wait(time_out) if reset_deadline else await target.get_result()
                results = await gather_results(
                    target.wait(time_out) if reset_deadline else target.get_result()
                    for target in self._targets.values()
                )
                return dict(zip(self._targets, results))
        except BaseException:
            self.close()
            await gather_results(target._wait_closed() for target in self._targets.values())
            raise

    async def wait(self, time_out: float | None = None) -> _ThreadEventMatches:
        """Wait for every target, replacing each target's deadline from this call.

        None clears deadlines. Any target's timeout or predicate error fails
        collection and closes unfinished listeners on the other targets.
        Completed results are returned after context exit as well, without
        contacting target cores or changing deadlines.
        """
        if time_out is not None:
            time_out = validate_number(time_out, "time_out")
        return await self._collect(time_out, reset_deadline=True)

    async def get_result(self) -> _ThreadEventMatches:
        """Await every target; completed results remain readable after context exit."""
        return await self._collect(None, reset_deadline=False)

    def close(self) -> None:
        """Request cancellation on all targets; context exit confirms cleanup."""
        for target in self._targets.values():
            target.close()


@asynccontextmanager
async def expect_event(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    predicate: EventPredicate | None = None,
    time_out: float | None = None,
    keep_listening: bool = False,
) -> AsyncIterator[EventWaiter]:
    """Register an event waiter before running the context body.

    Unlike :func:`wait_event`, this interface separates subscription readiness
    from waiting for the result. Context entry registers synchronously on the
    calling thread without yielding, including on worker cores.
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
    Waiting lends the current thread's dispatch permit. Local context entry
    does not suspend it. keep_listening is available on every local core.

    Example::

        async with expect_event(reply, point="processed") as waiter:
            await pipe.post_event(
                event_type=EventType.WORKFLOW,
                event_name="tool.request",
                context={"reply": reply},
            )
            event = await waiter.wait(time_out=40)
            result = event.context["result"]
    """
    async with EventWaiter(
        event_name, point=point, filter=filter, predicate=predicate,
        time_out=time_out, keep_listening=keep_listening,
    ) as waiter:
        yield waiter


async def wait_event(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    predicate: EventPredicate | None = None,
    time_out: float | None = None,
    keep_listening: bool = False,
) -> ApixEvent | dict[str, ApixEvent] | EventWaiter:
    """Register all patterns and immediately wait for their first matches.

    Subscription starts when this coroutine executes. It registers on the
    current thread's core without yielding before awaiting the result.
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
            Expiry raises ``TimeoutError`` unless ``keep_listening`` is true.
        keep_listening: Return an ``EventWaiter`` on timeout and keep collecting.
            Retrieve the eventual result with ``await waiter.get_result()``;
            call ``waiter.close()`` if abandoning it. Defaults to false.

    Waiting lends the current handler's dispatch permit via ``suspend_process``.
    The waiter owns all handlers and closes them on completion, ordinary timeout
    or cancellation. Only a deferred timeout preserves its listeners.

    This interface never forwards to another thread. Use wait_event_threadsafe
    to collect events from explicitly selected remote cores.

    Example::

        event = await wait_event("job.finished", point="processed", time_out=40)
        result = event.context["result"]
    """
    async with expect_event(
        event_name, point=point, filter=filter, predicate=predicate,
        time_out=time_out, keep_listening=keep_listening,
    ) as waiter:
        return await waiter.wait(time_out=time_out)


async def collect_events(
    event_name: str | list[str] | set[str], *,
    point: Literal['received', 'processed'], filter: list[str] | str | None,
    time_out: float | None, predicate: EventPredicate | None = None,
) -> ApixEvent | dict[str, ApixEvent]:
    """Collect on the owner loop using the deadline set at registration."""
    async with EventWaiter(event_name, point=point, filter=filter, predicate=predicate, time_out=time_out) as waiter:
        return await waiter.get_result()


async def wait_event_threadsafe(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    predicate: EventPredicate | None = None,
    time_out: float | None = None,
    thread_ids: ThreadIds = None,
) -> _ThreadEventMatches:
    """Await event collection on the owner loop from any caller loop.

    None selects the main core; a string selects one named core, and a
    collection returns results keyed by target thread ID. Collection waits
    for every target. Registration starts when each target executes its request. Use
    ``async with expect_event_threadsafe(...)`` to confirm registration before
    publishing. Admission waits asynchronously. Cancellation waits for listener
    cleanup on the owner loop. Results are the original mutable events.
    A predicate also runs on the owner loop; its exceptions reach the caller.
    """
    return await get_thread_bridge().call(partial(
        collect_events, event_name, point=point, filter=filter, predicate=predicate, time_out=time_out,
    ), thread_ids=thread_ids, background=True)


def expect_event_threadsafe(
    event_name: str | list[str] | set[str], *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    predicate: EventPredicate | None = None,
    time_out: float | None = None,
    thread_ids: ThreadIds = None,
) -> ThreadEventWaiter:
    """Build an async waiting context; entry confirms owner-loop registration.

    Publish inside ``async with``, then ``await waiter.wait()``. Replies that
    arrive before waiting are retained. Context exit awaits listener cleanup.
    None selects the main core; a string selects one target, and a collection
    waits on all named cores and returns results keyed by thread ID. Each
    predicate executes on its target loop. Entry confirms every registration;
    failure, timeout or cancellation closes unfinished collections.
    """
    return ThreadEventWaiter(
        event_name, point=point, filter=filter, predicate=predicate,
        time_out=time_out, thread_ids=thread_ids,
    )


# Preserve the original public names as aliases.
wait_for_event = expect_event
await_for = wait_event
wait_for_event_threadsafe = expect_event_threadsafe
await_for_threadsafe = wait_event_threadsafe


__all__ = [
    "EventWaiter",
    "ThreadEventWaiter",
    "expect_event",
    "wait_event",
    "wait_for_event",
    "await_for",
    "expect_event_threadsafe",
    "wait_event_threadsafe",
    "wait_for_event_threadsafe",
    "await_for_threadsafe",
]
