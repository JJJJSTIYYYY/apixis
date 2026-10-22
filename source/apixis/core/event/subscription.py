"""Subscription conveniences backed by the factory-managed handler registry."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import copy
from typing import Literal
from uuid import uuid4

from apixis.core.event.base import ApixEvent, ApixEventHandler, EventHandlerFunc, suspend_process
from apixis.core.event.factory import get_handler_registry


def subscribe(
    *event_names: str,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
):
    """Register an async handler for one or more event-name patterns.

    Handler names are unique across the process-global registry. The decorated
    function or ApixEventHandler instance is returned unchanged. Validation
    happens in register_handler when the returned decorator is applied.

    Event subscription and filtering use case-sensitive
    :func:`fnmatch.fnmatchcase` semantics. The handler chain is resolved lazily
    from the priority buckets when an event is dequeued or the chain is queried.

    Args:
        event_names:
            One or more event-name patterns to subscribe to. Exact names and
            ``fnmatchcase`` wildcards are both supported, including ``*``,
            ``?``, and character classes such as ``[a-z]``.

            Duplicate patterns are removed while preserving their first
            occurrence. At least one non-empty pattern is required.

            Matching is case-sensitive. For example, ``"Graph.*"`` matches
            ``"Graph.Start"`` but does not match ``"graph.start"``.

        exist_ok:
            If ``True``, replace an existing same-name entry with this handler
            and configuration. Validation failures preserve the old registration.

            If ``False``, a duplicate function name raises
            :class:`EventHandlerAlreadyRegisteredError`.

            Uniqueness is based on the handler name, regardless of subscribed
            patterns or callback identity.

        priority:
            Numeric handler priority. Higher values are dispatched first.
            Handlers with the same priority retain registration order.

            Defaults to ``1`` when ``between_handlers`` is not specified.
            ``priority`` and ``between_handlers`` cannot be supplied together.

        between_handlers:
            Insert the handler relative to active handler names already stored
            in ``priority_buckets``. Boundary names refer to handler function
            names and must already be actively registered.

            Supported forms:

            1. ``(left_handler, right_handler)``

               Insert after ``left_handler`` and immediately before
               ``right_handler``. Existing handlers between the boundaries
               remain before the new handler.

               Example::

                   Existing order:
                       left_handler
                       handler_a
                       handler_b
                       right_handler

                   Registration:
                       between_handlers=(
                           "left_handler",
                           "right_handler",
                       )

                   Result:
                       left_handler
                       handler_a
                       handler_b
                       new_handler
                       right_handler

               The left boundary must already dispatch before the right
               boundary. Reversed boundaries raise ``ValueError``.

            2. ``(None, right_handler)``

               Insert immediately before ``right_handler``.

               Example::

                   Existing:
                       handler_a
                       right_handler
                       handler_b

                   Result:
                       handler_a
                       new_handler
                       right_handler
                       handler_b

            3. ``(left_handler, None)``

               Insert immediately after ``left_handler``.

               Example::

                   Existing:
                       handler_a
                       left_handler
                       handler_b

                   Result:
                       handler_a
                       left_handler
                       new_handler
                       handler_b

            When a right boundary is present, the new handler is inserted into
            the right boundary's priority bucket. With only a left boundary,
            it is inserted into the left boundary's bucket. The handler's own
            ``priority`` metadata remains ``None``.

            ``(None, None)`` is invalid. The two boundaries cannot name the
            same handler, and every non-``None`` boundary must be a non-empty
            string.

        filter_event:
            Optional case-sensitive event-name patterns to exclude after a
            subscription matches. A handler is selected only when the exact
            event name matches at least one ``event_names`` pattern and does
            not match any ``filter_event`` pattern.

            Example::

                @subscribe(
                    "graph.*",
                    filter_event=["graph.internal.*"],
                )
                async def public_graph_handler(event):
                    ...

            The example receives ``"graph.started"`` but not
            ``"graph.internal.snapshot"``.

        stop_when_error:
            If ``True``, skip this handler's core function when the event has
            upstream errors. Error notifications still run. If ``False``, the
            core may run after notification unless the event is accepted.
            Background-handler failures are logged without changing event errors.

        time_out:
            Maximum execution time in seconds for each invoked core or
            notification function. ``None`` preserves the supplied instance's
            timeout (unlimited for a plain function). Values less than or equal
            to zero explicitly disable the timeout.

        background:
            If ``True``, schedule the handler as a background task without
            waiting for it before dispatching subsequent handlers.

    Dispatch rules:
        1. Each event instance is dispatched in its own task.
        2. Higher-priority buckets dispatch before lower-priority buckets.
        3. Handlers in one bucket retain their explicit or registration order.
        4. ``between_handlers`` determines placement when supplied and cannot
           be combined with an explicit priority.
        5. Matching handler references and their order are captured when the
           event is dequeued, before waiting for dispatch capacity. Foreground
           execution, background tasks and cancellation notifications use this
           list without registry lookups or matching checks. Registration
           changes affect subsequent dequeues. Captured instances are not copied;
           changes to their callbacks or execution options remain visible.
        6. Events with different exact names may dispatch concurrently.
        7. Calling :meth:`ApixEvent.accept` skips subsequent core functions,
           while applicable error and acceptance notifications still run.
        8. Subscription and ordering options replace existing metadata.
           Execution options set to None preserve a supplied instance's settings;
           explicit values override them. Notification functions are preserved.

    Examples:
        Register handlers by priority::

            @subscribe("graph.*", priority=10)
            async def validate_graph_event(event):
                ...

            @subscribe("graph.*", priority=1)
            async def persist_graph_event(event):
                ...

        Insert a handler before an existing handler::

            @subscribe(
                "graph.*",
                between_handlers=(None, "persist_graph_event"),
            )
            async def enrich_graph_event(event):
                ...

    Returns:
        A decorator that returns the supplied function or handler instance
        unchanged after successful registration or replacement.

    Raises:
        ValueError:
            If no event-name pattern is supplied; a pattern is empty;
            ``between_handlers`` is malformed; both boundaries are ``None``;
            boundary names are empty or equal; explicit priority is combined
            with boundaries; boundaries are reversed; or priority is not
            finite.

        TypeError:
            If a priority is not numeric or the decorated callback is not
            callable.

        EventHandlerNotRegisteredError:
            If a non-``None`` boundary handler is not actively registered.

        EventHandlerAlreadyRegisteredError:
            If ``exist_ok`` is ``False`` and the function name already exists
            in the handler registry.
    """
    if priority is not None and (priority > 9999 or priority < -9999):
        raise ValueError(f"priority must be in the range [-9999, 9999], got {priority}")
    def decorator[HandlerT: EventHandlerFunc | ApixEventHandler](
        func: HandlerT,
    ) -> HandlerT:
        handler_name = func.__name__
        # Stage instance metadata separately so a failed replacement cannot
        # mutate an object that is still registered or being executed.
        entry = (
            copy(func) if isinstance(func, ApixEventHandler)
            else ApixEventHandler(func)
        )
        entry.name = handler_name
        entry.subscribe = list(event_names)
        entry.filter_event = filter_event if filter_event is not None else []
        entry.priority = 1 if priority is None and between_handlers is None else priority
        entry.between_handlers = between_handlers
        # Explicit execution options override the supplied instance's settings.
        # None preserves its setting; non-positive timeouts disable its limit.
        if stop_when_error is not None:
            entry.stop_when_error = stop_when_error
        if time_out is not None:
            entry.time_out = time_out if time_out > 0 else None
        if background is not None:
            entry.background = background
        registry = get_handler_registry()
        registry.register_handler(entry, exist_ok=exist_ok)
        if isinstance(func, ApixEventHandler):
            func.__dict__.update(entry.__dict__)
            registry.registry[handler_name] = func
        return func

    return decorator


def unsubscribe(
    handler_name: str,
    *,
    missing_ok: bool = True,
) -> None:
    """Remove a global handler; optionally reject unknown names.

    Already dequeued events retain their captured handler references.
    """
    get_handler_registry().unregister_handler(handler_name, missing_ok = missing_ok)


def get_handler(
    handler_name: str,
) -> ApixEventHandler | None:
    return get_handler_registry().get_handler(handler_name)


def get_handler_meta(
    handler_name: str,
) -> dict | None:
    handler = get_handler_registry().get_handler(handler_name)
    if handler is None:
        return None
    return {
        'id': handler.id,
        'name': handler.name,
        'register_order': handler._register_order,
        'subscribe': handler.subscribe,
        'filter_event': handler.filter_event,
        'priority': handler.priority,
        'between_handlers': handler.between_handlers,
        'stop_when_error': handler.stop_when_error,
        'time_out': handler.time_out,
        'background': handler.background,
    }


class EventWaiter:
    """One event result retained within a :func:`wait_for_event` context.

    Obtain this object through ``async with wait_for_event(...) as waiter``.
    It stores the first matching event, including one received before ``wait()``
    is called. The stored object is the original mutable :class:`ApixEvent`,
    not a copy. Leaving the context closes the waiter.
    """

    def __init__(self) -> None:
        self._future: asyncio.Future[ApixEvent] = (
            asyncio.get_running_loop().create_future()
        )
        self._deadline: float | None = None
        self._timeout_handle: asyncio.TimerHandle | None = None
        self._closed = False

    async def wait(self) -> ApixEvent:
        """Return the stored event, or wait while lending the handler's permit.

        The timeout is shared by all calls and starts at context entry, not at
        this method call. A result received in time remains available even if
        ``wait()`` is called after the deadline. Timeout raises ``TimeoutError``;
        cancellation propagates and cancels the pending result. Sequential
        calls return the same event. Calling after context exit raises
        ``RuntimeError``.
        """
        if self._closed:
            raise RuntimeError("The event waiter context has already exited.")
        async with suspend_process():
            return await self._future

    def _start_timeout(self, time_out: float | None) -> None:
        if time_out is not None:
            loop = asyncio.get_running_loop()
            self._deadline = loop.time() + time_out
            self._timeout_handle = loop.call_at(self._deadline, self._expire)

    def _expire(self) -> None:
        if not self._future.done():
            self._future.set_exception(TimeoutError("Timed out waiting for an event."))

    async def _resolve(self, event: ApixEvent, *args, **kwargs) -> None:
        if self._future.done():
            return
        if (
            self._deadline is not None
            and asyncio.get_running_loop().time() >= self._deadline
        ):
            self._expire()
            return
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
        self._future.set_result(event)

    def _close(self) -> None:
        self._closed = True
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
        if not self._future.done():
            self._future.cancel()
        elif not self._future.cancelled():
            # The body may exit without wait(), including after a timeout.
            self._future.exception()


def _register_wait_handler(
    waiter: EventWaiter,
    event_name: str,
    point: Literal['received', 'processed'],
    filter: list[str] | str | None,
) -> ApixEventHandler:
    """Register without yielding so context entry is a subscription barrier."""

    if filter is not None and isinstance(filter, str):
        filter = [filter]

    handler = ApixEventHandler(
        core_func=waiter._resolve,
        on_accepted=waiter._resolve,
        on_cancelled=waiter._resolve,
        on_has_error=waiter._resolve,
        on_error=waiter._resolve,
        stop_when_error=False,
        name='resolve_future-'+uuid4().hex
    )

    handler.subscribe = [event_name]
    handler.priority = 10000 if point == 'received' else -10000
    handler.filter_event = filter
    registry = get_handler_registry()
    registry.register_handler(handler, exist_ok=True)
    return handler


@asynccontextmanager
async def wait_for_event(
    event_name: str,
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    time_out: float | None = None,
) -> AsyncIterator[EventWaiter]:
    """Register an event waiter before running the context body.

    Unlike :func:`await_for`, this interface separates subscription readiness
    from waiting for the result. Entering the context registers synchronously,
    without yielding to other tasks before registration, then yields an
    :class:`EventWaiter`. Publish the request inside the context and call
    ``await waiter.wait()`` to get its reply. No task or ``sleep(0)`` is needed;
    a reply received before ``wait()`` is retained.

    Merely constructing this context manager does not register anything. Neither
    interface replays events whose handler chains were captured before
    registration. Each context stores only its first matching event.

    Args:
        event_name: Case-sensitive glob pattern to subscribe to.
        point: ``'received'`` resolves before ordinary foreground handlers;
            ``'processed'`` resolves after them. Background handlers are not
            awaited. Both return the original mutable ``ApixEvent``.
        filter: Glob pattern or list of patterns to exclude.
        time_out: Seconds from registration until the matching event reaches
            the selected point. ``None`` means no deadline; non-positive values
            expire immediately. The deadline includes time spent publishing
            or doing other work before ``wait()``. It expires the result, not
            the context body: ``wait()`` raises ``TimeoutError``. A result
            received before the deadline remains available afterwards.

    The handler is unregistered on every context exit, including publication
    errors, timeouts and cancellation. Exiting without waiting cancels a pending
    result. Only ``wait()`` lends the current handler's dispatch permit; context
    entry does not suspend the handler chain.

    Example::

        async with wait_for_event(reply, point="processed", time_out=40) as waiter:
            await pipe.post_event(
                event_type=EventType.WORKFLOW,
                event_name="tool.request",
                context={"reply": reply},
            )
            event = await waiter.wait()
            result = event.context["result"]
    """
    waiter = EventWaiter()
    handler = _register_wait_handler(waiter, event_name, point, filter)
    try:
        waiter._start_timeout(time_out)
        yield waiter
    finally:
        waiter._close()
        handler.unregister(missing_ok=True)


async def await_for(
    event_name: str,
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    time_out: float | None = None,
) -> ApixEvent:
    """Register and immediately wait for the first matching event.

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
        event_name: Case-sensitive glob pattern to subscribe to.
        point: ``'received'`` resolves before ordinary foreground handlers;
            ``'processed'`` resolves after them, without awaiting background
            handlers. The returned ``ApixEvent`` is mutable, not a snapshot.
        filter: Glob pattern or list of patterns to exclude.
        time_out: Maximum seconds spent waiting after registration, or ``None``
            for no limit. Time before this coroutine starts is not included.
            Expiry raises ``TimeoutError``.

    Waiting lends the current handler's dispatch permit via ``suspend_process``.
    The temporary handler is removed on completion, timeout or cancellation.

    Example::

        event = await await_for("job.finished", point="processed", time_out=40)
        result = event.context["result"]
    """
    waiter = EventWaiter()
    handler = _register_wait_handler(waiter, event_name, point, filter)

    try:
        async with suspend_process():
            if time_out is not None:
                result = await asyncio.wait_for(waiter._future, timeout=time_out)
            else:
                result = await waiter._future
    finally:
        waiter._close()
        handler.unregister(missing_ok=True)
    return result



def is_registered(
    handler_name: str,
) -> bool:
    """Return if a handler is registered."""
    return handler_name in get_handler_registry()


def get_unmatched_subscriptions(handler_name: str) -> list[str]:
    """Return global handler patterns that matched no observed event name."""
    return get_handler_registry().get_unmatched_subscriptions(handler_name)


__all__ = [
    "subscribe", "unsubscribe", "get_handler", "get_handler_meta",
    "is_registered", "get_unmatched_subscriptions",
    "await_for", "wait_for_event", "EventWaiter"
]
