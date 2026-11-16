"""Global handler subscription, removal, and registration inspection."""

from collections.abc import Callable, Coroutine
from copy import copy, deepcopy
from functools import partial
from threading import current_thread, main_thread
from typing import Any

from apixis.core.event.base.event import EventHandlerFunc
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.factory import get_handler_registry, get_thread_bridge
from apixis.core.utils.validation import validate_number


def subscribe(
    *event_names: str,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
    group: str | None = None,
):
    """Register an async handler for one or more event-name patterns.

    Handler names are unique across the process-global registry. The decorated
    function or ApixEventHandler instance is returned unchanged. Validation
    happens in register_handler when the returned decorator is applied.

    On the main thread, ``subscribe(...)(handler)`` registers synchronously.
    On a worker thread, use ``await subscribe(...)(handler)``; the decorator
    returns a coroutine that waits for registration on the main loop without
    blocking the worker loop. Thread selection happens when the decorator is
    applied. Call ``await start_core()`` on the main thread first.
    Use ``subscribe_threadsafe`` for an always-async registration interface.

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

        group:
            Non-empty, case-sensitive group name for indexed batch removal.
            None preserves a supplied instance's group. New handlers default
            to ``"global"``. Names remain unique across groups.

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
        On worker threads, the decorator returns a coroutine yielding that
        same object after registration completes. Do not use it as ``@subscribe``
        on a worker thread; explicitly await the decorator application instead.

    Raises:
        ValueError:
            If no event-name pattern is supplied; a pattern is empty;
            ``between_handlers`` is malformed; both boundaries are ``None``;
            boundary names are empty or equal; explicit priority is combined
            with boundaries; boundaries are reversed; or priority is not
            finite; or the group is empty.

        TypeError:
            If a priority is not numeric or the decorated callback is not
            callable, or the group is not a string.

        EventHandlerNotRegisteredError:
            If a non-``None`` boundary handler is not actively registered.

        EventHandlerAlreadyRegisteredError:
            If ``exist_ok`` is ``False`` and the function name already exists
            in the handler registry.
    """
    if priority is not None:
        priority = validate_number(priority, "priority")

    def decorator[HandlerT: EventHandlerFunc | ApixEventHandler](
        func: HandlerT,
    ) -> HandlerT | Coroutine[Any, Any, HandlerT]:
        if current_thread() is not main_thread():
            return get_thread_bridge().call_sync(partial(decorator, func))
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
        if group is not None:
            entry.group = group
        # Explicit execution options override the supplied instance's settings.
        # None preserves its setting; non-positive timeouts disable its limit.
        if stop_when_error is not None:
            entry.stop_when_error = stop_when_error
        if time_out is not None:
            entry.time_out = time_out
        if background is not None:
            entry.background = background
        registry = get_handler_registry()
        registry.register_handler(entry, exist_ok=exist_ok)
        if isinstance(func, ApixEventHandler):
            func.__dict__.update(entry.__dict__)
            registry.registry[handler_name] = func
        return func

    return decorator


def subscribe_threadsafe[HandlerT: EventHandlerFunc | ApixEventHandler](
    *event_names: str,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
    group: str | None = None,
) -> Callable[[HandlerT], Coroutine[Any, Any, HandlerT]]:
    """Create an async registration decorator usable from any caller loop.

    Use ``registered = await subscribe_threadsafe("job.*")(handler)``.
    All arguments have the same meaning as :func:`subscribe`, including
    ordering, filtering, instance execution options and group replacement.
    Do not apply this factory with ``@``: its decorator returns a coroutine.

    Call ``await start_core()`` on the main thread before awaiting registration.
    Admission waits asynchronously for bridge capacity. Completion confirms
    registration on the owner loop and returns the original function or
    ApixEventHandler instance. Callbacks continue to execute on the owner loop.
    Validation and registration errors propagate to the awaiting caller.

    Cancellation before execution skips registration. Once registered, a
    handler remains registered even if the caller is cancelled; remove it
    explicitly with ``unsubscribe_threadsafe``. Stopped or uninitialized
    cores raise ``RuntimeError``, as with other threadsafe event APIs.
    """
    register = subscribe(
        *event_names, exist_ok=exist_ok, priority=priority,
        between_handlers=between_handlers, filter_event=filter_event,
        stop_when_error=stop_when_error, time_out=time_out,
        background=background, group=group,
    )

    async def decorator(func: HandlerT) -> HandlerT:
        return await get_thread_bridge().call_sync(partial(register, func))

    return decorator


def asubscribe[HandlerT: EventHandlerFunc | ApixEventHandler](
    *event_names: str,
    exist_ok: bool = True,
    priority: float | None = None,
    between_handlers: tuple[str | None, str | None] | None = None,
    filter_event: list[str] | None = None,
    stop_when_error: bool | None = None,
    time_out: float | None = None,
    background: bool | None = None,
    group: str | None = None,
) -> Callable[[HandlerT], Coroutine[Any, Any, HandlerT]]:
    """Create an always-async registration decorator for main and worker loops.

    Use ``registered = await asubscribe("job.*")(handler)``. All arguments
    have the same meaning as :func:`subscribe`. This factory is applied
    explicitly rather than with ``@`` because its decorator returns a coroutine.

    When the coroutine runs, main-thread callers register directly through
    ``subscribe``; worker callers await ``subscribe_threadsafe``. Completion
    confirms registration and returns the original function or handler instance.
    Callbacks run on the main loop regardless of the registering thread.

    Main-thread registration can precede core startup. Worker callers require
    ``await start_core()`` on the main thread first; bridge admission waits
    asynchronously. Argument and registration errors propagate to the caller.
    Pending worker requests can be cancelled; completed registrations are not
    rolled back. Use ``await unasubscribe(...)`` to remove them explicitly.
    """
    async def decorator(func: HandlerT) -> HandlerT:
        on_main_thread = current_thread() is main_thread()
        register = subscribe if on_main_thread else subscribe_threadsafe
        result = register(
            *event_names, exist_ok=exist_ok, priority=priority,
            between_handlers=between_handlers, filter_event=filter_event,
            stop_when_error=stop_when_error, time_out=time_out,
            background=background, group=group,
        )(func)
        if on_main_thread:
            return result
        return await result

    return decorator


def unsubscribe(
    handler_name: str | None = None,
    *,
    group: str | None = None,
    missing_ok: bool = True,
) -> None | Coroutine[Any, Any, None]:
    """Remove one handler by name or all handlers in an exact group.

    Supply exactly one of ``handler_name`` and ``group``. Group removal uses
    the group index without scanning the handler registry. Unknown names or
    groups are ignored unless ``missing_ok=False``.
    Already dequeued events retain their captured handler references.

    Main-thread calls remove registrations synchronously. On a worker thread,
    use ``await unsubscribe(...)`` to wait for removal on the main loop without
    blocking the worker loop. Call ``await start_core()`` on the main thread
    first. Use ``unsubscribe_threadsafe`` for an always-async interface.
    """
    if current_thread() is not main_thread():
        return unsubscribe_threadsafe(handler_name, group=group, missing_ok=missing_ok)
    if (handler_name is None) == (group is None):
        raise ValueError("Supply exactly one of handler_name and group.")
    registry = get_handler_registry()
    if group is not None:
        registry.unregister_group(group, missing_ok=missing_ok)
    elif handler_name is not None:
        registry.unregister_handler(handler_name, missing_ok=missing_ok)


async def unsubscribe_threadsafe(
    handler_name: str | None = None,
    *,
    group: str | None = None,
    missing_ok: bool = True,
) -> None:
    """Await global handler removal on the owner loop from any caller loop.

    Supply exactly one of ``handler_name`` and ``group``, as with
    :func:`unsubscribe`. ``missing_ok=False`` raises
    ``EventHandlerNotRegisteredError`` for an unknown name or group. Invalid
    selectors raise ``ValueError``. Errors propagate to the awaiting caller.

    Call ``await start_core()`` on the main thread first. Bridge admission
    waits asynchronously; completion confirms removal. Already dequeued events
    retain their captured handler references and can still run them.
    Cancellation before execution skips removal; completed removal is not
    rolled back. Stopped or uninitialized cores raise ``RuntimeError``.
    """
    await get_thread_bridge().call_sync(partial(
        unsubscribe, handler_name, group=group, missing_ok=missing_ok,
    ))


async def unasubscribe(
    handler_name: str | None = None,
    *,
    group: str | None = None,
    missing_ok: bool = True,
) -> None:
    """Await handler removal with one interface for main and worker loops.

    Supply exactly one of ``handler_name`` and ``group``, as with
    :func:`unsubscribe`. Main-thread callers remove directly through
    ``unsubscribe``; worker callers await ``unsubscribe_threadsafe`` after
    main-thread core startup. Bridge capacity waits do not block the worker loop.

    Completion confirms removal. Invalid selectors and strict missing-name or
    missing-group errors propagate to the caller. Cancellation can skip pending
    worker requests; completed removal is not rolled back. Already dequeued
    events retain their captured handlers.
    """
    if current_thread() is main_thread():
        unsubscribe(handler_name, group=group, missing_ok=missing_ok)
        return
    await unsubscribe_threadsafe(handler_name, group=group, missing_ok=missing_ok)


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
        'group': handler.group,
        'register_order': handler._register_order,
        'subscribe': deepcopy(handler.subscribe),
        'filter_event': deepcopy(handler.filter_event),
        'priority': handler.priority,
        'between_handlers': deepcopy(handler.between_handlers),
        'stop_when_error': handler.stop_when_error,
        'time_out': handler.time_out,
        'background': handler.background,
    }


def is_registered(
    handler_name: str,
) -> bool:
    """Return if a handler is registered."""
    return handler_name in get_handler_registry()


def get_unmatched_subscriptions(handler_name: str) -> list[str]:
    """Return global handler patterns that matched no observed event name."""
    return get_handler_registry().get_unmatched_subscriptions(handler_name)


__all__ = [
    "subscribe",
    "subscribe_threadsafe",
    "asubscribe",
    "unsubscribe",
    "unsubscribe_threadsafe",
    "unasubscribe",
    "get_handler",
    "get_handler_meta",
    "is_registered",
    "get_unmatched_subscriptions",
]
