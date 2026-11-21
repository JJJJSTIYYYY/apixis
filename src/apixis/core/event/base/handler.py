"""Handler execution, notifications, cancellation, and registration metadata."""

import asyncio
import traceback
from typing import Any, Awaitable, Callable, Literal, Self
from uuid import uuid4

from apixis.core.event.base.event import ApixEvent, ApixEventError, EventHandlerFunc, EventHandlerErrorFunc
from apixis.core.event.base.process import handler_chain_token_context, suspend_process
from apixis.core.utils.logger import Logger
from apixis.core.utils.validation import validate_non_empty_string, validate_timeout


class ApixEventHandler:
    """Internal execution record owned by the handler registry.

    ``on_has_error`` handles upstream failures only. ``on_error`` receives
    this handler's own uncaught exception after it has been logged and, for
    foreground handlers, recorded. Local try/except/finally blocks may still
    handle recovery and cleanup without reporting an event failure.
    ``on_cancelled`` receives event cancellation notifications from the event
    loop, independently of the normal handler chain. Background cancellation
    only notifies the cancelled background handler.
    Subscription patterns and exclusion filters are private immutable tuples.
    Change them through :func:`subscribe` or :meth:`register`; inspect independent
    metadata snapshots through :func:`get_handler_meta`.
    ``group`` defaults to ``"global"`` and identifies indexed batch removals.
    """

    __slots__ = (
        "_id", "_name", "_group", "_subscribe", "_filter_event", "_core_func",
        "_on_accepted", "_on_has_error", "_on_error", "_on_cancelled",
        "_stop_when_error", "_timeout", "_background", "_priority",
        "_between_handlers", "_register_order",
    )

    _id: str
    _name: str
    _group: str
    _subscribe: tuple[str, ...]
    _filter_event: tuple[str, ...]
    _core_func: EventHandlerFunc
    _on_accepted: EventHandlerFunc | None
    _on_has_error: EventHandlerFunc | None
    _on_error: EventHandlerErrorFunc | None
    _on_cancelled: EventHandlerFunc | None
    _stop_when_error: bool
    _timeout: float | None
    _background: bool
    _priority: float | None
    _between_handlers: tuple[str | None, str | None] | None
    _register_order: float

    def __init__(
        self,
        core_func: EventHandlerFunc,
        on_accepted: EventHandlerFunc | None = None,
        on_has_error: EventHandlerFunc | None = None,
        on_error: EventHandlerErrorFunc | None = None,
        on_cancelled: EventHandlerFunc | None = None,
        *,
        stop_when_error: bool = True,
        time_out: float | None = None,
        background: bool = False,
        name: str | None = None,
        group: str = "global",
    ) -> None:
        for callback in (core_func, on_accepted, on_has_error, on_error, on_cancelled):
            if callback is not None and not callable(callback):
                raise TypeError("Handler functions must be callable.")
        if core_func is None:
            raise TypeError("core_func must be callable.")
        self._core_func: EventHandlerFunc = core_func
        self._on_accepted: EventHandlerFunc | None = on_accepted
        self._on_has_error: EventHandlerFunc | None = on_has_error
        self._on_error: EventHandlerErrorFunc | None = on_error
        self._on_cancelled: EventHandlerFunc | None = on_cancelled
        self._stop_when_error = stop_when_error
        self._time_out = time_out
        self._background = background
        self._name = name or getattr(core_func, "__name__", type(core_func).__name__)
        self._group = validate_non_empty_string(group, "Handler group")
        self._id = "handler-" + uuid4().hex
        self._register_order = -1
        self._subscribe: tuple[str, ...] = ()
        self._filter_event: tuple[str, ...] = ()
        self._priority: float | None = None
        self._between_handlers: tuple[str | None, str | None] | None = None

    @property
    def __name__(self) -> str:
        """Expose the registered name for decorator and inspection tools."""
        return self._name

    async def __call__(self, event: ApixEvent) -> None:
        """Use the same execution contract when called directly."""
        await self.execute(event)

    async def execute(self, event: ApixEvent) -> None:
        """Notify about upstream state, then conditionally execute the core.

        Error notification precedes acceptance notification when both apply.
        Each upstream notification runs at most once. State is checked again after
        notification, so accepting the event there also suppresses the core.
        A timeout applies separately to each invoked function. The handler name
        is appended to seen immediately before the core starts, even if it fails.
        Cancellation is recorded for foreground handlers and then re-raised;
        other failures are logged without terminating dispatch.

        Each failed core or upstream notification calls ``on_error(event, exc)``
        once. A failure in ``on_error`` is recorded without recursive handling.
        Successful error handling does not remove the original error or retry
        the failed function. Cancellation propagates without calling on_error.
        Background exceptions still invoke on_error but never enter error_stack;
        background cancellation propagates without on_error or an error record.
        """
        if event.has_error and self._on_has_error is not None:
            await self._execute_func(self._on_has_error, "on_has_error", event)
        if event.accepted:
            if self._on_accepted is not None:
                await self._execute_func(self._on_accepted, "on_accepted", event)
            return
        if event.has_error and self._stop_when_error:
            return
        event.seen.append(self._name)
        await self._execute_func(self._core_func, "core_func", event)

    async def _execute_func(
        self,
        func: Callable[..., Awaitable[None]],
        phase: Literal["core_func", "on_has_error", "on_accepted", "on_error"],
        event: ApixEvent,
        *args: Any,
    ) -> None:
        """Run one phase and report its own failure without recursive hooks."""
        try:
            chain = handler_chain_token_context.get()
            if chain is not None and not chain.held:
                # A handler timeout may have cancelled its child waiters.
                # Recover capacity before callbacks or the next core resume.
                async with suspend_process():
                    pass
            if self._time_out is None:
                await func(event, *args)
            else:
                async with asyncio.timeout(self._time_out):
                    await func(event, *args)
        except asyncio.CancelledError as exc:
            error = ApixEventError(
                handler_name=self._name,
                phase=phase,
                exception_type=type(exc).__name__,
                message=str(exc),
                traceback=traceback.format_exc(),
            )
            if not self._background:
                event.error_stack.append(error)
            raise
        except Exception as exc:
            error = ApixEventError(
                handler_name=self._name,
                phase=phase,
                exception_type=type(exc).__name__,
                message=str(exc),
                traceback=traceback.format_exc(),
            )
            if not self._background:
                event.error_stack.append(error)
            Logger.error(
                f"Handler failed: event={event.event_name}, "
                f"handler={self._name}, phase={phase}, "
                f"error={error.exception_type}: {error.message}\n"
                f"{error.traceback}"
            )
            if asyncio.current_task().cancelling() and (self._background or event.cancelled):
                raise asyncio.CancelledError from exc
            if phase != "on_error" and self._on_error is not None:
                await self._execute_func(self._on_error, "on_error", event, exc)

    def set_core_func(self, callback: EventHandlerFunc) -> None:
        """Replace the core callback without changing identity or registration.

        The callback must be callable and return an awaitable when invoked.
        Existing invocations finish with their original callback.
        """
        if not callable(callback):
            raise TypeError("Core function must be callable.")
        self._core_func = callback

    def add_has_error_callback(self, callback: EventHandlerFunc, *, exist_ok: bool = True) -> None:
        """Add a callback to be invoked when an upstream handler has failed.

        The callback is invoked once per event if the event has an error in error_stack, before the core function.
        """
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self._on_has_error is not None and not exist_ok:
            raise ValueError("on_has_error already set.")
        self._on_has_error = callback

    def add_on_accepted_callback(self, callback: EventHandlerFunc, *, exist_ok: bool = True) -> None:
        """Add a callback to be invoked when the event has been accepted.

        The callback is invoked once per event if the event has been accepted, before the core function.
        """
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self._on_accepted is not None and not exist_ok:
            raise ValueError("on_accepted already set.")
        self._on_accepted = callback

    def add_on_error_callback(self, callback: EventHandlerErrorFunc, *, exist_ok: bool = True) -> None:
        """Add a callback to be invoked when this handler has failed.

        The callback is invoked once per event if the core function fails.
        """
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self._on_error is not None and not exist_ok:
            raise ValueError("on_error already set.")
        self._on_error = callback

    @property
    def _time_out(self) -> float | None:
        """Maximum callback execution time; None means unlimited."""
        return self._timeout

    @_time_out.setter
    def _time_out(self, value: float | None) -> None:
        self._timeout = validate_timeout(value, "Handler time_out")

    async def notify_cancelled(self, event: ApixEvent, time_out: float | None = None) -> None:
        """Run cancellation cleanup without interrupting other notifications.

        The event loop calls this outside normal execution, then re-raises the
        original cancellation. The runtime owns the event's cancellation state;
        a background task's cancellation does not cancel its shared event.
        Each hook uses this handler's timeout. Cleanup
        failures, including cancellation, are logged only: they do not invoke
        on_error, append business errors, or replace the original cancellation.
        """
        timeout = self._time_out if time_out is None else validate_timeout(time_out, "time_out")
        if self._on_cancelled is None:
            return
        try:
            async with asyncio.timeout(timeout):
                await self._on_cancelled(event)
        except (Exception, asyncio.CancelledError) as exc:
            Logger.error(
                f"Cancellation cleanup failed: event={event.event_name}, "
                f"handler={self._name}, phase=on_cancelled, "
                f"error={type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            )

    def add_on_cancelled_callback(self, callback: EventHandlerFunc, *, exist_ok: bool = True) -> None:
        """Set the event cancellation cleanup callback, optionally rejecting replacement."""
        if not callable(callback):
            raise TypeError("Callback must be callable.")
        if self._on_cancelled is not None and not exist_ok:
            raise ValueError("on_cancelled already set.")
        self._on_cancelled = callback

    def register(
        self,
        *event_names: str,
        exist_ok: bool = True,
        priority: float | None = None,
        between_handlers: tuple[str | None, str | None] | None = None,
        filter_event: list[str] | None = None,
        group: str | None = None,
    ) -> Self:
        """Register this instance in the shared runtime with the same options as subscribe().

        Equivalent to ``subscribe(*event_names, **options)(self)``. At least
        one non-empty event-name pattern is required. Subscriptions and filters
        use case-sensitive fnmatchcase matching. Subscription, filtering and
        ordering options replace previous registration metadata; priority
        defaults to 1 unless between_handlers is supplied.
        ``group=None`` preserves this instance's group; a non-empty string
        moves its registration to that group.

        Matching configuration is private. Call register() again to change it;
        get_handler_meta() returns independent lists for inspection. Changes to
        supplied filter lists after registration do not affect matching.

        Execution options set to None preserve this instance's settings.
        Explicit values override them; non-positive time_out disables the
        timeout. Core and notification callbacks are preserved.

        Handler names are unique across the shared registry. With exist_ok=True,
        this instance replaces the same-name registration. With exist_ok=False,
        a duplicate raises EventHandlerAlreadyRegisteredError. Validation
        failures leave the instance and any existing registration unchanged.

        Returns:
            This instance after successful registration or replacement.
            Registration is synchronous in the shared core.
        """
        # Import at call time because handler_registry imports this class.
        from apixis.core.event.subscription.subscriber import subscribe

        return subscribe(
            *event_names,
            exist_ok=exist_ok,
            priority=priority,
            between_handlers=between_handlers,
            filter_event=filter_event,
            group=group,
            stop_when_error=self._stop_when_error,
            time_out=self._time_out,
            background=self._background,
        )(self)

    def unregister(
        self, *, same_group: bool = False, missing_ok: bool = True,
    ) -> None:
        """Immediately remove the shared registration under this handler's name.

        Equivalent to ``unsubscribe(self.name, missing_ok=missing_ok)``. Remove
        the whole registration, including all subscriptions. Missing names are
        ignored by default; missing_ok=False raises EventHandlerNotRegisteredError.
        Removal is name-based, including when another instance has replaced
        this handler under the same name. Already dequeued events retain this
        handler if it was captured in their chains, even before execution starts.

        With ``same_group=True``, remove every registration in ``self.group``
        using the group index instead. Missing groups follow ``missing_ok``.
        Removal is synchronous in the shared core.
        """
        # Import at call time because handler_registry imports this class.
        from apixis.core.event.subscription.subscriber import unsubscribe

        if same_group:
            unsubscribe(group=self._group, missing_ok=missing_ok)
        else:
            unsubscribe(self._name, missing_ok=missing_ok)


__all__ = [
    "ApixEventHandler",
]
