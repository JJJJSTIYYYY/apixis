import asyncio
from contextvars import copy_context
from datetime import datetime
from functools import partial
import traceback

from apixis.core.config.core_config import (
    EVENT_LOOP_BACKPRESSURE,
    SHOW_EVENT_DISPATCH,
)
from apixis.core.event.base.event import ApixEvent
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.base.process import HandlerChainToken, handler_chain_token_context
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.base.status import EventLoopStatus
from apixis.core.utils.logger import Logger
from apixis.core.utils.semaphore import ApixisSemaphore
from apixis.core.utils.exception import CoreClosedError


class ApixEventLoop:
    """Consume events and limit concurrent foreground event dispatches."""

    def __init__(
        self,
        registry: ApixHandlerRegistry,
        event_pipe: ApixEventPipe,
        event_registry: ApixEventRegistry,
    ):
        self._registry = registry
        self._event_pipe = event_pipe
        self._event_registry = event_registry

        self._event_semaphore = ApixisSemaphore(EVENT_LOOP_BACKPRESSURE)

        self._event_consumer_task: asyncio.Task[None] | None = None
        self._dispatch_tasks: dict[asyncio.Task, ApixEvent] = {}
        self._background_handler_tasks: dict[asyncio.Task, ApixEvent] = {}

        # An event may already be dequeued while waiting for foreground
        # dispatch capacity. Keep the event and its captured handler references
        # together so stopping the consumer cannot lose or re-resolve them.
        self._pending_dispatch: tuple[ApixEvent, list[ApixEventHandler]] | None = None

    @property
    def is_running(self) -> bool:
        """Whether a live queue consumer is currently assigned."""
        return self._event_consumer_task is not None and not self._event_consumer_task.done()

    def owns_task(self, task: asyncio.Task | None) -> bool:
        """Whether a task belongs to foreground or background execution."""
        return task in self._dispatch_tasks or task in self._background_handler_tasks

    def cancel_in_flight(self) -> None:
        """Cancel foreground/background work without interrupting existing cleanup."""
        for tasks in (self._dispatch_tasks, self._background_handler_tasks):
            for task, event in tasks.items():
                if task.done():
                    continue
                if tasks is self._dispatch_tasks:
                    event.cancelled = True
                if not task.cancelling():
                    task.cancel()

    def get_status(self) -> EventLoopStatus:
        """Read tracked work on the runtime loop without starting consumption."""
        return EventLoopStatus(
            running=self.is_running,
            dispatch_capacity=self._event_semaphore.capacity,
            dispatch_permits_in_use=self._event_semaphore.in_use,
            dispatch_tasks=len(self._dispatch_tasks),
            pending_dispatch=self._pending_dispatch is not None,
            background_tasks=len(self._background_handler_tasks),
        )

    async def start(self) -> asyncio.Task[None]:
        """Start the local consumer and return its task without awaiting it.

        Repeated starts return the same live task. After it completes, an
        explicit start creates a replacement. The caller may observe task
        completion to coordinate its own lifecycle; this component only
        cleans up consumer ownership and reports consumer failures.

        Raises CoreClosedError if the event pipe has been permanently closed.
        """
        if self._event_pipe.closed:
            raise CoreClosedError("The event pipe is closed.")
        task = self._event_consumer_task
        if task is not None and not task.done():
            return task

        task = asyncio.create_task(
            self._event_consumer_loop(),
            name="pipe-event-consumer",
        )
        self._event_consumer_task = task
        task.add_done_callback(self._on_consumer_done)
        Logger.info("Worker started.")
        return task

    def _on_consumer_done(self, task: asyncio.Task) -> None:
        """Reset startup state when the current consumer exits for any reason."""
        # An old consumer may finish after stop() has started a replacement.
        # Completion callbacks also cover cancellation before the coroutine runs.
        if self._event_consumer_task is task:
            self._event_consumer_task = None
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                Logger.error(f"Event consumer failed: {type(error).__name__}: {error}")

    async def stop(self) -> None:
        """Pause consumption, preserving queued and pending events.

        Queued events remain available for restart. An event already dequeued
        but still waiting for dispatch capacity retains its captured handlers
        for the next consumer. Existing dispatch and background tasks continue
        without being awaited here.

        Consumer failures are recorded by its completion callback and are not
        raised again by stop. Errors in the stop operation itself propagate.
        """
        task = self._event_consumer_task

        if task is None:
            return

        # A concurrent start() may create a replacement; only stop this worker.
        if not task.cancelling():
            task.cancel()

        # Consumer failures are reported once by _on_consumer_done.
        # Keep ownership until completion, even if the stop operation is cancelled.
        await asyncio.shield(asyncio.gather(task, return_exceptions=True))

        Logger.info("Worker stopped.")

    async def close(self) -> None:
        """Cancel handler execution and discard queued and pending events.

        The owning core seals pipe admission first. Running handler chains
        deliver their own cancellation callbacks. Unassigned events are only
        removed and acknowledged; closing does not dispatch notifications for them.

        Consumer failures are recorded by its completion callback. Cleanup
        failures propagate to the owning core; they do not signify closure.
        """
        self.cancel_in_flight()
        await self.stop()
        # Standalone eager cleanup may begin before the consumer registers
        # its current dispatch task. Stopping completes that ownership handoff.
        self.cancel_in_flight()
        if self._pending_dispatch is not None:
            self._pending_dispatch = None
            self._event_pipe.task_done()
        await self._event_pipe.clear()
        # Complete waiter Futures before joining handlers whose cleanup may
        # await them. This does not notify discarded events.
        await self._registry.cancel_waiters()
        while self._dispatch_tasks or self._background_handler_tasks:
            await asyncio.shield(asyncio.gather(
                *self._dispatch_tasks, *self._background_handler_tasks,
                return_exceptions=True,
            ))
            # gather() can complete synchronously for already-done tasks.
            # Their ownership callbacks must run before inspecting the sets.
            await asyncio.sleep(0)

    async def _event_consumer_loop(self) -> None:
        """Dequeue events and launch dispatch when foreground capacity is available."""
        try:
            while True:
                # Dequeue before acquiring dispatch capacity. This prevents an
                # idle consumer from occupying a slot needed by a suspended
                # foreground handler when it tries to reacquire that slot.
                if self._pending_dispatch is None:
                    event = await self._event_pipe.get()
                    try:
                        # Capture references and order synchronously at dequeue.
                        # Capacity waits must not expose this event to later
                        # registration changes.
                        handler_chain = (
                            self._registry.get_handlers_for_event(event.event_name)
                            if event.event_name
                            else []
                        )
                    except BaseException as exc:
                        self._event_pipe.task_done()
                        if not isinstance(exc, Exception):
                            raise
                        Logger.error(
                            f"Handler chain resolution failed: "
                            f"{type(exc).__name__}: {exc}\n"
                            f"{traceback.format_exc()}"
                        )
                        continue
                    self._pending_dispatch = event, handler_chain

                # If cancellation happens here, the event and its handlers
                # remain pending and will be resumed by the next consumer.
                await self._event_semaphore.acquire()

                event, handler_chain = self._pending_dispatch
                self._pending_dispatch = None

                chain = HandlerChainToken(self._event_semaphore)
                try:
                    if event.event_name:
                        self._event_registry.record_event(event)

                    if SHOW_EVENT_DISPATCH:
                        print(
                            f"\033[38;5;59m"
                            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            f"\033[0m"
                            f" \033[1;38;5;147m[EVENT LOOP]\033[0m"
                            f" \033[38;5;59m│\033[0m"
                            f" \033[38;5;116m{event.event_name}\033[0m"
                        )

                    # Transfer the snapshot to the task; retained events should
                    # not keep the publisher's context variables alive.
                    execution_context, event._execution_context = (
                        event._execution_context, None,
                    )
                    if execution_context is None:
                        execution_context = copy_context()
                    execution_context.run(handler_chain_token_context.set, chain)
                    coroutine = self._dispatch_event(event, handler_chain)
                    try:
                        task = asyncio.create_task(coroutine, context=execution_context)
                    except BaseException:
                        coroutine.close()
                        raise

                except BaseException as exc:
                    chain.closed = True
                    if chain.held:
                        self._event_semaphore.release()
                        chain.held = False
                    self._event_pipe.task_done()

                    if not isinstance(exc, Exception):
                        raise

                    Logger.error(
                        f"Dispatch preparation failed: "
                        f"{type(exc).__name__}: {exc}\n"
                        f"{traceback.format_exc()}"
                    )

                else:
                    self._dispatch_tasks[task] = event
                    task.add_done_callback(partial(self._on_dispatch_done, chain=chain))
                    self._event_pipe.task_done()

                finally:
                    # The dispatch task owns these references. Drop the
                    # consumer's copies before the next dequeue.
                    event = handler_chain = execution_context = task = None

        except asyncio.CancelledError:
            Logger.info("Event loop cancelled.")

    async def _dispatch_event(
        self,
        event: ApixEvent,
        handler_chain: list[ApixEventHandler],
    ) -> ApixEvent | None:
        """Dispatch an event and notify foreground handlers on cancellation."""
        chain = handler_chain_token_context.get()
        if chain is not None:
            # Initialize inside the coroutine, before user code, including
            # when an application installs an eager task factory.
            chain.owner = asyncio.current_task()
        Logger.debug(
            f"Dispatching event `{event.event_name}` "
            f"to {len(handler_chain)} handlers."
        )

        try:
            if not event.event_name:
                return None

            if not handler_chain:
                return event

            for handler in handler_chain:
                if event.cancelled and asyncio.current_task().cancelling():
                    raise asyncio.CancelledError
                if handler._background:
                    self._create_background_handler_task(
                        handler,
                        event,
                    )
                else:
                    await self._execute_handler(
                        handler,
                        event,
                    )

            if event.cancelled and asyncio.current_task().cancelling():
                raise asyncio.CancelledError
            return event

        except asyncio.CancelledError:
            # Notify the same captured foreground handlers in chain order.
            event.cancelled = True
            for handler in handler_chain:
                if not handler._background:
                    try:
                        await handler.notify_cancelled(event, time_out=5.0)
                    except (Exception, asyncio.CancelledError):
                        pass
            raise

        except Exception as exc:
            Logger.error(
                f"Dispatch failed: {type(exc).__name__}: "
                f"{exc}\n{traceback.format_exc()}"
            )

    def _on_dispatch_done(
        self, task: asyncio.Task, *, chain: HandlerChainToken,
    ) -> None:
        """Close the chain and release its permit only if it still owns one."""
        chain.closed = True
        if chain.resuming_task is not None:
            # A detached child may be waiting after its dispatch has ended.
            chain.resuming_task.cancel()
        chain.owner = None
        if chain.held:
            self._event_semaphore.release()
            chain.held = False
        self._dispatch_tasks.pop(task)

        if not task.cancelled():
            error = task.exception()
            if error is not None:
                Logger.error(
                    f"Dispatch task failed: "
                    f"{type(error).__name__}: {error}"
                )

    async def _execute_handler(
        self,
        handler: ApixEventHandler,
        event: ApixEvent,
    ) -> None:
        """Execute one foreground handler in the current event dispatch."""
        try:
            await handler.execute(event)

        except Exception as exc:
            Logger.error(
                f"Foreground handler execution failed: "
                f"{type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc()}"
            )

    def _create_background_handler_task(
        self,
        handler: ApixEventHandler,
        event: ApixEvent,
    ) -> None:
        """Create one background handler task without foreground semaphore context."""
        coroutine = self._run_background_handler(handler, event)

        # Background handlers do not own foreground event capacity. Clearing
        # the context also prevents their event publishing from releasing or
        # reacquiring a foreground event slot.
        token = handler_chain_token_context.set(None)

        try:
            task = asyncio.create_task(coroutine)
        except BaseException:
            coroutine.close()
            raise
        finally:
            handler_chain_token_context.reset(token)

        self._background_handler_tasks[task] = event
        task.add_done_callback(self._on_background_handler_done)

    def _on_background_handler_done(
        self,
        task: asyncio.Task,
    ) -> None:
        """Remove a completed background handler task."""
        self._background_handler_tasks.pop(task)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                Logger.error(
                    f"Background task failed: "
                    f"{type(error).__name__}: {error}"
                )

    async def _run_background_handler(
        self,
        handler: ApixEventHandler,
        event: ApixEvent,
    ) -> None:
        """Execute one background handler and notify only its own cancellation."""
        try:
            try:
                await handler.execute(event)
                if asyncio.current_task().cancelling():
                    raise asyncio.CancelledError

            except asyncio.CancelledError:
                # Notification failure must never replace the original
                # cancellation of the background task.
                try:
                    await handler.notify_cancelled(
                        event,
                        time_out=5.0,
                    )
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    Logger.error(
                        f"Background cancellation notification failed: "
                        f"{type(exc).__name__}: {exc}\n"
                        f"{traceback.format_exc()}"
                    )

                raise

        except Exception as exc:
            Logger.error(
                f"Background handler execution failed: "
                f"{type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc()}"
            )


__all__ = ["ApixEventLoop"]
