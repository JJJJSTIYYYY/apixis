"""Bounded thread-to-loop requests; all event operations run on the owner loop."""

import asyncio
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from concurrent.futures import Future, InvalidStateError
from contextlib import AbstractAsyncContextManager, nullcontext
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from apixis.core.event.base.process import handler_chain_token_context, suspend_process
from apixis.core.event.base.status import EventThreadBridgeStatus


@dataclass(eq=False)
class ThreadSubmission[T]:
    """A result and an acknowledgement that owner-loop cleanup has finished."""

    operation: Callable[[], Awaitable[T]] | None
    context: Context | None
    background: bool
    result: Future[T] = field(default_factory=Future)
    closed: Future[None] = field(default_factory=Future)
    task: asyncio.Task[None] | None = None


def complete_future(future: Future[Any], *, value: Any = None, error: BaseException | None = None) -> None:
    """Complete once; cancellation or shutdown may concurrently finish a Future."""
    try:
        if error is None:
            future.set_result(value)
        else:
            future.set_exception(error)
    except InvalidStateError:
        pass


class EventThreadBridge:
    """Transfer bounded requests without moving asyncio objects between threads.

    Publications are awaited serially so a full event pipe cannot create an
    unbounded collection of publishing tasks. Background operations register
    event waiters and collect results independently of subsequent requests.
    A single lock protects queue capacity and the idle-to-running handoff;
    producers share one scheduled consumer until it drains the queue. Async
    admission uses cross-thread Futures rather than a caller's executor.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, *, maxsize: int) -> None:
        self.loop = loop
        self._requests: deque[ThreadSubmission[Any]] = deque()
        self._maxsize = maxsize
        self._admission = Lock()
        self._async_admission: OrderedDict[Future[None], None] = OrderedDict()
        self._closed = False
        self._consumer_scheduled = False
        self._consumer: asyncio.Task[None] | None = None
        self._active: set[ThreadSubmission[Any]] = set()

    @property
    def closed(self) -> bool:
        with self._admission:
            return self._closed

    def get_status(self) -> EventThreadBridgeStatus:
        """Snapshot on the owner thread; lock admission state shared by workers."""
        with self._admission:
            return EventThreadBridgeStatus(
                running=not self._closed and self.loop.is_running(),
                queue_size=len(self._requests),
                queue_capacity=self._maxsize,
                admission_waiters=len(self._async_admission),
                active_requests=len(self._active),
            )

    async def asubmit[T](
        self, operation: Callable[[], Awaitable[T]], *, background: bool = False,
    ) -> ThreadSubmission[T]:
        """Wait for admission without blocking the calling asyncio loop."""
        request = self._make_request(operation, background=background)
        try:
            while True:
                with self._admission:
                    if self._try_enqueue(request):
                        return request
                    available: Future[None] = Future()
                    self._async_admission[available] = None
                try:
                    await asyncio.wrap_future(available)
                except BaseException:
                    with self._admission:
                        self._async_admission.pop(available, None)
                        if len(self._requests) < self._maxsize:
                            # A cancelled producer may already have received
                            # the only wakeup for this free slot.
                            self._notify_admission()
                    raise
        except asyncio.CancelledError:
            request.result.cancel()
            await asyncio.shield(asyncio.wrap_future(request.closed))
            raise

    def suspend_caller(self) -> AbstractAsyncContextManager[None]:
        """Lend managed caller capacity only on the bridge's owner loop."""
        return suspend_process() if asyncio.get_running_loop() is self.loop else nullcontext()

    async def call[T](
        self, operation: Callable[[], Awaitable[T]], *, background: bool = False,
    ) -> T:
        """Await an owner-loop operation and finish cleanup when cancelled."""
        async with self.suspend_caller():
            request = await self.asubmit(operation, background=background)
            try:
                return await asyncio.wrap_future(request.result)
            except asyncio.CancelledError:
                request.result.cancel()
                await asyncio.shield(asyncio.wrap_future(request.closed))
                raise

    def _make_request[T](
        self, operation: Callable[[], Awaitable[T]], *, background: bool,
    ) -> ThreadSubmission[T]:
        context = copy_context()
        context.run(handler_chain_token_context.set, None)
        request = ThreadSubmission(operation, context, background)
        request.result.add_done_callback(lambda future: self._on_result_done(request, future))
        return request

    def _try_enqueue(self, request: ThreadSubmission) -> bool:
        """Check capacity and schedule the consumer under the admission lock."""
        if self._closed or not self.loop.is_running():
            raise RuntimeError("The event thread bridge is not running; await start_core() first.")
        if self._maxsize > 0 and len(self._requests) >= self._maxsize:
            return False
        self._requests.append(request)
        if not self._consumer_scheduled:
            self._consumer_scheduled = True
            self.loop.call_soon_threadsafe(self._ensure_consumer, context=Context())
        return True

    def _notify_admission(self) -> None:
        """Wake an async producer after capacity becomes available."""
        while self._async_admission:
            available, _ = self._async_admission.popitem(last=False)
            try:
                available.set_result(None)
            except InvalidStateError:
                # Cancellation can race capacity delivery from another loop.
                continue
            break

    def _on_result_done(self, request: ThreadSubmission, future: Future) -> None:
        if not future.cancelled():
            return
        if not self.loop.is_closed():
            try:
                self.loop.call_soon_threadsafe(self._cancel, request, context=Context())
            except RuntimeError:
                # Closing a loop can race a cancelling worker thread.
                if not self.loop.is_closed():
                    raise

    def _cancel(self, request: ThreadSubmission) -> None:
        if request.task is not None:
            request.task.cancel()
        elif not request.closed.done():
            self._release(request)

    def _ensure_consumer(self) -> None:
        # Only the owner loop changes _closed or runs this callback.
        if not self._closed:
            self._consumer = self.loop.create_task(self._consume(), context=Context())

    async def _consume(self) -> None:
        while True:
            with self._admission:
                if self._closed or not self._requests:
                    self._consumer_scheduled = False
                    return
                request = self._requests.popleft()
                self._notify_admission()
            if request.result.cancelled():
                if not request.closed.done():
                    self._release(request)
                continue
            self._active.add(request)
            request.task = self.loop.create_task(self._execute(request), context=request.context)
            request.task.add_done_callback(lambda task, request=request: self._on_task_done(request, task))
            if request.background:
                # Let registration run before handling another queued request.
                await asyncio.sleep(0)
            else:
                try:
                    await request.task
                except asyncio.CancelledError:
                    if asyncio.current_task().cancelling():
                        raise

    async def _execute(self, request: ThreadSubmission) -> None:
        try:
            value = await request.operation()
        except asyncio.CancelledError:
            request.result.cancel()
        except Exception as error:
            complete_future(request.result, error=error)
        else:
            complete_future(request.result, value=value)

    def _on_task_done(self, request: ThreadSubmission, task: asyncio.Task) -> None:
        if task.cancelled():
            request.result.cancel()
        self._active.discard(request)
        self._release(request)

    def _release(self, request: ThreadSubmission) -> None:
        request.operation = None
        request.context = None
        request.task = None
        request.closed.set_result(None)

    async def close(self) -> None:
        """Reject new requests and finish pending results before returning."""
        with self._admission:
            self._closed = True
            self._consumer_scheduled = False
            requests, self._requests = self._requests, deque()
            waiters, self._async_admission = self._async_admission, OrderedDict()
        for available in waiters:
            complete_future(available, error=RuntimeError("The event thread bridge is not running; await start_core() first."))
        for request in requests:
            if not request.result.done():
                complete_future(request.result, error=RuntimeError("The event core stopped."))
            if not request.closed.done():
                self._release(request)
        tasks = []
        for request in tuple(self._active):
            if not request.result.done():
                complete_future(request.result, error=RuntimeError("The event core stopped."))
            if request.task is not None:
                request.task.cancel()
                tasks.append(request.task)
        if self._consumer is not None:
            self._consumer.cancel()
            tasks.append(self._consumer)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


__all__ = ["EventThreadBridge", "ThreadSubmission", "complete_future"]
