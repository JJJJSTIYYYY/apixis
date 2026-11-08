"""Bounded thread-to-loop requests; all event operations run on the owner loop."""

import asyncio
from collections.abc import Awaitable, Callable
from concurrent.futures import Future, InvalidStateError
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from queue import Empty, Queue
from threading import Condition, current_thread, main_thread
from typing import Any

from apixis.core.event.base import handler_chain_context


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
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, *, maxsize: int) -> None:
        self.loop = loop
        self._requests: Queue[ThreadSubmission[Any]] = Queue(maxsize=maxsize)
        self._admission = Condition()
        self._closed = False
        self._consumer: asyncio.Task[None] | None = None
        self._active: set[ThreadSubmission[Any]] = set()

    @property
    def closed(self) -> bool:
        with self._admission:
            return self._closed

    def submit[T](
        self, operation: Callable[[], Awaitable[T]], *, background: bool = False,
    ) -> ThreadSubmission[T]:
        """Wait in the calling worker thread until request capacity is available."""
        request = self._make_request(operation, background=background)
        self._enqueue(request)
        return request

    async def asubmit[T](
        self, operation: Callable[[], Awaitable[T]], *, background: bool = False,
    ) -> ThreadSubmission[T]:
        """Wait for admission without blocking the calling asyncio loop."""
        request = self._make_request(operation, background=background)
        admitting = asyncio.create_task(asyncio.to_thread(self._enqueue, request))
        try:
            await asyncio.shield(admitting)
        except asyncio.CancelledError:
            request.result.cancel()
            await asyncio.shield(admitting)
            raise
        return request

    def _make_request[T](
        self, operation: Callable[[], Awaitable[T]], *, background: bool,
    ) -> ThreadSubmission[T]:
        context = copy_context()
        context.run(handler_chain_context.set, None)
        request = ThreadSubmission(operation, context, background)
        request.result.add_done_callback(lambda future: self._on_result_done(request, future))
        return request

    def _enqueue(self, request: ThreadSubmission) -> None:
        with self._admission:
            while True:
                if request.result.cancelled():
                    return
                if self._closed or not self.loop.is_running():
                    raise RuntimeError("The event thread bridge is not running; await start_core() first.")
                if not self._requests.full():
                    break
                if current_thread() is main_thread():
                    raise RuntimeError("Use await post_event() or await_for() instead of blocking the main thread.")
                self._admission.wait()
            self._requests.put_nowait(request)
            self.loop.call_soon_threadsafe(self._ensure_consumer, context=Context())

    def _on_result_done(self, request: ThreadSubmission, future: Future) -> None:
        if not future.cancelled():
            return
        with self._admission:
            self._admission.notify_all()
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
        if not self.closed and (self._consumer is None or self._consumer.done()):
            self._consumer = self.loop.create_task(self._consume(), context=Context())

    async def _consume(self) -> None:
        while not self.closed:
            with self._admission:
                try:
                    request = self._requests.get_nowait()
                except Empty:
                    return
                self._requests.task_done()
                self._admission.notify()
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
                await asyncio.gather(request.task, return_exceptions=True)

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
            self._admission.notify_all()
        while True:
            try:
                request = self._requests.get_nowait()
            except Empty:
                break
            self._requests.task_done()
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
