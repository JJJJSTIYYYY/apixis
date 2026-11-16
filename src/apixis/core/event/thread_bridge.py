"""Process-wide routing and bounded receiver endpoints for independent thread cores."""

import asyncio
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from concurrent.futures import Future, InvalidStateError
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from threading import Lock, Thread, current_thread, main_thread
from typing import Any

from apixis.core.event.base.process import handler_chain_token_context, suspend_process
from apixis.core.event.base.status import EventThreadBridgeStatus
from apixis.core.utils.tasks import gather_results
from apixis.core.utils.validation import validate_non_empty_string


type ThreadIds = str | list[str] | tuple[str, ...] | set[str] | None


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


class ThreadEndpoint:
    """Transfer bounded requests without moving asyncio objects between threads.

    Publications are awaited serially so a full event pipe cannot create an
    unbounded collection of publishing tasks. Background operations, including
    event waiters, run independently of subsequent requests.
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
        """Read the close marker; admission rechecks it under its queue lock."""
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

    async def call[T](
        self, operation: Callable[[], Awaitable[T]], *, background: bool = False,
    ) -> T:
        """Await one target operation and finish its cleanup when cancelled.

        The process-wide router owns caller-permit suspension. This endpoint
        owns only target admission, execution and acknowledgement.
        """
        request = await self.asubmit(operation, background=background)
        try:
            return await asyncio.wrap_future(request.result)
        except asyncio.CancelledError:
            request.result.cancel()
            await asyncio.shield(asyncio.wrap_future(request.closed))
            raise
        except Exception:
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
        if len(self._requests) >= self._maxsize:
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
        current = asyncio.current_task()
        closing_from_request = False
        for request in tuple(self._active):
            if not request.result.done():
                complete_future(request.result, error=RuntimeError("The event core stopped."))
            if request.task is not None:
                if request.task is current:
                    # A routed stop_core() must finish its own shutdown rather
                    # than cancelling or awaiting itself through the consumer.
                    closing_from_request = True
                else:
                    request.task.cancel()
                    tasks.append(request.task)
        if self._consumer is not None and not closing_from_request:
            self._consumer.cancel()
            tasks.append(self._consumer)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


class ThreadBridge:
    """Process-wide directory routing explicit requests to named thread cores.

    Registration and target resolution take one directory lock. Each target
    owns its bounded endpoint and asyncio state; ordinary local operations do
    not visit this directory. No event is forwarded implicitly.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._owners: dict[str, Thread] = {}
        self._endpoints: dict[str, ThreadEndpoint] = {}
        self._main_thread_id: str | None = None

    def register(self, thread_id: str, endpoint: ThreadEndpoint | None = None) -> None:
        """Reserve a name for the calling thread, optionally binding its endpoint."""
        validate_non_empty_string(thread_id, "thread_id")
        owner = current_thread()
        with self._lock:
            previous = self._owners.get(thread_id)
            if previous is not None and previous is not owner and previous.is_alive():
                raise ValueError(f"Thread core {thread_id!r} is already registered.")
            if previous is not owner:
                self._endpoints.pop(thread_id, None)
            self._owners[thread_id] = owner
            if endpoint is not None:
                self._endpoints[thread_id] = endpoint
            if owner is main_thread():
                self._main_thread_id = thread_id

    def unregister(self, thread_id: str) -> None:
        """Release the caller's name after its endpoint has closed."""
        with self._lock:
            if self._owners.get(thread_id) is current_thread():
                self._owners.pop(thread_id)
                self._endpoints.pop(thread_id, None)
                if self._main_thread_id == thread_id:
                    self._main_thread_id = None

    def targets(self, thread_ids: ThreadIds = None) -> dict[str, ThreadEndpoint]:
        """Resolve every target before submission; None selects the main core.

        A string selects one target. Lists, tuples and sets select a non-empty
        group and deduplicate names. Missing, stopped or unstarted cores raise
        RuntimeError. Resolved endpoints are retained for the entire operation,
        so restarting a target cannot redirect an in-flight request.
        """
        if thread_ids is not None and not isinstance(thread_ids, (str, list, tuple, set)):
            raise TypeError("thread_ids must be a string or a list, tuple or set of strings.")
        names = (thread_ids,) if isinstance(thread_ids, str) else thread_ids
        if names is not None:
            if not names:
                raise ValueError("thread_ids cannot be empty.")
            for name in names:
                validate_non_empty_string(name, "thread_ids")
        with self._lock:
            if names is None:
                if self._main_thread_id is None:
                    raise RuntimeError("The main thread core is not running; await start_core() there first.")
                names = (self._main_thread_id,)
            targets = {}
            for name in names:
                endpoint = self._endpoints.get(name)
                owner = self._owners.get(name)
                if endpoint is None or not owner.is_alive() or endpoint.closed or not endpoint.loop.is_running():
                    raise RuntimeError(f"Thread core {name!r} is not running; await start_core() there first.")
                targets[name] = endpoint
            return targets

    async def call[T](
        self, operation: Callable[[], Awaitable[T]], *,
        thread_ids: ThreadIds = None, background: bool = True,
    ) -> T | dict[str, T]:
        """Execute on the selected cores, lending only the caller's local permit.

        None or a string returns a single result; a collection returns results
        keyed by thread ID, including for a one-item collection. All targets
        run concurrently. On failure or cancellation unfinished requests are
        cancelled and their cleanup is awaited; completed effects remain.

        Async operations run independently by default so nested requests can
        enter the same endpoint. Use background=False only for operations
        that do not await further bridge requests or handler completion.
        """
        targets = self.targets(thread_ids)
        async with suspend_process():
            if thread_ids is None or isinstance(thread_ids, str):
                endpoint = next(iter(targets.values()))
                return await endpoint.call(operation, background=background)
            results = await gather_results(
                endpoint.call(operation, background=background) for endpoint in targets.values()
            )
        return dict(zip(targets, results))

    async def call_sync[T](
        self, operation: Callable[[], T], *, thread_ids: ThreadIds = None,
    ) -> T | dict[str, T]:
        """Run a synchronous operation on each selected core without yielding there."""
        async def execute() -> T:
            return operation()

        return await self.call(execute, thread_ids=thread_ids, background=False)


_thread_bridge = ThreadBridge()


def get_thread_bridge() -> ThreadBridge:
    """Return the process-wide thread router without creating a local core."""
    return _thread_bridge


__all__ = ["ThreadBridge", "ThreadIds", "get_thread_bridge"]
