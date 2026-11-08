"""Worker-thread waiting APIs with main-loop registration and cleanup."""

from concurrent.futures import Future
from functools import partial
from threading import current_thread, main_thread
from typing import Literal, Self

from apixis.core.event.base.event import ApixEvent
from apixis.core.event.factory import get_thread_bridge
from apixis.core.event.subscription.waiter import EventWaiter, collect_events
from apixis.core.event.thread_bridge import ThreadSubmission, complete_future
from apixis.core.utils.validation import validate_number


def await_for_threadsafe(
    event_name: str | list[str] | set[str],
    *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    time_out: float | None = None,
) -> Future[ApixEvent | dict[str, ApixEvent]]:
    """Submit event collection to the main thread and return a thread Future.

    Registration happens when the main thread processes the queued request,
    not when this function returns. Use ``wait_for_event_threadsafe`` when
    registration must finish before publishing a request. The timeout begins
    at registration. Cancellation removes temporary listeners on the main
    thread. Results are the original mutable events, not copies.
    A full request queue blocks this call in the worker thread. Async workers
    can instead use ``await await_for(...)`` for asynchronous admission.
    """
    return get_thread_bridge().submit(partial(
        collect_events, event_name, point=point, filter=filter, time_out=time_out,
    ), background=True).result


class ThreadEventWaiter:
    """Synchronous worker-thread context with confirmed main-thread registration.

    Entering blocks until listeners are registered. ``wait`` blocks for the
    result; expiry cancels collection. Exit blocks until listeners have been
    cleaned up. Never enter or wait on the main thread. A context is single-use.
    """

    def __init__(
        self, event_name: str | list[str] | set[str], *,
        point: Literal['received', 'processed'] = 'received',
        filter: list[str] | str | None = None,
        time_out: float | None = None,
    ) -> None:
        self._make_waiter = partial(
            EventWaiter, event_name, point=point, filter=filter, time_out=time_out,
        )
        self._submission: ThreadSubmission[ApixEvent | dict[str, ApixEvent]] | None = None
        self._entered = False

    def __enter__(self) -> Self:
        self._check_thread()
        if self._entered:
            raise RuntimeError("The thread event waiter context has already been entered.")
        self._entered = True
        ready: Future[None] = Future()

        async def collect() -> ApixEvent | dict[str, ApixEvent]:
            async with self._make_waiter() as waiter:
                complete_future(ready)
                return await waiter.get_result()

        self._submission = get_thread_bridge().submit(collect, background=True)

        def on_done(future: Future) -> None:
            if not ready.done():
                error = (
                    RuntimeError("Event collection was cancelled before registration.")
                    if future.cancelled() else future.exception()
                )
                complete_future(ready, error=error)

        self._submission.result.add_done_callback(on_done)
        try:
            ready.result()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def wait(self, time_out: float | None = None) -> ApixEvent | dict[str, ApixEvent]:
        """Block for collection; a timeout closes listeners before raising.

        This optional budget starts at this call and does not extend the
        registration deadline supplied to ``wait_for_event_threadsafe``.
        """
        self._check_thread()
        if self._submission is None:
            raise RuntimeError("Enter the thread event waiter context before waiting.")
        if time_out is not None:
            time_out = validate_number(time_out, "time_out")
        try:
            return self._submission.result.result(timeout=time_out)
        except TimeoutError:
            self.close()
            raise

    def close(self) -> None:
        """Cancel unfinished collection and wait for main-thread cleanup."""
        if self._submission is not None:
            self._check_thread()
            self._submission.result.cancel()
            self._submission.closed.result()

    @staticmethod
    def _check_thread() -> None:
        if current_thread() is main_thread():
            raise RuntimeError("ThreadEventWaiter must be used on a worker thread.")


def wait_for_event_threadsafe(
    event_name: str | list[str] | set[str], *,
    point: Literal['received', 'processed'] = 'received',
    filter: list[str] | str | None = None,
    time_out: float | None = None,
) -> ThreadEventWaiter:
    """Build a worker-thread waiting context; entry confirms registration.

    Publish inside the context, then call ``waiter.wait()``. Replies arriving
    before ``wait`` are retained. Main-thread registration and cleanup reuse
    ``EventWaiter``; subscriptions and handler execution never move threads.
    """
    return ThreadEventWaiter(event_name, point=point, filter=filter, time_out=time_out)


__all__ = [
    "await_for_threadsafe",
    "ThreadEventWaiter",
    "wait_for_event_threadsafe",
]
