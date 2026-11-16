"""Thread-local event cores and lifecycle; cross-thread routing lives in ThreadBridge."""

import asyncio
from dataclasses import dataclass, field
from threading import current_thread, local, main_thread

from apixis.core.config.core_config import HANDLER_CHAIN_CACHE_SIZE
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event.thread_bridge import ThreadEndpoint, get_thread_bridge
from apixis.core.utils.validation import validate_non_empty_string
from apixis.core.event.base.status import EventRuntimeStatus
from apixis.core.utils import logger


@dataclass
class EventCore:
    """Keep one complete dependency graph and serialize asynchronous startup."""

    event_registry: ApixEventRegistry
    event_pipe: ApixEventPipe
    handler_registry: ApixHandlerRegistry
    event_loop: ApixEventLoop
    start_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    start_task: asyncio.Task[None] | None = field(default=None, init=False)
    thread_id: str = "main"
    loop: asyncio.AbstractEventLoop | None = field(default=None, init=False)
    endpoint: ThreadEndpoint | None = field(default=None, init=False)

    @property
    def started(self) -> bool:
        return self.event_pipe.is_running and self.event_loop._started


class _CoreLocal(local):
    core: EventCore | None = None


_local = _CoreLocal()


def _check_loop(core: EventCore) -> None:
    """Reject access from a different loop on the same OS thread."""
    if core.loop is not None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if core.loop is not loop:
            raise RuntimeError("The thread core is bound to a different asyncio loop.")


def create_core(thread_id: str) -> EventCore:
    """Construct and register the calling thread's event core under a unique ID.

    Args:
        thread_id: Non-empty process-wide routing name, for example ``"worker"``.
            Each OS thread owns one core. Repeating the same ID on its owner
            returns that core; changing an existing core's ID raises ValueError.

    Returns:
        The thread-local core without starting consumers or creating an asyncio
        loop. Call ``await start_core()`` inside this thread's loop to bind it
        and make it available to ``*_threadsafe(thread_ids=...)`` requests.

    Raises:
        ValueError: If the ID is empty, belongs to another live thread, or the
            caller already owns a differently named core.
        RuntimeError: If the caller uses a different loop from the bound one.

    Main-thread getters lazily create ``"main"`` for import-time decorators.
    Worker threads must call create_core explicitly before using local getters.
    Stop the core before its thread exits. A stopped core retains subscriptions
    and can restart on the same loop; its routing name is released on stop.
    """
    validate_non_empty_string(thread_id, "thread_id")
    core = _local.core
    if core is not None:
        _check_loop(core)
        if core.thread_id != thread_id:
            raise ValueError(f"This thread already owns core {core.thread_id!r}.")
        get_thread_bridge().register(thread_id)
        return core
    event_registry = ApixEventRegistry()
    event_pipe = ApixEventPipe()
    handler_registry = ApixHandlerRegistry(event_registry, cache_size=HANDLER_CHAIN_CACHE_SIZE)
    event_loop = ApixEventLoop(handler_registry, event_pipe, event_registry)
    core = EventCore(event_registry, event_pipe, handler_registry, event_loop, thread_id=thread_id)
    get_thread_bridge().register(thread_id)
    _local.core = core
    return core


def _get_core() -> EventCore:
    """Read only this thread's core; never use a copied ContextVar for ownership."""
    core = _local.core
    if core is None:
        if current_thread() is not main_thread():
            raise RuntimeError("Call create_core(thread_id) in this thread before using local event APIs.")
        core = create_core("main")
    return core


def get_event_runtime_status() -> EventRuntimeStatus:
    """Snapshot the current thread without constructing or starting a core.

    Other threads' tasks, queues and permits are never inspected. Use an
    explicit bridge operation when a remote snapshot is needed.
    """
    core = _local.core
    if core is None:
        return EventRuntimeStatus()
    _check_loop(core)
    return EventRuntimeStatus(
        initialized=True,
        running=core.started,
        event_queue_size=core.event_pipe.qsize(),
        event_queue_capacity=core.event_pipe.maxsize,
        wait_listeners=core.handler_registry.waiter_count,
        event_loop=core.event_loop.get_status(),
        thread_bridge=None if core.endpoint is None else core.endpoint.get_status(),
    )


async def start_core() -> None:
    """Start the current thread's core on its running asyncio loop.

    Concurrent calls share its startup lock. Registration through create_core
    must precede worker startup. A successful start exposes this core to the
    process-wide bridge. Failed or cancelled startup can be retried.
    """
    await _start_core(_get_core())


async def _start_core(core: EventCore) -> None:
    loop = asyncio.get_running_loop()
    if core.loop is not None and core.loop is not loop:
        raise RuntimeError("The thread core is bound to a different asyncio loop.")
    if core.started and core.endpoint is not None and not core.endpoint.closed:
        return
    async with core.start_lock:
        get_thread_bridge().register(core.thread_id)
        core.loop = loop
        await core.event_pipe.start()
        await core.event_loop.start()
        if core.endpoint is None or core.endpoint.closed:
            endpoint = ThreadEndpoint(loop, maxsize=core.event_pipe.maxsize)
            get_thread_bridge().register(core.thread_id, endpoint)
            core.endpoint = endpoint


async def stop_core() -> None:
    """Pause the current thread core without constructing it if it does not exist.

    Cancel automatic startup, then stop the consumer and pipe under the startup
    lock. Preserve components, subscriptions, and queued events for restart.
    Existing dispatch and background tasks continue without being awaited.
    A later start_core() or getter can restart the same core.
    """
    core = _local.core
    if core is None:
        return
    _check_loop(core)
    if core.start_task is not None and not core.start_task.done():
        core.start_task.cancel()
        await asyncio.gather(core.start_task, return_exceptions=True)
    async with core.start_lock:
        try:
            if core.endpoint is not None:
                await core.endpoint.close()
            get_thread_bridge().unregister(core.thread_id)
            await core.event_loop.stop()
        finally:
            await core.event_pipe.stop()


def _ensure_started(core: EventCore) -> None:
    """Schedule one startup attempt without changing synchronous getter APIs."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Imports and synchronous registration may construct the core before
        # asyncio begins. The next getter inside asyncio schedules startup.
        return
    if core.loop is not None and core.loop is not loop:
        raise RuntimeError("The thread core is bound to a different asyncio loop.")
    if core.started or (core.start_task is not None and not core.start_task.done()):
        return
    core.loop = loop
    core.start_task = loop.create_task(_start_core(core), name="event-core-start")
    core.start_task.add_done_callback(_on_start_done)


def _on_start_done(task: asyncio.Task[None]) -> None:
    """Report automatic startup failures; subsequent getters may retry."""
    if not task.cancelled():
        error = task.exception()
        if error is not None:
            logger.error(f"Event core startup failed: {type(error).__name__}: {error}")


def get_event_registry() -> ApixEventRegistry:
    """Return the event-name registry and schedule startup inside asyncio."""
    core = _get_core()
    _ensure_started(core)
    return core.event_registry


def get_event_pipe() -> ApixEventPipe:
    """Return the event pipe and schedule startup inside asyncio."""
    core = _get_core()
    _ensure_started(core)
    return core.event_pipe


def get_handler_registry() -> ApixHandlerRegistry:
    """Return the event handler registry and schedule startup inside asyncio."""
    core = _get_core()
    _ensure_started(core)
    return core.handler_registry


def get_event_loop() -> ApixEventLoop:
    """Return the event dispatcher loop and schedule startup inside asyncio."""
    core = _get_core()
    _ensure_started(core)
    return core.event_loop


async def aget_event_registry() -> ApixEventRegistry:
    """Return the event-name registry"""
    core = _get_core()
    await _start_core(core)
    return core.event_registry


async def aget_event_pipe() -> ApixEventPipe:
    """Return the event pipe"""
    core = _get_core()
    await _start_core(core)
    return core.event_pipe


async def aget_handler_registry() -> ApixHandlerRegistry:
    """Return the event handler registry"""
    core = _get_core()
    await _start_core(core)
    return core.handler_registry


async def aget_event_loop() -> ApixEventLoop:
    """Return the event dispatcher loop"""
    core = _get_core()
    await _start_core(core)
    return core.event_loop


__all__ = [
    "create_core", "get_event_runtime_status",
    "start_core", "stop_core", "get_event_registry", "get_event_pipe", "get_handler_registry",
    "aget_event_registry", "aget_event_pipe", "aget_handler_registry",
    "get_event_loop", "aget_event_loop",
]
