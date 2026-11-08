"""Construct, start, and stop the shared event core with explicit dependencies.

Main-thread getters construct components on first access and schedule shared
startup inside asyncio. Await start_core() when startup completion or errors
must be observed directly. Construction itself never starts background consumers.
"""

import asyncio
from dataclasses import dataclass, field
from threading import current_thread, main_thread

from apixis.core.config.core_config import HANDLER_CHAIN_CACHE_SIZE
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.event.thread_bridge import EventThreadBridge
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
    thread_bridge: EventThreadBridge | None = field(default=None, init=False)

    @property
    def started(self) -> bool:
        if not self.event_pipe or not self.event_loop:
            return False
        if self.event_pipe.is_running and self.event_loop._started:
            return True
        return False


_core: EventCore | None = None


def _get_core() -> EventCore:
    """Build synchronously so asyncio callers cannot observe a partial core."""
    global _core
    if current_thread() is not main_thread():
        raise RuntimeError("The event core belongs to the main thread; use the threadsafe event APIs.")
    if _core is None:
        event_registry = ApixEventRegistry()
        event_pipe = ApixEventPipe()
        handler_registry = ApixHandlerRegistry(
            event_registry, cache_size=HANDLER_CHAIN_CACHE_SIZE,
        )
        event_loop = ApixEventLoop(handler_registry, event_pipe, event_registry)
        _core = EventCore(event_registry, event_pipe, handler_registry, event_loop)
    return _core


async def start_core() -> None:
    """Signal startup on the main asyncio loop and initialize thread requests.

    A restart that has already signalled started may still have a startup task
    in progress. Initial successful startup binds the thread bridge.

    Concurrent calls share the same startup lock. Failed or cancelled startup
    propagates to the caller; a later call retries using the same components.
    """
    await _start_core(_get_core())


async def _start_core(core: EventCore) -> None:
    loop = asyncio.get_running_loop()
    if core.thread_bridge is not None and core.thread_bridge.loop is not loop:
        raise RuntimeError("The event core is bound to a different asyncio loop.")
    if core.started and (core.thread_bridge is None or not core.thread_bridge.closed):
        return
    async with core.start_lock:
        await core.event_pipe.start()
        await core.event_loop.start()
        if core.thread_bridge is None or core.thread_bridge.closed:
            core.thread_bridge = EventThreadBridge(loop, maxsize=core.event_pipe.maxsize)


async def stop_core() -> None:
    """Pause the shared core without constructing it if it does not exist.

    Cancel automatic startup, then stop the consumer and pipe under the startup
    lock. Preserve components, subscriptions, and queued events for restart.
    Existing dispatch and background tasks continue without being awaited.
    A later start_core() or getter can restart the same core.
    """
    core = _core
    if core is None:
        return
    if current_thread() is not main_thread():
        raise RuntimeError("The event core must stop on the main thread.")
    if core.thread_bridge is not None and core.thread_bridge.loop is not asyncio.get_running_loop():
        raise RuntimeError("The event core is bound to a different asyncio loop.")
    if core.start_task is not None and not core.start_task.done():
        core.start_task.cancel()
        await asyncio.gather(core.start_task, return_exceptions=True)
    async with core.start_lock:
        try:
            if core.thread_bridge is not None:
                await core.thread_bridge.close()
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
    if core.thread_bridge is not None and core.thread_bridge.loop is not loop:
        raise RuntimeError("The event core is bound to a different asyncio loop.")
    if core.started or (core.start_task is not None and not core.start_task.done()):
        return
    core.start_task = loop.create_task(_start_core(core), name="event-core-start")
    core.start_task.add_done_callback(_on_start_done)


def get_thread_bridge() -> EventThreadBridge:
    """Return the bridge already bound by main-thread core startup."""
    core = _core
    if core is None or core.thread_bridge is None:
        raise RuntimeError("Await start_core() on the main thread before using threadsafe event APIs.")
    return core.thread_bridge


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
    "start_core", "stop_core", "get_event_registry", "get_event_pipe", "get_handler_registry",
    "aget_event_registry", "aget_event_pipe", "aget_handler_registry",
    "get_event_loop", "aget_event_loop",
]
