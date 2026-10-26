"""Construct and start the shared event core with explicit dependencies.

Getters construct components on first access and schedule shared startup when
called inside asyncio. Await start_core() when startup completion or errors
must be observed directly. Construction itself never starts background consumers.
"""

import asyncio
from dataclasses import dataclass, field

from apixis.core.config.core_config import HANDLER_CHAIN_CACHE_SIZE
from apixis.core.event.event_loop import ApixEventLoop
from apixis.core.event.event_pipe import ApixEventPipe
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry import ApixHandlerRegistry
from apixis.core.utils.logger import logger


@dataclass
class EventCore:
    """Keep one complete dependency graph and serialize asynchronous startup."""

    event_registry: ApixEventRegistry
    event_pipe: ApixEventPipe
    handler_registry: ApixHandlerRegistry
    event_loop: ApixEventLoop
    start_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    start_task: asyncio.Task[None] | None = field(default=None, init=False)

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
    """Construct once but not ensure both services have started before returning.
    This method only publishes the start signal. The upper-layer interface is
    unaffected by whether the core's consumers have started.

    Concurrent calls share the same startup lock. Failed or cancelled startup
    propagates to the caller; a later call retries using the same components.
    """
    await _start_core(_get_core())


async def _start_core(core: EventCore) -> None:
    if core.started:
        return
    async with core.start_lock:
        await core.event_pipe.start()
        await core.event_loop.start()


def _ensure_started(core: EventCore) -> None:
    """Schedule one startup attempt without changing synchronous getter APIs."""
    if core.started or (core.start_task is not None and not core.start_task.done()):
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Imports and synchronous registration may construct the core before
        # asyncio begins. The next getter inside asyncio schedules startup.
        return
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
    "start_core", "get_event_registry", "get_event_pipe", "get_handler_registry",
    "aget_event_registry", "aget_event_pipe", "aget_handler_registry",
    "get_event_loop", "aget_event_loop",
]
