"""Public event types, channels, registries, and subscription helpers."""

from apixis.core.event.base import (
    EventType,
    ApixEvent,
    ApixEventError,
    EventHandlerFunc,
    EventHandlerErrorFunc,
    ApixEventHandler,
)
from apixis.core.event.event_loop import (
    ApixEventLoop,
)
from apixis.core.event.event_pipe import (
    ApixEventPipe,
)
from apixis.core.event.pipe_channel import (
    BuiltinChannel,
)
from apixis.core.utils.exception import (
    EventHandlerNotRegisteredError,
    EventHandlerAlreadyRegisteredError,
    GraphNodeError,
    InvalidNodeReturnsError,
    InvalidContextError,
)
from apixis.core.event.handler_registry import (
    ApixHandlerRegistry,
)
from apixis.core.event.factory import (
    start_core,
    get_event_registry,
    get_event_pipe,
    get_handler_registry,
    get_event_loop,
    aget_event_registry,
    aget_event_pipe,
    aget_handler_registry,
    aget_event_loop,
    EventCore,
)
from apixis.core.event.subscription import (
    get_unmatched_subscriptions,
    subscribe,
    unsubscribe,
    get_handler,
    get_handler_meta,
    is_registered,
    await_for,
    wait_for_event,
    EventWaiter,
)
from apixis.core.event.event_registry import (
    ApixEventRegistry,
)

__all__ = [
    "start_core",
    "get_event_registry",
    "get_event_pipe",
    "get_handler_registry",
    "get_event_loop",
    "aget_event_registry",
    "aget_event_pipe",
    "aget_handler_registry",
    "aget_event_loop",
    "EventCore",
    "EventType",
    "ApixEvent",
    "ApixEventError",
    "EventHandlerFunc",
    "EventHandlerErrorFunc",
    "ApixEventHandler",
    "ApixEventLoop",
    "ApixEventPipe",
    "BuiltinChannel",
    "EventHandlerNotRegisteredError",
    "EventHandlerAlreadyRegisteredError",
    "GraphNodeError",
    "InvalidNodeReturnsError",
    "InvalidContextError",
    "ApixHandlerRegistry",
    "get_unmatched_subscriptions",
    "subscribe",
    "unsubscribe",
    "get_handler",
    "get_handler_meta",
    "is_registered",
    "await_for",
    "wait_for_event",
    "EventWaiter",
    "ApixEventRegistry",
]
