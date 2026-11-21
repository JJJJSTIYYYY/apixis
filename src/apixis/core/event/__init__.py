"""Public event types, queues, registries, and subscription helpers."""

from apixis.core.event.base import (
    CoreState,
    EventLoopStatus,
    EventRuntimeStatus,
    EventType,
    ApixEvent,
    ApixEventError,
    EventPredicate,
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
from apixis.core.utils.exception import (
    CoreClosedError,
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
    create_core,
    get_core,
    get_event_runtime_status,
    start_core,
    stop_core,
    close_core,
    get_event_registry,
    get_event_pipe,
    get_handler_registry,
    get_event_loop,
    ApixisCore,
)
from apixis.core.event.subscription import (
    get_unmatched_subscriptions,
    post_event,
    post_and_wait,
    subscribe,
    unsubscribe,
    get_handler,
    get_handler_meta,
    is_registered,
    expect_event,
    wait_event,
    await_for,
    wait_for_event,
    EventWaiter,
)
from apixis.core.event.event_registry import (
    ApixEventRegistry,
)

__all__ = [
    "CoreState",
    "CoreClosedError",
    "create_core",
    "get_core",
    "get_event_runtime_status",
    "EventRuntimeStatus",
    "EventLoopStatus",
    "start_core",
    "stop_core",
    "close_core",
    "get_event_registry",
    "get_event_pipe",
    "get_handler_registry",
    "get_event_loop",
    "ApixisCore",
    "EventType",
    "ApixEvent",
    "ApixEventError",
    "EventPredicate",
    "EventHandlerFunc",
    "EventHandlerErrorFunc",
    "ApixEventHandler",
    "ApixEventLoop",
    "ApixEventPipe",
    "EventHandlerNotRegisteredError",
    "EventHandlerAlreadyRegisteredError",
    "GraphNodeError",
    "InvalidNodeReturnsError",
    "InvalidContextError",
    "ApixHandlerRegistry",
    "get_unmatched_subscriptions",
    "post_event",
    "post_and_wait",
    "subscribe",
    "unsubscribe",
    "get_handler",
    "get_handler_meta",
    "is_registered",
    "expect_event",
    "wait_event",
    "await_for",
    "wait_for_event",
    "EventWaiter",
    "ApixEventRegistry",
]
