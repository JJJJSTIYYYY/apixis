"""Application event contracts, lifecycle, subscriptions and waiting."""

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
)

from apixis.core.utils.exception import (
    CoreClosedError,
    EventHandlerNotRegisteredError,
    EventHandlerAlreadyRegisteredError,
)

from apixis.core.event.factory import (
    create_core,
    get_core,
    get_event_runtime_status,
    start_core,
    stop_core,
    close_core,
)

from apixis.core.event.subscription import (
    get_unmatched_subscriptions,
    post_event,
    post_and_wait,
    subscribe,
    unsubscribe,
    get_handler_meta,
    is_registered,
    expect_event,
    wait_event,
    await_for,
    wait_for_event,
    EventWaiter,
)

__all__ = [
    'CoreState',
    'CoreClosedError',
    'create_core',
    'get_core',
    'get_event_runtime_status',
    'EventRuntimeStatus',
    'EventLoopStatus',
    'start_core',
    'stop_core',
    'close_core',
    'EventType',
    'ApixEvent',
    'ApixEventError',
    'EventPredicate',
    'EventHandlerFunc',
    'EventHandlerErrorFunc',
    'EventHandlerNotRegisteredError',
    'EventHandlerAlreadyRegisteredError',
    'get_unmatched_subscriptions',
    'post_event',
    'post_and_wait',
    'subscribe',
    'unsubscribe',
    'get_handler_meta',
    'is_registered',
    'expect_event',
    'wait_event',
    'await_for',
    'wait_for_event',
    'EventWaiter',
]
