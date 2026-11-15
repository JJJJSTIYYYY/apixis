"""Immutable event runtime snapshots, independent of runtime components."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EventLoopStatus:
    """Tracked foreground/background work and shared dispatch capacity.

    Task counts include suspended tasks and completed tasks whose cleanup
    callback has not run. Permits in use include permits reserved for waking
    acquirers; suspended chains lend theirs and remain in the task count.
    """

    running: bool
    dispatch_capacity: int | None
    dispatch_permits_in_use: int
    dispatch_tasks: int
    pending_dispatch: bool
    background_tasks: int


@dataclass(frozen=True, slots=True)
class EventThreadBridgeStatus:
    """Owner-loop snapshot of bridge admission and tracked requests."""

    running: bool
    queue_size: int
    queue_capacity: int
    admission_waiters: int
    active_requests: int


@dataclass(frozen=True, slots=True)
class EventRuntimeStatus:
    """Read-only snapshot; absent components are None before initialization."""

    initialized: bool = False
    running: bool = False
    event_queue_size: int = 0
    event_queue_capacity: int | None = None
    wait_listeners: int = 0
    event_loop: EventLoopStatus | None = None
    thread_bridge: EventThreadBridgeStatus | None = None


__all__ = ["EventLoopStatus", "EventThreadBridgeStatus", "EventRuntimeStatus"]
