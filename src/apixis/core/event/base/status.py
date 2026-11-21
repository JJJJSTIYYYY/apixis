"""Immutable event runtime snapshots, independent of runtime components."""

from dataclasses import dataclass
from enum import Enum


class CoreState(str, Enum):
    """Explicit construction, startup, consumption, stop and close states.

    STARTING creates the consumer task. STARTED records task creation while
    startup scheduling is unconfirmed; RUNNING follows its first scheduling
    turn. STOPPING waits for consumption to stop, without stopping handlers.
    CLOSING and CLOSED permanently reject new event work.
    """

    CREATED = "created"
    STARTING = "starting"
    STARTED = "started"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CLOSING = "closing"
    CLOSED = "closed"


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
class EventRuntimeStatus:
    """Read-only snapshot; absent components are None before initialization."""

    initialized: bool = False
    state: CoreState | None = None
    running: bool = False
    event_queue_size: int = 0
    event_queue_capacity: int | None = None
    wait_listeners: int = 0
    event_loop: EventLoopStatus | None = None


__all__ = ["CoreState", "EventLoopStatus", "EventRuntimeStatus"]
