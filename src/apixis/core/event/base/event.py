"""Event values and callback type contracts, independent of runtime state."""

from contextvars import Context
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Literal


class EventType(str, Enum):
    TRANSIENT = 'transient' # Transient events are dispatched without recording their names.
    WORKFLOW = 'workflow'
    LIFECYCLE = 'lifecycle'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'


@dataclass(frozen=True, slots=True)
class ApixEventError:
    """Serializable failure details without references to live traceback frames."""

    handler_name: str
    phase: Literal["core_func", "on_has_error", "on_accepted", "on_error"]
    exception_type: str
    message: str
    traceback: str


@dataclass(slots=True)
class ApixEvent:
    event_id: str
    event_type: EventType
    event_name: str
    context: Any
    timestamp: float
    accepted: bool = False
    seen: list[str] = field(default_factory=list) # Handler names recorded immediately before core execution.
    error_stack: list[ApixEventError] = field(default_factory=list)
    _execution_context: Context | None = field(
        default=None, init=False, repr=False, compare=False,
    )

    def accept(self) -> None:
        '''
        Mark this event item as accepted.

        Once accepted, subsequent handlers skip their core functions but
        still receive applicable error and acceptance notifications.
        '''
        self.accepted = True

    @property
    def has_error(self) -> bool:
        """Return whether a foreground handler has recorded a failure."""
        return bool(self.error_stack)

    @property
    def datetime(self) -> datetime:
        '''
        Convert timestamp to datetime object.
        '''
        return datetime.fromtimestamp(self.timestamp)


EventHandlerFunc = Callable[[ApixEvent], Awaitable[None]]


EventHandlerErrorFunc = Callable[[ApixEvent, Exception], Awaitable[None]]


__all__ = [
    "EventType",
    "ApixEventError",
    "ApixEvent",
    "EventHandlerFunc",
    "EventHandlerErrorFunc",
]
