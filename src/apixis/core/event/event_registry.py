"""Process-local registry of exact event names observed at runtime."""

from threading import RLock

from apixis.core.event.base import ApixEvent, EventType


class ApixEventRegistry:
    """Store exact event names observed by the local event runtime.

    The registry is observational only: it does not own queued events or
    control handler dispatch. Event names are stored instead of
    :class:`ApixEvent` instances because event objects are mutable and
    unhashable. Exact names can be used for diagnostics and subscription
    analysis.

    Each instance owns its observations. All reads and writes are protected
    by a reentrant lock so event observation and handler registration may query
    the registry safely from different threads.
    """

    def __init__(self) -> None:
        self._registered_events: set[str] = set()
        self._lock = RLock()

    def record_event(self, event: ApixEvent) -> None:
        """Record the exact name of one observed event.

        The event loop records events after local dequeue. Repeated
        observations of the same event name have no additional effect.

        Args:
            event: Observed event whose exact name should be recorded.

        Raises:
            TypeError: If ``event`` is not an :class:`ApixEvent`.
            ValueError: If the event name is empty.
        """
        if not isinstance(event, ApixEvent):
            raise TypeError("event must be an ApixEvent instance.")
        if event.event_type == EventType.TRANSIENT:
            # Transient event names are not retained in the registry.
            return
        if not event.event_name:
            raise ValueError("event.event_name must be a non-empty string.")

        with self._lock:
            self._registered_events.add(event.event_name)

    def get_registered_events(self) -> frozenset[str]:
        """Return an immutable snapshot of exact event names seen at runtime."""
        with self._lock:
            return frozenset(self._registered_events)

    def clear(self) -> None:
        """Forget every observed event name without affecting queued events."""
        with self._lock:
            self._registered_events.clear()


__all__ = ["ApixEventRegistry"]
