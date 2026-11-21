"""Case-sensitive event matching shared by ordinary and temporary listeners."""

from fnmatch import fnmatchcase

from apixis.core.event.base.handler import ApixEventHandler


def is_glob_pattern(pattern: str) -> bool:
    return "*" in pattern or "?" in pattern or "[" in pattern


def matches_handler(handler: ApixEventHandler, event_name: str) -> bool:
    """Return whether a handler accepts one exact, case-sensitive event."""
    subscribed = any(
        fnmatchcase(event_name, pattern)
        for pattern in handler._subscribe
    )
    return subscribed and not any(
        fnmatchcase(event_name, pattern)
        for pattern in handler._filter_event
    )


__all__ = [
    "is_glob_pattern",
    "matches_handler",
]
