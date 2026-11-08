"""Temporary received/processed listener indexes and their bounded cache."""

from collections import OrderedDict
from fnmatch import fnmatchcase
from heapq import merge
from typing import Literal

from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.handler_registry.matching import is_glob_pattern, matches_handler


class WaiterHandlerCache:
    """Index single-pattern listeners independently of ordinary chain caches."""

    def __init__(self, cache_size: int) -> None:
        self._cache_size = cache_size
        self.positions: dict[str, int] = {}
        self._exact: dict[str, dict[str, ApixEventHandler]] = {}
        self._glob: dict[str, ApixEventHandler] = {}
        self._chains: OrderedDict[
            str, tuple[list[ApixEventHandler], list[ApixEventHandler]]
        ] = OrderedDict()

    def __contains__(self, name: str) -> bool:
        return name in self.positions

    def clear(self) -> None:
        self.positions.clear()
        self._exact.clear()
        self._glob.clear()
        self._chains.clear()

    def _invalidate(self, handler: ApixEventHandler, is_glob: bool) -> None:
        if not self._chains:
            return
        pattern = handler.subscribe[0]
        if not is_glob:
            self._chains.pop(pattern, None)
            return
        expired = [
            name for name in self._chains
            if matches_handler(handler, name)
        ]
        for name in expired:
            del self._chains[name]

    def register(
        self, handler: ApixEventHandler, point: Literal['received', 'processed'],
    ) -> None:
        self.positions[handler.name] = 0 if point == 'received' else 1
        pattern = handler.subscribe[0]
        is_glob = is_glob_pattern(pattern)
        if is_glob:
            self._glob[handler.name] = handler
        else:
            self._exact.setdefault(pattern, {})[handler.name] = handler
        self._invalidate(handler, is_glob)

    def unregister(self, handler: ApixEventHandler) -> None:
        del self.positions[handler.name]
        pattern = handler.subscribe[0]
        is_glob = handler.name in self._glob
        if is_glob:
            del self._glob[handler.name]
        else:
            handlers = self._exact[pattern]
            del handlers[handler.name]
            if not handlers:
                del self._exact[pattern]
        if not self.positions:
            # Do not retain empty event names after the last waiter closes.
            self._chains.clear()
        else:
            self._invalidate(handler, is_glob)

    def get(
        self, event_name: str,
    ) -> tuple[list[ApixEventHandler], list[ApixEventHandler]]:
        cached = self._chains.get(event_name)
        if cached is not None:
            self._chains.move_to_end(event_name)
            return cached
        exact = self._exact.get(event_name)
        candidates = exact.values() if exact else ()
        if self._glob:
            # Both indexes retain registration order; merge only matched
            # candidates instead of sorting or scanning unrelated exact waits.
            glob = (
                handler for handler in self._glob.values()
                if fnmatchcase(event_name, handler.subscribe[0])
            )
            candidates = (
                merge(candidates, glob, key=lambda handler: handler._register_order)
                if exact else glob
            )
        cached = ([], [])
        for handler in candidates:
            if not any(fnmatchcase(event_name, pattern) for pattern in handler.filter_event):
                cached[self.positions[handler.name]].append(handler)
        self._chains[event_name] = cached
        if len(self._chains) > self._cache_size:
            self._chains.popitem(last=False)
        return cached


__all__ = [
    "WaiterHandlerCache",
]
