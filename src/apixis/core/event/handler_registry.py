"""Current handler registry with lazily resolved event-specific chains."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from fnmatch import fnmatchcase

from apixis.core.utils import logger
from apixis.core.event.base import ApixEventHandler
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.utils.validation import validate_int, validate_number
from apixis.core.utils.exception import (
    EventHandlerAlreadyRegisteredError,
    EventHandlerNotRegisteredError,
)


class ApixHandlerRegistry:
    """Index current subscriptions and cache bounded event-specific chains.

    ``priority_buckets`` determines dispatch order. Missing cache keys and
    ``None`` require rebuilding; an empty list is a valid cached result.
    Registration changes invalidate affected entries without modifying lists
    already held by dispatch tasks. The consumer captures handler references
    in this order at dequeue, before waiting for dispatch capacity.
    ``cache_size`` limits cached names, including empty chains; evicted chains
    are rebuilt from the current registrations on their next lookup.
    """

    registry: dict[str, ApixEventHandler]
    priority_buckets: dict[float, list[str]]
    cached_chain: OrderedDict[str, list[str] | None]

    def __init__(
        self, event_registry: ApixEventRegistry, *, cache_size: int = 8192,
    ) -> None:
        self._event_registry = event_registry
        self._cache_size = validate_int(cache_size, "cache_size", minimum=1)
        self.registry = {}
        self.priority_buckets = {}
        self.cached_chain = OrderedDict()
        self._exact_subscriptions: dict[str, set[str]] = {}
        self._handler_exact_names: dict[str, tuple[str, ...]] = {}
        self._glob_subscriptions: dict[str, tuple[str, ...]] = {}
        # Sort keys use the actual bucket priority, including relative insertions
        # whose public handler.priority is None. Removal leaves ordinals intact.
        self._handler_order: dict[str, tuple[float, int]] = {}
        self._bucket_next_order: dict[float, int] = {}
        self._register_order = 0

    def clear(self) -> None:
        """Remove registrations and cached chains without changing observations.

        Already captured handler references and chain lists remain usable.
        """
        self.registry.clear()
        self.priority_buckets.clear()
        self.cached_chain.clear()
        self._exact_subscriptions.clear()
        self._handler_exact_names.clear()
        self._glob_subscriptions.clear()
        self._handler_order.clear()
        self._bucket_next_order.clear()
        self._register_order = 0

    def __contains__(self, item):
        return item in self.registry

    @staticmethod
    def _normalise_patterns(
        patterns: Iterable[str],
        *,
        argument_name: str | None = None,
    ) -> list[str]:
        """Validate patterns and remove duplicates without changing order."""
        if isinstance(patterns, str):
            patterns = (patterns,)

        supplied = list(patterns)
        if not supplied:
            raise ValueError(f"{argument_name or 'Patterns'} cannot be empty.")
        if any(
            not isinstance(pattern, str) or not pattern
            for pattern in supplied
        ):
            raise ValueError(
                f"Every pattern in {argument_name or 'patterns'} must be a non-empty string."
            )
        return list(dict.fromkeys(supplied))

    @staticmethod
    def _is_glob(pattern: str) -> bool:
        return any(character in pattern for character in "*?[")

    @staticmethod
    def _matches_handler(handler: ApixEventHandler, event_name: str) -> bool:
        """Return whether a handler accepts one exact, case-sensitive event."""
        subscribed = any(
            fnmatchcase(event_name, pattern)
            for pattern in handler.subscribe
        )
        return subscribed and not any(
            fnmatchcase(event_name, pattern)
            for pattern in handler.filter_event
        )

    def _find_handler_position(self, handler_name: str) -> tuple[float, int] | None:
        """Return the active bucket and index for a handler name."""
        order = self._handler_order.get(handler_name)
        if order is None:
            return None
        priority = -order[0]
        return priority, self.priority_buckets[priority].index(handler_name)

    def _validate_between_handlers(
        self,
        between_handlers: tuple[str | None, str | None],
        *,
        handler_name: str,
    ) -> float:
        """Validate boundaries without mutation and return the target priority."""
        if (
            not isinstance(between_handlers, tuple)
            or len(between_handlers) != 2
        ):
            raise ValueError(
                "between_handlers must be a tuple containing exactly two "
                "handler names."
            )
        left_name, right_name = between_handlers
        if left_name is None and right_name is None:
            raise ValueError("between_handlers cannot be (None, None).")
        if any(
            name is not None and (not isinstance(name, str) or not name)
            for name in between_handlers
        ):
            raise ValueError(
                "Boundary handler names must be non-empty strings or None."
            )
        if left_name is not None and left_name == right_name:
            raise ValueError(
                "The left and right handlers in between_handlers cannot "
                "be the same handler."
            )
        if handler_name in between_handlers:
            raise ValueError("A handler cannot use itself as an insertion boundary.")
        left_position = (
            self._find_handler_position(left_name)
            if left_name is not None
            else None
        )
        right_position = (
            self._find_handler_position(right_name)
            if right_name is not None
            else None
        )

        if left_name is not None and left_position is None:
            raise EventHandlerNotRegisteredError(
                f"Handler `{left_name}` is not actively registered."
            )
        if right_name is not None and right_position is None:
            raise EventHandlerNotRegisteredError(
                f"Handler `{right_name}` is not actively registered."
            )

        if left_position is not None and right_position is not None:
            left_priority, left_index = left_position
            right_priority, right_index = right_position
            if left_priority < right_priority or (
                left_priority == right_priority and left_index >= right_index
            ):
                raise ValueError(
                    f"Handler `{left_name}` must be before handler "
                    f"`{right_name}`."
                )

            # The right boundary determines the target priority bucket.
            return right_priority

        if left_position is not None:
            return left_position[0]

        return right_position[0]

    def _invalidate_matching_chains(self, handler: ApixEventHandler) -> None:
        """Expire existing caches accepted by this handler without rebuilding."""
        # Exact subscriptions identify their own cache keys. Temporary reply
        # listeners must not scan unrelated historical event names.
        event_names = (
            self.cached_chain
            if any(self._is_glob(pattern) for pattern in handler.subscribe)
            else (name for name in handler.subscribe if name in self.cached_chain)
        )
        need_delete = []
        for event_name in event_names:
            if self._matches_handler(handler, event_name):
                need_delete.append(event_name)
        for event_name in need_delete:
            self.cached_chain.pop(event_name, None)

    def _resolve_handler_chain(self, event_name: str) -> list[str]:
        """Match exact candidates and glob subscriptions, then apply ordering."""
        candidates = set(self._exact_subscriptions.get(event_name, ()))
        for name, patterns in self._glob_subscriptions.items():
            if name not in candidates and any(
                fnmatchcase(event_name, pattern) for pattern in patterns
            ):
                candidates.add(name)
        chain = [
            name for name in candidates
            if not any(
                fnmatchcase(event_name, pattern)
                for pattern in self.registry[name].filter_event
            )
        ]
        chain.sort(key=self._handler_order.__getitem__)
        return chain

    def get_handlers_chain_for_event(self, event_name: str) -> list[str]:
        """Return the current chain, rebuilding only absent or expired entries."""
        if not isinstance(event_name, str) or not event_name:
            raise ValueError("event_name must be a non-empty string.")

        chain = self.cached_chain.get(event_name)
        if chain is None:
            chain = self._resolve_handler_chain(event_name)
            self.cached_chain[event_name] = chain
            if len(self.cached_chain) > self._cache_size:
                self.cached_chain.popitem(last=False)
        self.cached_chain.move_to_end(event_name)
        return chain

    def get_handlers_for_event(self, event_name: str) -> list[ApixEventHandler]:
        """Capture the current matching handlers in dispatch order."""
        return [
            self.registry[name]
            for name in self.get_handlers_chain_for_event(event_name)
        ]

    def _remove_from_bucket(self, handler_name: str) -> None:
        """Remove a registered name and discard its bucket when empty."""
        priority = -self._handler_order.pop(handler_name)[0]
        bucket = self.priority_buckets[priority]
        if bucket[-1] == handler_name:
            bucket.pop()
        else:
            bucket.remove(handler_name)
        if not bucket:
            del self.priority_buckets[priority]
            del self._bucket_next_order[priority]

    def _index_subscriptions(self, handler: ApixEventHandler) -> None:
        exact_names = []
        glob_patterns = []
        for pattern in handler.subscribe:
            if self._is_glob(pattern):
                glob_patterns.append(pattern)
            else:
                exact_names.append(pattern)
                self._exact_subscriptions.setdefault(pattern, set()).add(handler.name)
        self._handler_exact_names[handler.name] = tuple(exact_names)
        if glob_patterns:
            self._glob_subscriptions[handler.name] = tuple(glob_patterns)

    def _unindex_subscriptions(self, handler_name: str) -> None:
        for event_name in self._handler_exact_names.pop(handler_name):
            names = self._exact_subscriptions[event_name]
            names.remove(handler_name)
            if not names:
                del self._exact_subscriptions[event_name]
        self._glob_subscriptions.pop(handler_name, None)

    def register_handler(
        self, handler_entry: ApixEventHandler, *, exist_ok: bool = False,
    ) -> None:
        """Validate, then insert or replace an entry and expire affected caches."""
        if not isinstance(handler_entry, ApixEventHandler):
            raise TypeError("handler_entry must be an ApixEventHandler instance.")
        if not handler_entry.name:
            raise ValueError("Handler name cannot be empty.")
        if handler_entry.name in self.registry and not exist_ok:
            raise EventHandlerAlreadyRegisteredError(
                f"Handler `{handler_entry.name}` already registered."
            )
        if not callable(handler_entry.core_func):
            raise TypeError("Handler core_func must be callable.")
        if handler_entry.on_accepted is not None and not callable(handler_entry.on_accepted):
            raise TypeError("Handler on_accepted must be callable.")
        if handler_entry.on_has_error is not None and not callable(handler_entry.on_has_error):
            raise TypeError("Handler on_has_error must be callable.")
        if handler_entry.on_error is not None and not callable(handler_entry.on_error):
            raise TypeError("Handler on_error must be callable.")
        if handler_entry.on_cancelled is not None and not callable(handler_entry.on_cancelled):
            raise TypeError("Handler on_cancelled must be callable.")
        if handler_entry.priority is not None and handler_entry.between_handlers is not None:
            raise ValueError(
                "between_handlers and priority cannot be set together."
            )

        subscriptions = self._normalise_patterns(
            handler_entry.subscribe,
            argument_name="subscribe",
        )
        filters = (
            self._normalise_patterns(handler_entry.filter_event, argument_name="filter_event",)
            if handler_entry.filter_event else []
        )

        between_handlers = handler_entry.between_handlers
        if between_handlers is not None:
            bucket_priority = self._validate_between_handlers(
                between_handlers,
                handler_name=handler_entry.name
            )
        else:
            priority = handler_entry.priority
            bucket_priority = validate_number(priority, "Handler priority")

        # Validate first so failed replacements preserve the old registration.
        # Reuse normal unregistration, then insert against the updated bucket.
        self.unregister_handler(handler_entry.name, missing_ok=True)

        handler_entry.subscribe = subscriptions
        handler_entry.filter_event = filters
        handler_entry._register_order = self._register_order
        self.registry[handler_entry.name] = handler_entry
        bucket = self.priority_buckets.setdefault(bucket_priority, [])
        if between_handlers is None:
            bucket.append(handler_entry.name)
            ordinal = self._bucket_next_order.get(bucket_priority, 0)
            self._handler_order[handler_entry.name] = -bucket_priority, ordinal
            self._bucket_next_order[bucket_priority] = ordinal + 1
        else:
            left_name, right_name = between_handlers
            if right_name is not None:
                insert_index = bucket.index(right_name)
            else:
                insert_index = bucket.index(left_name) + 1
            bucket.insert(insert_index, handler_entry.name)
            # Relative insertion is uncommon and already shifts this bucket's
            # list. Reindex only this bucket; ordinary append/removal stays local.
            for ordinal, name in enumerate(bucket):
                self._handler_order[name] = -bucket_priority, ordinal
            self._bucket_next_order[bucket_priority] = len(bucket)
        self._index_subscriptions(handler_entry)
        self._invalidate_matching_chains(handler_entry)
        self._register_order += 1

        logger.debug(
            f"Registered handler `{handler_entry.name}`, "
            f"priority={handler_entry.priority}, "
            f"between_handlers={handler_entry.between_handlers}"
        )

    def unregister_handler(self, handler_name: str, *, missing_ok: bool = False) -> None:
        """Immediately remove an entry and its bucket record, expiring caches."""
        handler = self.registry.get(handler_name)
        if handler is None:
            if not missing_ok:
                raise EventHandlerNotRegisteredError(
                    f"Handler `{handler_name}` not registered."
                )
            return
        self._invalidate_matching_chains(handler)
        self._unindex_subscriptions(handler_name)
        self._remove_from_bucket(handler_name)
        del self.registry[handler_name]
        logger.debug(f"Unregistered handler {handler_name}.")

    def get_handler(self, handler_name: str) -> ApixEventHandler | None:
        """Return a handler entry by name, or ``None`` when it is unknown."""
        return self.registry.get(handler_name)

    def get_unmatched_subscriptions(self, handler_name: str) -> list[str]:
        """Return subscription patterns that matched no observed event name."""
        handler = self.registry.get(handler_name)
        if handler is None:
            raise EventHandlerNotRegisteredError(
                f"Handler `{handler_name}` not registered."
            )

        event_names = self._event_registry.get_registered_events()
        return [
            subscription
            for subscription in handler.subscribe
            if not any(
                fnmatchcase(event_name, subscription)
                and not any(
                    fnmatchcase(event_name, pattern)
                    for pattern in handler.filter_event
                )
                for event_name in event_names
            )
        ]


__all__ = ["ApixHandlerRegistry"]
