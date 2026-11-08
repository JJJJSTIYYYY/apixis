"""Current handler registry with lazily resolved event-specific chains."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from fnmatch import fnmatchcase
from heapq import merge
from typing import Literal

from apixis.core.utils import logger
from apixis.core.event.base import ApixEventHandler
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.utils.validation import validate_int, validate_non_empty_string, validate_number
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
    Group membership is indexed in both directions for batch removal and
    replacement without scanning unrelated registrations or groups.
    Wait listeners use a separate exact/glob cache and never invalidate the
    ordinary subscription cache or occupy numeric priority buckets.
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
        self._group_handlers: dict[str, set[str]] = {}
        # Snapshot membership so re-registering a mutated instance also removes
        # its previous group entry without scanning unrelated groups.
        self._handler_groups: dict[str, str] = {}
        # Sort keys use the actual bucket priority, including relative insertions
        # whose public handler.priority is None. Removal leaves ordinals intact.
        self._handler_order: dict[str, tuple[float, int]] = {}
        self._bucket_next_order: dict[float, int] = {}
        self._register_order = 0
        self._waiter_cache = _WaiterHandlerCache(self._cache_size)

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
        self._group_handlers.clear()
        self._handler_groups.clear()
        self._handler_order.clear()
        self._bucket_next_order.clear()
        self._register_order = 0
        self._waiter_cache.clear()

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
        return "*" in pattern or "?" in pattern or "[" in pattern

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
        # Exact subscriptions identify their own cache keys.
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

    def get_subscription_chain_for_event(self, event_name: str) -> list[str]:
        """Return the ordinary subscription chain, excluding wait listeners."""
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

    def get_handlers_chain_for_event(self, event_name: str) -> list[str]:
        """Return all matching names in dispatch order, including wait listeners."""
        chain = self.get_subscription_chain_for_event(event_name)
        if not self._waiter_cache.positions:
            return chain
        received, processed = self._waiter_cache.get(event_name)
        if not received and not processed:
            return chain
        return [*(handler.name for handler in received), *chain,
                *(handler.name for handler in processed)]

    def get_handlers_for_event(self, event_name: str) -> list[ApixEventHandler]:
        """Capture received listeners, ordinary handlers, then processed listeners.

        Only the resulting reference list is copied. Its order and membership
        survive listener cleanup and registration changes during dispatch.
        """
        chain = self.get_subscription_chain_for_event(event_name)
        if not self._waiter_cache.positions:
            return [self.registry[name] for name in chain]
        received, processed = self._waiter_cache.get(event_name)
        handlers = received.copy()
        handlers.extend(self.registry[name] for name in chain)
        handlers.extend(processed)
        return handlers

    def register_wait_handler(
        self, handler: ApixEventHandler, *, point: Literal['received', 'processed'],
    ) -> None:
        """Register an internal single-pattern listener outside priority buckets.

        Waiting APIs validate the pattern, filters and point before calling.
        Internal terminal handlers may also use the processed position.
        """
        if handler.name in self.registry:
            raise EventHandlerAlreadyRegisteredError(
                f"Handler `{handler.name}` already registered."
            )
        handler._register_order = self._register_order
        self._register_order += 1
        self.registry[handler.name] = handler
        self._group_handlers.setdefault(handler.group, set()).add(handler.name)
        self._handler_groups[handler.name] = handler.group
        self._waiter_cache.register(handler, point)

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
        validate_non_empty_string(handler_entry.group, "Handler group")
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
        self._group_handlers.setdefault(handler_entry.group, set()).add(handler_entry.name)
        self._handler_groups[handler_entry.name] = handler_entry.group
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
            f"group={handler_entry.group}, "
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
        if handler_name in self._waiter_cache:
            self._waiter_cache.unregister(handler)
        else:
            self._invalidate_matching_chains(handler)
            self._unindex_subscriptions(handler_name)
            self._remove_from_bucket(handler_name)
        group = self._handler_groups.pop(handler_name)
        members = self._group_handlers[group]
        members.remove(handler_name)
        if not members:
            del self._group_handlers[group]
        del self.registry[handler_name]
        logger.debug(f"Unregistered handler {handler_name}.")

    def unregister_group(self, group: str, *, missing_ok: bool = False) -> None:
        """Remove indexed group members, preserving already captured chains."""
        validate_non_empty_string(group, "Handler group")
        members = self._group_handlers.get(group)
        if not members:
            if not missing_ok:
                raise EventHandlerNotRegisteredError(
                    f"Group `{group}` has no registered handlers."
                )
            return
        for handler_name in tuple(members):
            self.unregister_handler(handler_name)

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


class _WaiterHandlerCache:
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
            if ApixHandlerRegistry._matches_handler(handler, name)
        ]
        for name in expired:
            del self._chains[name]

    def register(
        self, handler: ApixEventHandler, point: Literal['received', 'processed'],
    ) -> None:
        self.positions[handler.name] = 0 if point == 'received' else 1
        pattern = handler.subscribe[0]
        is_glob = ApixHandlerRegistry._is_glob(pattern)
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


__all__ = ["ApixHandlerRegistry"]
