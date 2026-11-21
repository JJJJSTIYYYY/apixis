"""Registered handlers, ordinary routing caches, ordering, and group indexes."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Sequence
from fnmatch import fnmatchcase
from typing import Literal

from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.event_registry import ApixEventRegistry
from apixis.core.event.handler_registry.matching import is_glob_pattern, matches_handler
from apixis.core.event.handler_registry.waiter_cache import WaiterHandlerCache, WaiterLifecycle
from apixis.core.utils.logger import Logger
from apixis.core.utils.validation import validate_int, validate_non_empty_string, validate_number
from apixis.core.utils.exception import (
    CoreClosedError,
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
        # whose _priority is None. Removal leaves ordinals intact.
        self._handler_order: dict[str, tuple[float, int]] = {}
        self._bucket_next_order: dict[float, int] = {}
        self._register_order = 0
        self._waiter_cache = WaiterHandlerCache(self._cache_size)
        self._closed = False

    @property
    def closed(self) -> bool:
        """Whether the owner has sealed registration for terminal closure."""
        return self._closed

    def close_admission(self) -> None:
        """Reject new registrations while existing callbacks finish cleanup."""
        self._closed = True

    async def close(self) -> None:
        """Complete active waiters directly and release registrations.

        In-flight cancellation notifications run first in the event loop.
        Remaining waiters finish with None without producing any event or
        invoking listener callbacks. Unregistration stays available throughout
        closure so waiter-owned cleanup can complete normally.
        """
        self.close_admission()
        await self.cancel_waiters()
        self.clear()

    def register_waiter(
        self, waiter: WaiterLifecycle, handlers: Sequence[ApixEventHandler], *,
        point: Literal['received', 'processed'],
    ) -> None:
        """Register a new collection and all its listeners as one operation.

        The waiting API supplies validated single-pattern handlers and an
        observation point. The registry records each listener's owner and
        installs the whole collection synchronously. Listener failure rolls
        back only entries installed by this call, preserving existing entries.
        """
        if self._closed:
            raise CoreClosedError("The handler registry is closed.")
        if waiter in self._waiter_cache.waiters:
            raise ValueError("The waiter is already registered.")
        self._waiter_cache.waiters[waiter] = {}
        try:
            for handler in handlers:
                self._add_waiter_handler(handler, point=point, waiter=waiter)
        except BaseException:
            self.unregister_waiter(waiter)
            raise

    def unregister_waiter(self, waiter: WaiterLifecycle) -> None:
        """Remove a collection and all listeners still registered for it.

        Repeated calls are harmless. Individual handler or group removal does
        not end collection ownership; its Future remains tracked until this
        operation runs when the waiter reaches a terminal state.
        """
        handlers = self._waiter_cache.waiters.get(waiter)
        if handlers is None:
            return
        for name in tuple(handlers):
            self.unregister_handler(name)
        del self._waiter_cache.waiters[waiter]

    async def cancel_waiters(self) -> None:
        """Finish active collections with None without creating notifications.

        The snapshot permits finish() to unregister the waiter and its listeners.
        Already terminal Futures retain their result, exception or cancellation.
        Their close() retries any interrupted listener removal.
        """
        for waiter in tuple(self._waiter_cache.waiters):
            if waiter.active:
                waiter.finish()
            else:
                # A previous release may have failed after completing the Future.
                waiter.close()

    def clear(self) -> None:
        """Remove registrations and cached chains without changing observations.

        Already captured handler references and chain lists remain usable.
        Pending waiter Futures stay tracked for close even without listeners.
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

    @property
    def waiter_count(self) -> int:
        """Return registered wait listeners; each pattern owns one listener."""
        return len(self._waiter_cache.positions)

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
            if any(is_glob_pattern(pattern) for pattern in handler._subscribe)
            else (name for name in handler._subscribe if name in self.cached_chain)
        )
        need_delete = []
        for event_name in event_names:
            if matches_handler(handler, event_name):
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
                for pattern in self.registry[name]._filter_event
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
        return [*(handler._name for handler in received), *chain,
                *(handler._name for handler in processed)]

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
        for name in chain:
            handlers.append(self.registry[name])
        handlers.extend(processed)
        return handlers

    def _add_handler_record(self, handler: ApixEventHandler) -> None:
        """Install identity, group membership and registration order."""
        handler._register_order = self._register_order
        self._register_order += 1
        self.registry[handler._name] = handler
        self._group_handlers.setdefault(handler._group, set()).add(handler._name)
        self._handler_groups[handler._name] = handler._group

    def _add_waiter_handler(
        self, handler: ApixEventHandler, *, point: Literal['received', 'processed'],
        waiter: WaiterLifecycle,
    ) -> None:
        """Install one prevalidated handler belonging to a wait collection."""
        if handler._name in self.registry:
            raise EventHandlerAlreadyRegisteredError(
                f"Handler `{handler._name}` already registered."
            )
        self._add_handler_record(handler)
        self._waiter_cache.index_handler(handler, point, waiter)

    def _remove_handler_from_priority_bucket(self, handler_name: str) -> None:
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

    def _index_handler_patterns(self, handler: ApixEventHandler) -> None:
        exact_names = []
        glob_patterns = []
        for pattern in handler._subscribe:
            if is_glob_pattern(pattern):
                glob_patterns.append(pattern)
            else:
                exact_names.append(pattern)
                self._exact_subscriptions.setdefault(pattern, set()).add(handler._name)
        self._handler_exact_names[handler._name] = tuple(exact_names)
        if glob_patterns:
            self._glob_subscriptions[handler._name] = tuple(glob_patterns)

    def _unindex_handler_patterns(self, handler_name: str) -> None:
        for event_name in self._handler_exact_names.pop(handler_name):
            names = self._exact_subscriptions[event_name]
            names.remove(handler_name)
            if not names:
                del self._exact_subscriptions[event_name]
        self._glob_subscriptions.pop(handler_name, None)

    def _prepare_handler_registration(
        self, handler_entry: ApixEventHandler, *, exist_ok: bool = False,
    ) -> tuple[list[str], list[str]]:
        """Validate common fields and return normalized subscription/filter patterns."""
        if self._closed:
            raise CoreClosedError("The handler registry is closed.")
        if not isinstance(handler_entry, ApixEventHandler):
            raise TypeError("handler_entry must be an ApixEventHandler instance.")
        if not handler_entry._name:
            raise ValueError("Handler name cannot be empty.")
        validate_non_empty_string(handler_entry._group, "Handler group")
        if handler_entry._name in self.registry and not exist_ok:
            raise EventHandlerAlreadyRegisteredError(
                f"Handler `{handler_entry._name}` already registered."
            )
        if not callable(handler_entry._core_func):
            raise TypeError("Handler core_func must be callable.")
        if handler_entry._on_accepted is not None and not callable(handler_entry._on_accepted):
            raise TypeError("Handler on_accepted must be callable.")
        if handler_entry._on_has_error is not None and not callable(handler_entry._on_has_error):
            raise TypeError("Handler on_has_error must be callable.")
        if handler_entry._on_error is not None and not callable(handler_entry._on_error):
            raise TypeError("Handler on_error must be callable.")
        if handler_entry._on_cancelled is not None and not callable(handler_entry._on_cancelled):
            raise TypeError("Handler on_cancelled must be callable.")
        subscriptions = self._normalise_patterns(
            handler_entry._subscribe,
            argument_name="subscribe",
        )
        filters = (
            self._normalise_patterns(handler_entry._filter_event, argument_name="filter_event",)
            if handler_entry._filter_event else []
        )
        return subscriptions, filters

    def _replace_handler_record(
        self, handler: ApixEventHandler, subscriptions: list[str], filters: list[str],
    ) -> None:
        """Remove the old registration and record its validated replacement."""
        self.unregister_handler(handler._name, missing_ok=True)
        handler._subscribe = tuple(subscriptions)
        handler._filter_event = tuple(filters)
        self._add_handler_record(handler)

    def register_handler(
        self, handler_entry: ApixEventHandler, *, exist_ok: bool = False,
    ) -> None:
        """Register an ordinary handler using priority or relative insertion.

        Matching and numeric priority buckets belong to the ordinary chain.
        Validation precedes replacement so failures preserve the previous entry.
        Event wait collections use register_waiter() and their own matching
        indexes. Their observation points are not part of ordinary registration.
        """
        subscriptions, filters = self._prepare_handler_registration(handler_entry, exist_ok=exist_ok)
        if handler_entry._priority is not None and handler_entry._between_handlers is not None:
            raise ValueError(
                "between_handlers and priority cannot be set together."
            )

        between_handlers = handler_entry._between_handlers
        if between_handlers is not None:
            bucket_priority = self._validate_between_handlers(
                between_handlers,
                handler_name=handler_entry._name
            )
        else:
            priority = handler_entry._priority
            bucket_priority = validate_number(priority, "Handler priority")

        self._replace_handler_record(handler_entry, subscriptions, filters)
        bucket = self.priority_buckets.setdefault(bucket_priority, [])
        if between_handlers is None:
            bucket.append(handler_entry._name)
            ordinal = self._bucket_next_order.get(bucket_priority, 0)
            self._handler_order[handler_entry._name] = -bucket_priority, ordinal
            self._bucket_next_order[bucket_priority] = ordinal + 1
        else:
            left_name, right_name = between_handlers
            if right_name is not None:
                insert_index = bucket.index(right_name)
            else:
                insert_index = bucket.index(left_name) + 1
            bucket.insert(insert_index, handler_entry._name)
            # Relative insertion is uncommon and already shifts this bucket's
            # list. Reindex only this bucket; ordinary append/removal stays local.
            for ordinal, name in enumerate(bucket):
                self._handler_order[name] = -bucket_priority, ordinal
            self._bucket_next_order[bucket_priority] = len(bucket)
        self._index_handler_patterns(handler_entry)
        self._invalidate_matching_chains(handler_entry)

        Logger.debug(
            f"Registered handler `{handler_entry._name}`, "
            f"group={handler_entry._group}, "
            f"priority={handler_entry._priority}, "
            f"between_handlers={handler_entry._between_handlers}"
        )

    def unregister_handler(self, handler_name: str, *, missing_ok: bool = False) -> None:
        """Remove one ordinary handler or listener and its indexes and ownership."""
        handler = self.registry.get(handler_name)
        if handler is None:
            if not missing_ok:
                raise EventHandlerNotRegisteredError(
                    f"Handler `{handler_name}` not registered."
                )
            return
        if handler_name in self._waiter_cache:
            self._waiter_cache.unindex_handler(handler)
        else:
            self._invalidate_matching_chains(handler)
            self._unindex_handler_patterns(handler_name)
            self._remove_handler_from_priority_bucket(handler_name)
        group = self._handler_groups.pop(handler_name)
        members = self._group_handlers[group]
        members.remove(handler_name)
        if not members:
            del self._group_handlers[group]
        del self.registry[handler_name]
        Logger.debug(f"Unregistered handler {handler_name}.")

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
            for subscription in handler._subscribe
            if not any(
                fnmatchcase(event_name, subscription)
                and not any(
                    fnmatchcase(event_name, pattern)
                    for pattern in handler._filter_event
                )
                for event_name in event_names
            )
        ]


__all__ = [
    "ApixHandlerRegistry",
]
