"""Shared types, registries, and predefined names for graph execution."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import (
    Any,
    TYPE_CHECKING,
    TypeAlias,
)

if TYPE_CHECKING:
    from apixis.core.graph.node_graph import NodeGraph


@dataclass(frozen=True, slots=True)
class AutoMerge:
    """Mark an ``Annotated`` state field as auto-increasing.

    This class contains no runtime data. It is metadata collected by
    :class:`NodeGraph` and used when a :class:`Command` update is applied.

    When an existing marked field is updated, the graph calls the current
    value's ``__add__`` method with the update value. A field that is not yet
    present in state is initialized directly from the update value.

    Override ``__add__`` to define a custom merge logic.

    Example:
        ``messages: Annotated[list, AutoMerge()]``
    """

    pass


@dataclass(frozen=True, slots=True)
class KeepRef:
    """Mark an ``Annotated`` state field to keep its reference during copying.

    This class contains no runtime data. It is metadata collected by
    :class:`NodeGraph` and used when a state copy operation is performed.

    When a marked field is copied, the graph keeps the original field value's
    reference instead of creating a copied object. Other state fields continue
    to follow the normal copy behavior.

    This is useful for fields that represent shared runtime resources or
    mutable objects that should remain synchronized across copied states.

    !!! Warning:
        Fields marked with ``KeepRef`` are not recommended for concurrent use.
        Since copied states share the same object reference, concurrent graph
        executions or parallel node operations may access and mutate the same
        object, causing unexpected side effects or race conditions.
        Additionally, it is also not recommended to update the key marked with
        ``KeepRef`` via Command. **This may lead to unpredictable behavior.**

    Example:
        ``context: Annotated[ContextOrganizer, KeepRef()]``
    """

    pass


@dataclass(frozen=True, slots=True)
class Reset:
    """Explicitly replace a state field during a command update.

    ``Reset`` is primarily used to bypass :class:`AutoMerge` for one
    update. The graph unwraps it before storing the value, so the wrapper never
    becomes part of the resulting state.

    Example:
        ``Command(update={"messages": Reset([])})``
    """

    value: Any


_END = "__end__"
"""Internal target marking a completed graph invocation."""


GRAPH_DISPATCH = "__graph_dispatch__"
"""Base name qualified by get_graph_dispatch_name for graph events and handlers."""

GRAPH_INTERRUPTED = "__graph_interrupted__"
"""Base name qualified by get_graph_interrupted_name for graph events and handlers."""

GLOBALNS = "<global>"
"""Explicit namespace for graph events and handlers in the global domain."""

_namespace_graphs: dict[str, NodeGraph] = {}
"""Compiled graph indexed by its exclusive listener namespace."""

namespace_set = _namespace_graphs.keys()
"""Namespaces currently owned by compiled graphs."""


@dataclass(slots=True)
class Command:
    """A node result that updates state and optionally chooses the next node.

    Attributes:
        update:
            Values merged into the state carried by the next event.
            Wrapping a value in :class:`Reset` explicitly replaces that
            field even when its state annotation contains
            :class:`AutoMerge`.
        goto:
            One or more next node names. A list schedules its nodes in one
            concurrent batch and also defines their deterministic result
            application order. ``None`` and an empty list contribute no next
            target. The graph finishes when the whole batch has no next target.
    """

    update: dict[str, Any] = field(default_factory=dict)
    goto: str | list[str] | None = None


NodeResult: TypeAlias = dict[str, Any] | Command
"""A state update or one command returned by a regular graph node."""

NodeFunction: TypeAlias = (
    Callable[[dict[str, Any]], NodeResult]
    | Callable[[dict[str, Any]], Awaitable[NodeResult]]
)
"""A synchronous or asynchronous callable that receives graph state."""