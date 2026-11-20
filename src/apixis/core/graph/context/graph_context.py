"""Invocation-local graph state, lifecycle, and recovery snapshots."""

from __future__ import annotations

import copy
from asyncio import Future
from dataclasses import dataclass, field
import time
from typing import Any, Callable, Literal, TypeAlias, TypedDict

from apixis.core.config.core_config import GRAPH_MAX_SNAPSHOTS
from apixis.core.utils.validation import validate_int
from apixis.core.graph.base import (
    _END,
)
from apixis.core.graph.context.stream_writer import StreamWriter


GraphContextStatus: TypeAlias = Literal[
    "pending",
    "running",
    "failed",
    "aborted",
    "finished",
]


class GraphContextSnapshot(TypedDict):
    """Checkpoint owned by a single compiled graph instance.

    State is deep-copied when the snapshot is taken. Restoring a snapshot
    deep-copies the complete mapping again, including state fields marked with
    :class:`~apixis.core.graph.base.KeepRef`.
    """

    timestamp: float
    state: dict[str, Any]
    target_node_name: str | list[str]
    steps: int
    graph_id: str


_ALLOWED_STATUS_TRANSITIONS: dict[
    GraphContextStatus,
    frozenset[GraphContextStatus],
] = {
    "pending": frozenset({"running", "failed", "aborted"}),
    "running": frozenset({"failed", "aborted", "finished"}),
    "failed": frozenset(),
    "aborted": frozenset(),
    "finished": frozenset(),
}


@dataclass(slots=True, eq=False)
class GraphContext:
    """State, lifecycle, and checkpoints for one graph-owned execution attempt.

    The immutable public graph_id identifies ownership without retaining a
    graph object. Use graph.create_context() and graph.restore_context() to
    prepare contexts. The graph manages only running attempts. Contexts store
    state references; their graph applies schema-specific copy and merge
    policies at execution boundaries.
    """

    _graph_id: str
    _max_snapshots: int = field(default=GRAPH_MAX_SNAPSHOTS, repr=False)
    run_id: str | None = field(default=None, init=False)
    state: dict[str, Any] = field(default_factory=dict, init=False)
    target_node_name: str | list[str] = field(default=_END, init=False)
    steps: int = field(default=0, init=False)
    context_snapshot: list[GraphContextSnapshot] = field(
        default_factory=list,
        init=False,
    )
    completion: Future[Any] | None = field(
        default=None,
        init=False,
        repr=False,
    )
    stream_writer: StreamWriter | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _status: GraphContextStatus = field(
        default="pending",
        init=False,
    )
    _on_terminal: Callable[[GraphContext], None] | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        validate_int(self._max_snapshots, "max_snapshots", minimum=1)

    @property
    def max_snapshots(self) -> int:
        """Maximum number of recent checkpoints retained by this context."""
        return self._max_snapshots

    @property
    def graph_id(self) -> str:
        """Return the compiled graph identity shared by this context's snapshots."""
        return self._graph_id

    @property
    def status(self) -> GraphContextStatus:
        """Return the current lifecycle state."""
        return self._status

    @property
    def is_consumed(self) -> bool:
        """Return whether this context can no longer start an invocation."""
        return self._status != "pending"

    @property
    def is_bound(self) -> bool:
        """Return whether runtime fields identify an invocation attempt."""
        return (
            self.run_id is not None
            and self.completion is not None
            and self.stream_writer is not None
        )

    @property
    def is_active(self) -> bool:
        """Return whether the current attempt is running and incomplete."""
        return self._status == "running"

    def _transition_to(self, status: GraphContextStatus) -> None:
        """Apply one validated lifecycle transition."""
        # Repeating a terminal notification must not replace its outcome.
        # Running is deliberately excluded: an attempt cannot be bound twice.
        if status == self._status and status in ("finished", "failed", "aborted"):
            return
        if status not in _ALLOWED_STATUS_TRANSITIONS[self._status]:
            raise RuntimeError(
                f"Invalid GraphContext status transition: {self._status} -> {status}."
            )
        self._status = status
        if status != "running" and self._on_terminal is not None:
            # Release runtime ownership at the transition, even for direct aborts.
            on_terminal, self._on_terminal = self._on_terminal, None
            on_terminal(self)

    def _bind(
        self,
        *,
        run_id: str,
        completion: Future[Any],
        stream_writer: StreamWriter,
        on_terminal: Callable[[GraphContext], None] | None = None,
    ) -> None:
        """Attach runtime resources to one pending attempt without copying state."""
        # Validate before replacing any resources belonging to an older run.
        self._transition_to("running")
        self.run_id = run_id
        self.completion = completion
        self.stream_writer = stream_writer
        self._on_terminal = on_terminal

    def _set_target_node(self, target_node_name: str | list[str]) -> None:
        """Record the node or concurrent batch targeted by dispatch."""
        self.target_node_name = (
            list(target_node_name)
            if isinstance(target_node_name, list)
            else target_node_name
        )

    def take_a_snapshot(self) -> None:
        """Capture an isolated copy of the current recoverable state."""
        if not self.is_active:
            raise RuntimeError(
                "A snapshot can only be taken from an active GraphContext."
            )
        self.context_snapshot.append(
            {
                "timestamp": time.time(),
                "state": copy.deepcopy(self.state),
                "target_node_name": (
                    list(self.target_node_name)
                    if isinstance(self.target_node_name, list)
                    else self.target_node_name
                ),
                "steps": self.steps,
                "graph_id": self.graph_id,
            }
        )
        del self.context_snapshot[:-self._max_snapshots]

    def get_snapshot(
        self,
        version: int = -1,
    ) -> GraphContextSnapshot | None:
        """Return an isolated copy of the selected checkpoint, if one exists."""
        version = validate_int(version, "snapshot version")
        if not self.context_snapshot:
            return None
        return copy.deepcopy(self.context_snapshot[version])

    def get_all_snapshots(self) -> list[GraphContextSnapshot]:
        """Return an isolated copy of checkpoints list."""
        return copy.deepcopy(self.context_snapshot)

    def _latest_snapshot_state(self) -> dict[str, Any]:
        """Return the stored checkpoint state, falling back to the live state.

        This is an internal reference. The graph copies execution results
        before exposing them to callers, using its own state policy.
        """
        return (
            self.context_snapshot[-1]["state"] if self.context_snapshot else self.state
        )

    def _finish(self) -> None:
        """Resolve a running attempt and enter the terminal finished state."""
        completion = self.completion
        if completion is None:
            raise RuntimeError(
                "Cannot finish a running context because the completion is None."
            )
        self._transition_to("finished")
        result = None if completion.done() else self.state
        if not completion.done():
            completion.set_result(result)

    def _fail(self, error: Exception) -> None:
        """Fail a pending or running attempt without changing its snapshot."""
        self._transition_to("failed")
        completion = self.completion
        if completion is not None and not completion.done():
            completion.set_exception(error)

    def abort(self) -> None:
        """Abort a pending or running attempt at its latest snapshot."""
        self._transition_to("aborted")
        completion = self.completion
        # Pending contexts own no runtime resources or graph management reference.
        if completion is not None and not completion.done():
            completion.set_result(self._latest_snapshot_state())

    def _cancel(self) -> None:
        """Abort an attempt by cancelling its completion instead of returning state."""
        self._transition_to("aborted")
        if self.completion is not None and not self.completion.done():
            self.completion.cancel()
