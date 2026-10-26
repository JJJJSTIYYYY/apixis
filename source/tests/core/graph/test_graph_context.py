"""Focused tests for graph context state, lifecycle, and snapshots."""

import asyncio
import json
import time
from typing import Annotated, TypedDict

import pytest

from apixis.core.graph.base import _END

from apixis.core.graph import AutoMerge, KeepRef, NodeGraph

from apixis.core.graph.context import GraphContext, GraphContextSnapshot
from apixis.core.graph.context import noop_stream_writer


class ContextState(TypedDict):
    values: Annotated[list[int], AutoMerge()]
    resource: Annotated[dict, KeepRef()]


@pytest.fixture
def graph():
    """Use a real graph owner for context lifecycle and checkpoint tests."""
    return NodeGraph({}, None, state_schema=ContextState)


def _bind(
    graph,
    context: GraphContext,
    run_id: str,
    state: dict | None = None,
) -> asyncio.Future:
    """Bind a context with the same runtime dependencies as NodeGraph."""
    completion = asyncio.get_running_loop().create_future()
    context.state = graph.create_context(
        state if state is not None else {"value": run_id}
    ).state
    context._bind(
        run_id=run_id,
        completion=completion,
        stream_writer=noop_stream_writer(),
    )
    return completion


def test_new_context_is_pending_unbound_and_has_no_snapshot(graph):
    """An empty graph creates a terminal context without runtime or recovery state."""
    context = graph.create_context({})

    assert context.status == "pending"
    assert context.run_id is None
    assert context.state == {}
    assert context.target_node_name == _END
    assert context.steps == 0
    assert context.context_snapshot == []
    assert context.completion is None
    assert context.stream_writer is None
    assert context.is_consumed is False
    assert context.is_bound is False
    assert context.is_active is False


def test_snapshot_round_trip_preserves_concurrent_target_list(graph):
    """Recovery retains the complete ordered batch target."""
    snapshot = {
        "timestamp": 1.0,
        "state": {"value": 1},
        "target_node_name": ["a", "b"],
        "steps": 2,
        "graph_id": graph.graph_id,
    }

    recovered = graph.restore_context(snapshot)

    assert recovered.target_node_name == ["a", "b"]


@pytest.mark.asyncio
async def test_bind_transitions_pending_to_running_without_taking_snapshot(graph):
    """Binding initializes runtime fields but leaves the snapshot absent."""
    context = graph.create_context({})
    state = {"value": 1}
    completion = _bind(graph, context, "run-1", state)

    assert context.status == "running"
    assert context.run_id == "run-1"
    assert context.state == state
    assert context.state is not state
    assert context.target_node_name == _END
    assert context.context_snapshot == []
    assert context.completion is completion
    assert context.is_consumed is True
    assert context.is_bound is True
    assert context.is_active is True


def test_snapshot_requires_an_active_bound_context(graph):
    """Detached and completed contexts cannot manufacture checkpoints."""
    context = graph.create_context({})

    with pytest.raises(RuntimeError, match="only be taken from an active"):
        context.take_a_snapshot()


@pytest.mark.asyncio
async def test_take_a_snapshot_deep_copies_recoverable_state(graph):
    """Later live-state mutations cannot alter the saved checkpoint."""
    context = graph.create_context({})
    _bind(graph, context, "run-1", {"nested": [1]})
    context.target_node_name = "retry"
    context.steps = 2

    before_snapshot = time.time()
    context.take_a_snapshot()
    after_snapshot = time.time()

    assert len(context.context_snapshot) == 1
    snapshot = context.context_snapshot[-1]
    assert before_snapshot <= snapshot["timestamp"] <= after_snapshot
    assert {key: value for key, value in snapshot.items() if key != "timestamp"} == {
        "state": {"nested": [1]},
        "target_node_name": "retry",
        "steps": 2,
        "graph_id": graph.graph_id,
    }
    assert snapshot["state"] is not context.state
    assert snapshot["state"]["nested"] is not context.state["nested"]

    context.state["nested"].append(2)
    context.state["new"] = "live-only"

    assert snapshot["state"] == {"nested": [1]}
    assert "run_id" not in snapshot
    assert "completion" not in snapshot
    assert "stream_writer" not in snapshot
    assert json.loads(json.dumps(snapshot)) == snapshot


@pytest.mark.asyncio
async def test_restore_context_deep_copies_every_field_including_keep_ref(graph):
    """Recovery deliberately ignores KeepRef and isolates the new attempt."""
    resource = {"items": [1]}
    context = graph.create_context({})
    _bind(
        graph,
        context,
        "run-1",
        {"values": [1], "resource": resource},
    )
    context.target_node_name = "retry"
    context.steps = 3
    context.take_a_snapshot()
    snapshot_history = context.context_snapshot
    snapshot = snapshot_history[-1]
    assert snapshot["state"] is not context.state
    assert snapshot["state"]["values"] is not context.state["values"]
    assert snapshot["state"]["resource"] is not resource
    assert snapshot["state"]["resource"]["items"] is not resource["items"]

    recovered = graph.restore_context(snapshot)

    assert recovered.status == "pending"
    assert recovered.run_id is None
    assert recovered.completion is None
    assert recovered.stream_writer is None
    assert recovered.is_consumed is False
    assert recovered.target_node_name == "retry"
    assert recovered.steps == 3
    assert recovered.graph_id == graph.graph_id
    assert recovered.context_snapshot is not snapshot_history
    assert len(recovered.context_snapshot) == 1
    assert recovered.context_snapshot[-1] is not snapshot
    assert recovered.state is not recovered.context_snapshot[-1]["state"]
    assert recovered.state is not snapshot["state"]
    assert recovered.state["values"] is not snapshot["state"]["values"]
    assert recovered.state["resource"] is not snapshot["state"]["resource"]
    assert recovered.state["resource"] is not resource
    assert recovered.state["resource"]["items"] is not resource["items"]


@pytest.mark.parametrize("snapshot", [None])
def test_restore_context_rejects_missing_snapshot(graph, snapshot):
    """Failures before the first checkpoint cannot be recovered."""
    with pytest.raises(RuntimeError, match="without a snapshot"):
        graph.restore_context(snapshot)


def test_restore_context_rejects_invalid_container(graph):
    """The public restoration API validates its container contract."""
    with pytest.raises(RuntimeError, match="without a snapshot"):
        graph.restore_context(())


def test_restore_context_rejects_empty_history(graph):
    """An empty history contains no recoverable checkpoint."""
    with pytest.raises(RuntimeError, match="without a snapshot"):
        graph.restore_context([])


def test_restore_context_rejects_missing_fields(graph):
    """Partially persisted checkpoints fail with a useful field list."""
    with pytest.raises(
        ValueError, match="missing required fields: graph_id, steps, timestamp"
    ):
        graph.restore_context({"state": {}, "target_node_name": "node"})


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("state", [], "state must be a dict"),
        ("target_node_name", 1, "target_node_name must be a string"),
        ("target_node_name", ["node", 1], "target_node_name must be a string"),
        ("steps", True, "steps must be an int"),
        ("steps", 1.5, "steps must be an int"),
        ("graph_id", None, "graph_id must be a string"),
    ],
)
def test_restore_context_rejects_invalid_field_types(graph, key, value, message):
    """Stored fields retain explicit runtime contracts."""
    snapshot: GraphContextSnapshot = {
        "timestamp": 1.0,
        "state": {},
        "target_node_name": "node",
        "steps": 0,
        "graph_id": graph.graph_id,
    }
    snapshot[key] = value

    with pytest.raises(TypeError, match=message):
        graph.restore_context(snapshot)


def test_restore_context_rejects_negative_steps(graph):
    """A checkpoint cannot precede the start of graph execution."""
    snapshot: GraphContextSnapshot = {
        "timestamp": 1.0,
        "state": {},
        "target_node_name": "node",
        "steps": -1,
        "graph_id": graph.graph_id,
    }

    with pytest.raises(ValueError, match="steps must be at least 0"):
        graph.restore_context(snapshot)


@pytest.mark.asyncio
async def test_snapshot_history_restores_latest_version_by_default(graph):
    """Each checkpoint is retained and default recovery selects the latest."""
    context = graph.create_context({})
    _bind(graph, context, "run-1", {"history": ["first"]})
    context.target_node_name = "first-node"
    context.steps = 1
    context.take_a_snapshot()

    context.state = {"history": ["second"]}
    context.target_node_name = "second-node"
    context.steps = 2
    context.take_a_snapshot()

    recovered = graph.restore_context(context.context_snapshot)

    assert len(context.context_snapshot) == 2
    assert recovered.state == {"history": ["second"]}
    assert recovered.target_node_name == "second-node"
    assert recovered.steps == 2
    assert len(recovered.context_snapshot) == 2
    assert recovered.context_snapshot is not context.context_snapshot
    assert recovered.context_snapshot[-1] is not context.context_snapshot[-1]


@pytest.mark.asyncio
async def test_snapshot_history_restores_selected_version_as_new_branch(graph):
    """Restoring an older checkpoint drops later versions from the new branch."""
    context = graph.create_context({})
    _bind(graph, context, "run-1", {"value": 1})

    for version in range(3):
        context.state = {"value": version}
        context.target_node_name = f"node-{version}"
        context.steps = version
        context.take_a_snapshot()

    recovered = graph.restore_context(context.context_snapshot, version=1)
    recovered_with_negative_version = graph.restore_context(
        context.context_snapshot,
        version=-2,
    )

    assert recovered.state == {"value": 1}
    assert recovered.target_node_name == "node-1"
    assert recovered.steps == 1
    assert len(recovered.context_snapshot) == 2
    assert recovered.get_snapshot() == context.context_snapshot[1]
    assert recovered.get_snapshot(0) == context.context_snapshot[0]
    assert recovered_with_negative_version.state == recovered.state
    assert recovered_with_negative_version.context_snapshot == (
        recovered.context_snapshot
    )


@pytest.mark.parametrize("version", [3, -4])
def test_restore_context_rejects_missing_version(graph, version):
    """Versions outside the stored history fail explicitly."""
    snapshot: GraphContextSnapshot = {
        "timestamp": 1.0,
        "state": {},
        "target_node_name": "node",
        "steps": 0,
        "graph_id": graph.graph_id,
    }

    with pytest.raises(IndexError, match="list index out of range"):
        graph.restore_context([snapshot, snapshot, snapshot], version=version)


@pytest.mark.parametrize("version", [1.5, "1"])
def test_restore_context_validates_version_type(graph, version):
    """History versions require integer indices."""
    snapshot: GraphContextSnapshot = {
        "timestamp": 1.0,
        "state": {},
        "target_node_name": "node",
        "steps": 0,
        "graph_id": graph.graph_id,
    }

    with pytest.raises(TypeError, match="snapshot version must be an int"):
        graph.restore_context([snapshot], version=version)


def test_invalid_status_transition_is_rejected(graph):
    """The transition table rejects paths outside the lifecycle contract."""
    context = graph.create_context({})

    with pytest.raises(RuntimeError, match="pending -> finished"):
        context._transition_to("finished")


def test_pending_context_can_abort_but_has_no_recovery_snapshot(graph):
    """Pending aborts are terminal and have no recovery checkpoint."""
    aborted = graph.create_context({})
    aborted.abort()
    aborted.abort()
    assert aborted.status == "aborted"
    assert aborted.context_snapshot == []


@pytest.mark.asyncio
async def test_running_abort_resolves_the_saved_snapshot_and_is_idempotent(graph):
    """Abort returns the checkpoint even if newer context state is present."""
    context = graph.create_context({})
    completion = _bind(graph, context, "run-1", {"history": ["saved"]})
    context.target_node_name = "retry"
    context.take_a_snapshot()
    context.state = {"history": ["newer"]}

    context.abort()
    context.abort()

    result = await completion
    assert result == {"history": ["saved"]}
    assert result is context.context_snapshot[-1]["state"]
    assert result["history"] is context.context_snapshot[-1]["state"]["history"]
    assert context.status == "aborted"
    assert context.is_active is False


@pytest.mark.asyncio
async def test_finish_is_idempotent_and_does_not_replace_snapshot(graph):
    """Completion resolves current state while leaving the previous checkpoint intact."""
    pending = graph.create_context({})
    with pytest.raises(RuntimeError, match="completion is None"):
        pending._finish()
    assert pending.status == "pending"

    finished = graph.create_context({})
    completion = _bind(graph, finished, "run-1")
    finished.target_node_name = "node"
    finished.take_a_snapshot()
    snapshot = finished.context_snapshot
    finished.state = {"value": "finished"}
    finished._finish()
    finished._finish()

    assert await completion == {"value": "finished"}
    assert finished.status == "finished"
    assert finished.context_snapshot is snapshot
    assert finished.context_snapshot[-1]["state"] == {"value": "run-1"}


@pytest.mark.asyncio
async def test_failed_context_restores_into_a_new_pending_context(graph):
    """The original attempt stays failed while its deep copy becomes reusable."""
    context = graph.create_context({})
    completion = _bind(
        graph,
        context,
        "run-1",
        {"values": [1], "resource": {}},
    )
    context.target_node_name = "retry-node"
    context.steps = 3
    context.take_a_snapshot()
    error = RuntimeError("failed")
    context._fail(error)

    with pytest.raises(RuntimeError, match="failed"):
        await completion
    recovered = graph.restore_context(context.context_snapshot)

    assert context.status == "failed"
    assert recovered.status == "pending"
    assert recovered.target_node_name == "retry-node"
    assert recovered.steps == 3
    assert recovered.graph_id == graph.graph_id
    assert recovered.run_id is None
    assert recovered.completion is None


@pytest.mark.asyncio
async def test_finished_context_cannot_abort_or_start_again(graph):
    """Finished contexts remain terminal and single-use."""
    context = graph.create_context({})
    completion = _bind(graph, context, "run-1")
    context._finish()
    await completion

    with pytest.raises(RuntimeError, match="finished -> aborted"):
        context.abort()
    with pytest.raises(RuntimeError, match="finished -> running"):
        _bind(graph, context, "run-2")


@pytest.mark.asyncio
async def test_running_context_cannot_start_again(graph):
    """A context cannot represent two invocation attempts."""
    context = graph.create_context({})
    _bind(graph, context, "run-1")

    with pytest.raises(RuntimeError, match="running -> running"):
        _bind(graph, context, "run-2")
