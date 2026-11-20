"""Public ownership, admission, replacement, and checkpoint-order contracts."""

import asyncio
import copy
from typing import Annotated, TypedDict
from unittest.mock import patch

import pytest

from apixis.core.event import get_handler
from apixis.core.graph import AutoMerge, KeepRef, GraphManager, NodeGraph
from apixis.core.graph import base as graph_base
from apixis.core.graph.utils import state as state_utils
from apixis.core.graph.context import GraphContext
from apixis.core.graph.interrupter import interrupt
from apixis.core.graph.utils.namespace import get_graph_interrupted_name
from apixis.core.utils.exception import InvalidContextError


pytestmark = pytest.mark.asyncio(loop_scope="session")


class State(TypedDict):
    history: Annotated[list[str], AutoMerge()]
    resource: Annotated[dict, KeepRef()]


def build(schema=None, namespace="ownership", replace=False, node=None):
    """Compile interchangeable node names without sharing graph ownership."""
    return (
        GraphManager(schema)
        .add_node(node or (lambda state: {"history": ["node"]}), "work")
        .compile_graph(entry_point="work", using_namespace=namespace, exist_ok=replace)
    )


async def test_schema_is_resolved_once_and_frozen_for_created_and_restored_contexts():
    class MutableState(TypedDict):
        history: Annotated[list[str], AutoMerge()]
        resource: Annotated[dict, KeepRef()]

    resource = {"items": []}
    with patch.object(
        state_utils, "get_type_hints", wraps=state_utils.get_type_hints
    ) as resolve:
        graph = build(MutableState)
        assert resolve.call_count == 1
        MutableState.__annotations__.clear()
        context = graph.create_context({"history": ["start"], "resource": resource})
        assert context.state["resource"] is resource
        assert (await graph.invoke(graph_context=context))["history"] == [
            "start",
            "node",
        ]
        recovered = graph.restore_context(context.get_all_snapshots())
        assert (await graph.invoke(graph_context=recovered))["history"] == [
            "start",
            "node",
        ]
        assert (await graph.invoke({"history": []}))["history"] == ["node"]
        assert resolve.call_count == 1


async def test_context_creation_copies_input_and_has_read_only_owner():
    graph = build(State)
    source = {"history": ["start"], "resource": {"items": []}}
    context = graph.create_context(source)
    source["history"].append("caller")
    assert context.state["history"] == ["start"]
    assert context.state["resource"] is source["resource"]
    assert context.graph_id == graph.graph_id
    with pytest.raises(AttributeError):
        context.graph_id = graph.graph_id
    prepared = GraphContext(graph.graph_id)
    prepared.state = {"history": []}
    prepared.target_node_name = "work"
    assert await graph.invoke(graph_context=prepared) == {"history": ["node"]}
    assert (await graph.invoke(graph_context=context))["history"] == ["start", "node"]


async def test_foreign_admission_and_ambiguous_input_leave_context_usable():
    owner = build(State, "owner")
    other = build(None, "other")
    context = owner.create_context({"history": ["start"]})
    state = context.state
    with pytest.raises(InvalidContextError, match="different graph"):
        await other.invoke(graph_context=context)
    with pytest.raises(InvalidContextError, match="different graph"):
        await anext(other.stream(graph_context=context))
    with pytest.raises(TypeError, match="not both"):
        await owner.invoke({}, graph_context=context)
    with pytest.raises(TypeError, match="not both"):
        await anext(owner.stream({}, graph_context=context))
    assert context.status == "pending"
    assert context.run_id is None
    assert context.state is state
    assert (await owner.invoke(graph_context=context))["history"] == ["start", "node"]


async def test_invalid_restored_entry_is_rejected_without_consuming_context():
    graph = build(State)
    original = graph.create_context({"history": []})
    await graph.invoke(graph_context=original)
    checkpoint = original.get_snapshot()
    checkpoint["target_node_name"] = "removed-node"
    context = graph.restore_context(checkpoint)
    before = copy.deepcopy(context.state)
    with pytest.raises(ValueError, match="Unknown graph node"):
        await graph.invoke(graph_context=context)
    assert context.status == "pending"
    assert context.run_id is None
    assert context.state == before
    assert context.get_snapshot() == checkpoint
    # Correcting the entry allows the same rejected context to start normally.
    context.target_node_name = "work"
    assert await graph.invoke(graph_context=context) == {"history": ["node"]}


async def test_concurrent_submission_accepts_a_context_only_once():
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def work(state):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"history": ["node"]}

    graph = build(State, node=work)
    context = graph.create_context({"history": []})
    first = asyncio.create_task(graph.invoke(graph_context=context))
    try:
        await asyncio.wait_for(started.wait(), 1)
        run_id = context.run_id
        with pytest.raises(InvalidContextError, match="pending"):
            await graph.invoke(graph_context=context)
        with pytest.raises(InvalidContextError, match="pending"):
            await anext(graph.stream(graph_context=context))
        assert context.status == "running"
        assert context.run_id == run_id
    finally:
        release.set()
        await asyncio.wait_for(first, 1)
    assert calls == 1
    assert context.state == {"history": ["node"]}


@pytest.mark.parametrize(
    "old_schema,new_schema", [(State, None), (None, State), (State, State)]
)
@pytest.mark.parametrize("restore_before_replace", [True, False])
async def test_replacement_cannot_adopt_prior_contexts_or_checkpoints(
    old_schema, new_schema, restore_before_replace
):
    old = build(old_schema)
    unused = old.create_context({"history": ["pending"]})
    executed = old.create_context({"history": ["start"]})
    await old.invoke(graph_context=executed)
    checkpoint = executed.get_snapshot()
    recovered = old.restore_context(checkpoint) if restore_before_replace else None

    new = build(new_schema, replace=True)
    assert new.namespace == old.namespace
    assert new.graph_id != old.graph_id
    for context in (unused, executed, recovered):
        if context is None:
            continue
        status = context.status
        with pytest.raises(InvalidContextError, match="different graph"):
            await new.invoke(graph_context=context)
        assert context.graph_id == old.graph_id
        assert context.status == status
    with pytest.raises(ValueError, match="different graph"):
        new.restore_context(checkpoint)
    with pytest.raises(RuntimeError, match="decomposed"):
        old.restore_context(checkpoint)
    with pytest.raises(RuntimeError, match="decomposed"):
        old.create_context({})
    with pytest.raises(RuntimeError, match="decomposed"):
        await old.invoke(graph_context=unused)
    expected = ["start", "node"] if new_schema else ["node"]
    assert (await new.invoke({"history": ["start"]}))["history"] == expected


async def test_pending_abort_does_not_inherit_new_schema_or_become_reusable():
    graph = build(State)
    context = graph.create_context({"history": []})
    context.abort()
    with pytest.raises(InvalidContextError, match="pending"):
        await graph.invoke(graph_context=context)
    assert context.status == "aborted"
    assert context.run_id is None


async def test_recovery_history_stays_independent_under_keep_ref_mutation():
    def work(state):
        state["resource"]["items"].append("node")
        return {"history": ["node"]}

    graph = build(State, node=work)
    original = graph.create_context({"history": ["start"], "resource": {"items": []}})
    await graph.invoke(graph_context=original)
    source = original.get_all_snapshots()
    saved = copy.deepcopy(source)
    restored = graph.restore_context(source)
    result = await graph.invoke(graph_context=restored)
    result["resource"]["items"].append("caller")
    assert source == saved
    assert restored.get_snapshot(0) == saved[0]
    assert restored.get_snapshot(1)["state"] == saved[0]["state"]
    branch = graph.restore_context(restored.get_all_snapshots(), version=0)
    assert len(branch.get_all_snapshots()) == 1
    assert (await graph.invoke(graph_context=branch))["resource"]["items"] == ["node"]
    assert restored.get_snapshot(0) == saved[0]


async def test_restore_rejects_mixed_graph_history_and_legacy_namespace_checkpoint():
    graph = build(State)
    context = graph.create_context({"history": []})
    await graph.invoke(graph_context=context)
    snapshot = context.get_snapshot()
    foreign = dict(snapshot, graph_id="foreign")
    with pytest.raises(ValueError, match="different graph"):
        graph.restore_context([foreign, snapshot])
    legacy = dict(snapshot, namespace=graph.namespace)
    del legacy["graph_id"]
    with pytest.raises(ValueError, match="graph_id"):
        graph.restore_context(legacy)


async def test_invalid_schema_leaves_original_graph_executable():
    old = build(State)
    context = old.create_context({"history": ["start"]})
    with pytest.raises(TypeError, match="state_schema"):
        build({}, replace=True)
    assert (await old.invoke(graph_context=context))["history"] == ["start", "node"]


@pytest.mark.parametrize("fail_after_registration", [False, True])
async def test_failed_registration_releases_namespace_without_restoring_old_graph(
    monkeypatch, fail_after_registration
):
    """Acquisition retires the old graph before the new listener is registered."""
    old = build(State)
    context = old.create_context({"history": ["start"]})
    register = NodeGraph._register_dispatch_handler
    error = RuntimeError("registration rejected")

    def reject_registration(self):
        assert self.namespace in graph_base.namespace_set
        assert context.status == "pending"
        assert get_handler(self.dispatch_name) is None
        with pytest.raises(RuntimeError, match="decomposed"):
            old.create_context({})
        if fail_after_registration:
            register(self)
        raise error

    with monkeypatch.context() as scoped:
        scoped.setattr(NodeGraph, "_register_dispatch_handler", reject_registration)
        with pytest.raises(RuntimeError, match="registration rejected") as caught:
            build(None, replace=True)
    assert caught.value is error
    assert old.namespace not in graph_base.namespace_set
    assert get_handler(old.dispatch_name) is None
    with pytest.raises(RuntimeError, match="decomposed"):
        await old.invoke(graph_context=context)

    replacement = build(State)
    assert (await replacement.invoke({"history": ["fresh"]}))["history"] == [
        "fresh", "node"
    ]


async def test_abort_closes_owned_interrupt_before_namespace_replacement():
    captured = asyncio.get_running_loop().create_future()
    old_called = []
    new_called = []

    async def work(state):
        await interrupt(data="review")
        return {"history": ["resumed"]}

    old = build(State, node=work)

    @old.add_interrupted_hook
    async def old_hook(block):
        old_called.append(block)
        captured.set_result(block)

    context = old.create_context({"history": ["start"]})
    invocation = asyncio.create_task(old.invoke(graph_context=context))
    block = await asyncio.wait_for(captured, 1)
    assert block.graph_id == context.graph_id
    context.abort()
    assert await asyncio.wait_for(invocation, 1) == {"history": ["start"]}
    assert block.done
    new = build(State, replace=True)

    @new.add_interrupted_hook
    async def new_hook(block):
        new_called.append(block)

    # Reposting a delayed interruption cannot transfer it to the new graph.
    from apixis.core.event.factory import get_event_pipe
    from apixis.core.event import EventType

    await get_event_pipe().post_event(
        event_type=EventType.WORKFLOW,
        event_name=get_graph_interrupted_name(new.namespace, missing_ok=True),
        context=block,
    )
    await new.invoke({"history": []})
    assert len(old_called) == 1
    assert new_called == []
    block.resolve("late")
    assert context.status == "aborted"
