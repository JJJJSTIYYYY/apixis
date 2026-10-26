"""Focused unit tests for NodeGraph defensive runtime behaviour."""

import asyncio
from typing import Annotated, TypedDict

import pytest

from apixis.core.graph.base import _END

from apixis.core.event.factory import (
    get_event_loop,
    get_handler_registry,
    get_event_pipe,
)
from apixis.core.event import unsubscribe, subscribe
from apixis.core.utils.exception import EventHandlerAlreadyRegisteredError
from apixis.core.graph import (
    AutoMerge,
    Command,
    GLOBALNS,
    Node,
    NodeGraph,
    Reset,
)
from apixis.core.graph.context import GraphContext
from apixis.core.graph.context import noop_stream_writer
from apixis.core.graph.base import namespace_set
from apixis.core.graph import get_graph_dispatch_name
from uuid import UUID


def _bound_context(
    graph: NodeGraph,
    run_id: str,
    state: dict,
    *,
    steps: int = 0,
) -> GraphContext:
    """Build a fully bound context for lifecycle unit tests."""
    context = graph.create_context(state)
    graph._begin_invocation(None, context, noop_stream_writer())
    context.run_id = run_id
    context.steps = steps
    return context


def test_apply_command_rejects_non_dict_update():
    """NodeGraph defensively validates commands from external callers."""
    graph = NodeGraph({}, None)

    with pytest.raises(TypeError, match="Command.update must be a dict"):
        graph.apply_command(Command(update=[]), "node", graph.create_context({}))


@pytest.mark.parametrize("using_namespace", [None, ""])
def test_empty_listener_namespace_uses_generated_namespace(
    using_namespace,
):
    """None and an empty string both request a generated listener namespace."""
    graph = NodeGraph(
        {},
        None,
        using_namespace=using_namespace,
    )

    assert UUID(hex=graph.namespace).version == 4


def test_global_listener_namespace_must_be_explicit():
    """GLOBALNS selects the shared global listener domain explicitly."""
    graph = NodeGraph({}, None, using_namespace=GLOBALNS)

    assert graph.namespace == GLOBALNS


def test_listener_namespace_uses_supplied_value():
    """A non-empty listener namespace is retained without random suffixes."""
    graph = NodeGraph(
        {},
        None,
        using_namespace="agent-runtime",
    )

    assert graph.namespace == "agent-runtime"


def test_set_max_steps_is_fluent_and_updates_runtime_limit():
    """The compatibility setter returns the graph and changes its limit."""
    graph = NodeGraph({}, None)

    assert graph.set_max_steps(7) is graph
    assert graph._max_steps == 7


def test_apply_command_rejects_non_string_goto():
    """A direct apply_command call cannot route to a non-string target."""
    graph = NodeGraph({}, None)

    with pytest.raises(TypeError, match="Command.goto must be a string or None"):
        graph.apply_command(Command(goto=1), "node", graph.create_context({}))


def test_apply_command_rejects_plain_dict():
    """Only Command instances satisfy the direct runtime contract."""
    graph = NodeGraph({}, None)

    with pytest.raises(TypeError, match="must return a Command"):
        graph.apply_command({}, "node", graph.create_context({}))


class AutoMergeState(TypedDict):
    """State schema used to exercise annotated update behaviour."""

    values: Annotated[list[int], AutoMerge()]
    total: Annotated[int, AutoMerge]
    replaced: list[int]


class MissingAdd:
    """AutoMerge value whose addition protocol is deliberately unavailable."""

    __add__ = None


class UnsupportedAdd:
    """AutoMerge value that rejects the supplied update type."""

    def __add__(self, value):
        return NotImplemented


def test_apply_command_auto_increases_annotated_fields():
    """Marked existing fields call __add__; unmarked fields are replaced."""
    graph = NodeGraph(
        {},
        None,
        state_schema=AutoMergeState,
    )
    context = graph.create_context(
        {
            "values": [1],
            "total": 2,
            "replaced": [1],
        }
    )

    next_node = graph.apply_command(
        Command(
            update={
                "values": [2, 3],
                "total": 4,
                "replaced": [2],
            }
        ),
        "node",
        context,
    )

    assert context.state == {
        "values": [1, 2, 3],
        "total": 6,
        "replaced": [2],
    }
    assert next_node == _END


def test_apply_command_initializes_missing_auto_increase_field():
    """A marked field without a current value is assigned directly."""
    graph = NodeGraph(
        {},
        None,
        state_schema=AutoMergeState,
    )

    context = graph.create_context({})
    graph.apply_command(
        Command(update={"values": [1]}),
        "node",
        context,
    )

    assert context.state == {"values": [1]}


def test_apply_command_applies_command_list_in_order():
    """A command batch commits each update before applying the next one."""
    graph = NodeGraph(
        {},
        None,
        state_schema=AutoMergeState,
    )
    context = graph.create_context(
        {
            "values": [1],
            "total": 0,
            "replaced": [],
        }
    )

    next_node = graph.apply_command(
        [
            Command(update={"values": [2], "total": 3}),
            Command(
                update={"values": [3], "total": 4},
                goto="batch-target",
            ),
        ],
        "node",
        context,
    )

    assert context.state == {
        "values": [1, 2, 3],
        "total": 7,
        "replaced": [],
    }
    assert next_node == "batch-target"


def test_apply_command_collects_all_command_routes():
    """Commands contribute next targets while terminal commands add no work."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})

    next_node = graph.apply_command(
        [
            Command(goto="overridden-target"),
            Command(update={"value": 1}),
        ],
        "node",
        context,
    )

    assert context.state == {"value": 1}
    assert next_node == "overridden-target"


def test_apply_command_empty_command_list_finishes():
    """An empty batch preserves state and finishes."""
    graph = NodeGraph({}, None)
    original_state = {"nested": [1]}
    context = graph.create_context(original_state)

    committed_state = context.state
    next_node = graph.apply_command([], "node", context)

    assert context.state == original_state
    assert context.state is committed_state
    assert context.state["nested"] is committed_state["nested"]
    assert next_node == _END


@pytest.mark.parametrize(
    "result",
    [
        Command(update={"total": 3}),
        [Command(update={"total": 1}), Command(update={"total": 2})],
    ],
    ids=["command", "command-list"],
)
def test_apply_command_singleton_batch_keeps_one_node_result(result):
    """A list target takes one result per node, including a multi-command result."""
    graph = NodeGraph({}, None, state_schema=AutoMergeState)
    context = graph.create_context({"total": 0})

    assert graph.apply_command([result], ["node"], context) == _END
    assert context.state == {"total": 3}


def test_apply_command_singleton_empty_result_finishes():
    """An empty result contributes no next target."""
    graph = NodeGraph({"after": Node(lambda state: {})}, "after")
    context = graph.create_context({"value": 1})

    assert graph.apply_command([[]], ["node"], context) == _END
    assert context.state == {"value": 1}


@pytest.mark.parametrize("results", [Command(), [], [Command(), Command()]])
def test_apply_command_rejects_singleton_batch_result_count_mismatch(results):
    """A singleton batch requires exactly one result before any state is applied."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})

    with pytest.raises(TypeError, match="one result per node"):
        graph.apply_command(results, ["node"], context)
    assert context.state == {}


@pytest.mark.parametrize("node_name", ["node", ["node"]])
def test_apply_command_rejects_nested_node_result(node_name):
    """Batch grouping must not make nested lists legal inside a node result."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})
    result = [[Command(update={"value": 1})]]

    with pytest.raises(TypeError, match="must return a Command"):
        graph.apply_command(
            [result] if isinstance(node_name, list) else result, node_name, context
        )
    assert context.state == {}


def test_auto_increase_requires_callable_add_method():
    """A marked current value must expose a callable addition protocol."""
    graph = NodeGraph(
        {},
        None,
        state_schema=AutoMergeState,
    )

    with pytest.raises(TypeError, match="does not provide a callable __add__"):
        graph.apply_command(
            Command(update={"values": [1]}),
            "node",
            graph.create_context({"values": MissingAdd()}),
        )


def test_auto_increase_rejects_not_implemented_addition():
    """NotImplemented becomes a useful state merge error."""
    graph = NodeGraph(
        {},
        None,
        state_schema=AutoMergeState,
    )

    with pytest.raises(TypeError, match="could not add an update"):
        graph.apply_command(
            Command(update={"values": [1]}),
            "node",
            graph.create_context({"values": UnsupportedAdd()}),
        )


def test_replace_bypasses_auto_increase_and_is_unwrapped():
    """Reset forces assignment for both marked and ordinary fields."""
    graph = NodeGraph(
        {},
        None,
        state_schema=AutoMergeState,
    )
    context = graph.create_context(
        {
            "values": [1, 2],
            "replaced": [1, 2],
        }
    )

    graph.apply_command(
        Command(
            update={
                "values": Reset([3]),
                "replaced": Reset([4]),
            }
        ),
        "node",
        context,
    )

    assert context.state == {
        "values": [3],
        "replaced": [4],
    }


def test_replace_initializes_missing_auto_increase_field():
    """Reset is also unwrapped when the marked field has no old value."""
    graph = NodeGraph(
        {},
        None,
        state_schema=AutoMergeState,
    )

    context = graph.create_context({})
    graph.apply_command(
        Command(update={"values": [1]}),
        "node",
        context,
    )
    assert context.state == {"values": [1]}

    graph.apply_command(
        Command(update={"values": [2]}),
        "node",
        context,
    )
    assert context.state == {"values": [1, 2]}

    graph.apply_command(
        Command(update={"values": Reset([3])}),
        "node",
        context,
    )
    assert context.state == {"values": [3]}


def test_node_graph_rejects_non_class_state_schema():
    """Annotated metadata must come from a schema class."""
    with pytest.raises(
        TypeError,
        match="state_schema.*class or None",
    ):
        NodeGraph(
            {},
            None,
            state_schema={},
        )


@pytest.mark.asyncio
async def test_finish_and_fail_do_not_replace_completed_future():
    """Late terminal or failure events cannot overwrite an invocation result."""
    graph = NodeGraph({}, None)
    context = _bound_context(
        graph,
        "test-run",
        {"replacement": True},
    )
    completion = context.completion
    assert completion is not None
    completion.set_result({"original": True})

    graph._finish(context)
    graph._fail(context, RuntimeError("late failure"))

    assert await completion == {"original": True}


@pytest.mark.parametrize(
    "context",
    [
        None,
        {},
        {"run_id": 1, "state": {}, "completion": None},
        {"run_id": "run", "state": [], "completion": None},
        {"run_id": "run", "state": {}, "completion": None},
    ],
)
def test_is_active_context_rejects_malformed_event_context(context):
    """Graph listeners ignore events that do not carry a valid run context."""
    graph = NodeGraph({}, None)

    assert graph._is_active_context(context) is False


def test_is_active_context_rejects_owned_but_unbound_context():
    """Ownership alone is insufficient without invocation runtime fields."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})

    assert graph._is_active_context(context) is False


@pytest.mark.asyncio
async def test_is_active_context_rejects_aborted_context():
    """An aborted context is no longer eligible for node dispatch."""
    graph = NodeGraph({}, None)
    context = _bound_context(graph, "completed-run", {"value": 1})
    assert graph._is_active_context(context) is True

    context.abort()

    assert graph._is_active_context(context) is False


@pytest.mark.asyncio
async def test_abort_rejects_non_context():
    """Abort accepts a GraphContext rather than an identifier."""
    graph = NodeGraph({}, None)

    with pytest.raises(TypeError, match="requires a GraphContext"):
        await graph.abort("missing")


@pytest.mark.asyncio
async def test_abort_accepts_owned_pending_context():
    """A graph can abort its pending context without managing or running it."""
    graph = NodeGraph({}, None)

    context = graph.create_context({})
    await graph.abort(context)
    assert context.status == "aborted"
    graph.decompose(force=False)


@pytest.mark.asyncio
async def test_abort_rejects_context_for_inactive_run():
    """A retained or foreign context can abort an inactive graph run with no side effects."""
    graph = NodeGraph({}, None)
    context = _bound_context(graph, "inactive-run", {"checkpoint": 1})
    context.abort()

    await graph.abort(context)

    completion = context.completion
    assert completion is not None
    assert completion.done()


@pytest.mark.asyncio
async def test_abort_finishes_active_run_with_saved_snapshot():
    """Abort resolves completion and lifecycle immediately becomes inactive."""
    graph = NodeGraph({}, None)
    context = _bound_context(
        graph,
        "active-run",
        {"history": ["saved"]},
        steps=2,
    )
    completion = context.completion
    assert completion is not None
    context.target_node_name = "retry"
    context.take_a_snapshot()

    await graph.abort(context)

    result = await completion
    assert result == {"history": ["saved"]}
    assert result is not context.state
    assert graph._is_active_context(context) is False


@pytest.mark.asyncio
async def test_abort_same_run_twice():
    """A completed context can be aborted through the graph twice."""
    graph = NodeGraph({}, None)
    context = _bound_context(graph, "active-run", {})
    context.target_node_name = "retry"
    context.take_a_snapshot()

    await graph.abort(context)
    await graph.abort(context)


@pytest.mark.asyncio
async def test_invoke_rejects_non_context_argument():
    """The optional invocation context has an explicit runtime type contract."""
    graph = NodeGraph({}, None)

    with pytest.raises(TypeError, match="GraphContext or None"):
        await graph.invoke(graph_context={})


@pytest.mark.asyncio
async def test_cancel_during_initial_publication_aborts_the_accepted_context(monkeypatch):
    """Cancellation while publishing the first event aborts the accepted attempt."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})

    async def cancel_post(**kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(get_event_pipe(), "post_event", cancel_post)

    with pytest.raises(asyncio.CancelledError):
        await graph.invoke(graph_context=context)

    assert context.status == "aborted"
    assert context.is_bound is True


@pytest.mark.asyncio
async def test_post_failure_marks_running_context_failed(monkeypatch):
    """A setup failure after binding resolves the context as failed."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})
    error = RuntimeError("post failed")

    async def fail_post(node_name, bound_context):
        raise error

    monkeypatch.setattr(graph, "_post_next", fail_post)

    with pytest.raises(RuntimeError, match="post failed"):
        await graph.invoke(graph_context=context)

    assert context.status == "failed"
    assert context.completion is not None
    with pytest.raises(RuntimeError, match="post failed"):
        await context.completion
    await get_event_loop().stop()


@pytest.mark.asyncio
async def test_terminal_dispatch_error_preserves_an_aborted_result(monkeypatch):
    """A terminal dispatch failure cannot overwrite an aborted result."""
    graph = NodeGraph({"node": Node(lambda state: {})}, "node")
    context = graph.create_context({"saved": True})
    post_next = graph._post_next

    async def abort_then_fail(node_name, bound_context):
        if node_name == "node":
            await post_next(node_name, bound_context)
        else:
            bound_context.abort()
            raise RuntimeError("late terminal failure")

    monkeypatch.setattr(graph, "_post_next", abort_then_fail)

    assert await asyncio.wait_for(graph.invoke(graph_context=context), 1) == {"saved": True}
    assert context.status == "aborted"


@pytest.mark.asyncio
async def test_execute_node_ignores_error_after_attempt_becomes_stale():
    """A late node error cannot fail an already aborted attempt."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def fail_late(state):
        started.set()
        await release.wait()
        raise RuntimeError("late node failure")

    graph = NodeGraph(
        {"node": Node(fail_late)},
        "node",
    )
    context = _bound_context(graph, "stale-node", {"saved": True})
    completion = context.completion
    assert completion is not None

    execution = asyncio.create_task(graph._execute_node("node", context))
    await started.wait()
    context.abort()
    release.set()
    await execution

    assert await completion == {"saved": True}
    assert context.status == "aborted"
    assert context.context_snapshot
    assert context.context_snapshot[-1]["target_node_name"] == "node"


@pytest.mark.asyncio
async def test_post_next_propagates_event_pipe_failure(monkeypatch):
    """Posting failures propagate to the caller with the selected target retained."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})

    async def fail_post_event(**kwargs):
        raise RuntimeError("event pipe unavailable")

    monkeypatch.setattr(get_event_pipe(), "post_event", fail_post_event)

    with pytest.raises(RuntimeError, match="event pipe unavailable"):
        await graph._post_next(_END, context)

    assert context.target_node_name == _END


def test_decompose_unregisters_only_graph_handlers_and_is_idempotent():
    """Decomposition removes owned listeners while retaining graph plugins."""

    async def retained_plugin(event):
        pass

    subscribe("node")(retained_plugin)
    graph = NodeGraph(
        {"node": Node(lambda state: {})},
        "node",
        using_namespace="decompose",
    )
    graph_handler_names = set(graph._handlers)

    graph.decompose()
    graph.decompose()

    assert graph._decomposed is True
    assert graph._handlers == {}
    assert graph_handler_names.isdisjoint(get_handler_registry().registry)
    assert get_handler_registry().get_handlers_chain_for_event("node") == [
        retained_plugin.__name__
    ]

    unsubscribe(retained_plugin.__name__)


def test_dispatch_listener_registration_collision_does_not_leak_handler():
    """A dispatch-handler collision cannot leak graph listener state."""

    async def conflicting_handler(event):
        pass

    conflicting_handler.__name__ = get_graph_dispatch_name("rollback")
    subscribe("foreign")(conflicting_handler)

    with pytest.raises(EventHandlerAlreadyRegisteredError):
        NodeGraph(
            {"node": Node(lambda state: {})},
            "node",
            using_namespace="rollback",
        )

    assert get_handler_registry().get_handler(
        conflicting_handler.__name__
    ).subscribe == ["foreign"]
    assert "rollback" not in namespace_set

    unsubscribe(conflicting_handler.__name__)
    graph = NodeGraph({}, None, using_namespace="rollback")
    assert graph.namespace in namespace_set


@pytest.mark.asyncio
async def test_decomposed_graph_rejects_new_invocation():
    """Every public business interface rejects an invalidated graph."""
    graph = NodeGraph({}, None)
    context = graph.create_context({})
    graph.decompose()

    with pytest.raises(RuntimeError, match="has been decomposed"):
        await graph.invoke({})
    with pytest.raises(RuntimeError, match="has been decomposed"):
        await anext(graph.stream({}))
    with pytest.raises(RuntimeError, match="has been decomposed"):
        await graph.abort(context)
    with pytest.raises(RuntimeError, match="has been decomposed"):
        graph.set_max_steps(1)


@pytest.mark.asyncio(loop_scope="session")
async def test_decompose_rejects_active_invocation():
    """Listeners remain installed until an active invocation completes."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocking_node(state):
        started.set()
        await release.wait()
        return {"finished": True}

    graph = NodeGraph(
        {"node": Node(blocking_node)},
        "node",
        using_namespace="active-decompose",
    )
    invocation = asyncio.create_task(graph.invoke({}))
    await started.wait()

    with pytest.raises(RuntimeError, match="contexts are running"):
        graph.decompose(force=False)

    assert graph._decomposed is False
    assert graph._handlers

    release.set()
    assert await invocation == {"finished": True}
    graph.decompose()
    await get_event_loop().stop()
