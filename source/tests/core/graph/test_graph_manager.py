"""Unit tests for graph construction and transition validation."""

import asyncio
import math

import pytest

from apixis.core.graph.base import _END

from apixis.core.event.factory import get_handler_registry
from apixis.core.graph import (
    GLOBALNS,
    GRAPH_DISPATCH,
    Command,
    GraphManager,
    get_graph_dispatch_name,
    namespace_set,
    release_namespace,
)
from apixis.core.graph.base import (
    _namespace_graphs,
)


def source(state):
    return {}


def target(state):
    return {}


def test_add_node_and_add_nodes_are_fluent():
    """Builder registration methods return the same manager instance."""
    manager = GraphManager()

    assert manager.add_node(source) is manager
    assert manager.add_nodes([target]) is manager


@pytest.mark.parametrize(
    ("timeout", "expected"),
    [
        (None, None),
        (0, None),
        (-1, None),
        (1, 1.0),
        (1.5, 1.5),
    ],
)
def test_add_node_stores_normalised_timeout(timeout, expected):
    """Node owns its timeout and non-positive values mean unlimited."""
    manager = GraphManager().add_node(source, timeout=timeout)

    assert manager._nodes["source"].timeout == expected


@pytest.mark.parametrize("timeout", [True, "1", object()])
def test_add_node_rejects_non_numeric_timeout_without_registering_node(timeout):
    """Invalid timeout types leave the manager unchanged."""
    manager = GraphManager()

    with pytest.raises(TypeError, match="timeout must be a number or None"):
        manager.add_node(source, timeout=timeout)

    assert manager.has_node("source") is False


@pytest.mark.parametrize("timeout", [math.inf, -math.inf, math.nan])
def test_add_node_rejects_non_finite_timeout_without_registering_node(timeout):
    """NaN and infinity cannot represent executable deadlines."""
    manager = GraphManager()

    with pytest.raises(ValueError, match="timeout must be finite"):
        manager.add_node(source, timeout=timeout)

    assert manager.has_node("source") is False


@pytest.mark.parametrize("reserved_name", [_END])
def test_reserved_node_names_are_rejected(reserved_name):
    """User nodes cannot use the internal terminal marker as their name."""
    with pytest.raises(ValueError, match="reserved graph node name"):
        GraphManager().add_node(source, reserved_name)


def test_duplicate_node_name_is_rejected():
    """Every user node name must be unique."""
    manager = GraphManager().add_node(source)

    with pytest.raises(ValueError, match="already registered"):
        manager.add_node(target, "source")


def test_compile_retains_timeout_on_node():
    """The compiled graph reads timeout policy from its node."""
    graph = (
        GraphManager()
        .add_node(source, timeout=2.5)
        .compile_graph(entry_point="source")
    )

    assert graph._nodes["source"].timeout == 2.5


@pytest.mark.parametrize(
    ("options", "snapshot_count"),
    [({}, 2), ({"no_snapshot": False}, 2), ({"no_snapshot": True}, 0)],
    ids=["default", "snapshots", "no-snapshots"],
)
async def test_compile_snapshot_policy_preserves_result_and_state_isolation(
    options, snapshot_count,
):
    """Disabling snapshots preserves copying at node and caller boundaries."""
    initial = {"payload": {"items": ["input"]}}
    update = {"items": ["committed"]}

    def first(state):
        state["payload"]["items"].append("first-node-only")
        return Command(update={"progress": update}, goto="second")

    def second(state):
        assert state["payload"] == {"items": ["input"]}
        state["progress"]["items"].append("second-node-only")
        return {"done": True}

    graph = (
        GraphManager()
        .add_nodes([first, second])
        .compile_graph(entry_point="first", **options)
    )
    context = graph.create_context(initial)
    result = await asyncio.wait_for(graph.invoke(graph_context=context), timeout=1)

    assert result == {
        "payload": {"items": ["input"]},
        "progress": {"items": ["committed"]},
        "done": True,
    }
    assert initial == {"payload": {"items": ["input"]}}
    assert update == {"items": ["committed"]}
    assert context.steps == 2
    assert len(context.context_snapshot) == snapshot_count
    result["progress"]["items"].append("caller-only")
    assert context.state["progress"] == {"items": ["committed"]}


async def test_compile_without_snapshots_aborts_at_committed_state():
    """Without a checkpoint, abort returns live state and recovery is unavailable."""
    started, release_node = asyncio.Event(), asyncio.Event()

    def commit(state):
        return Command(update={"items": ["committed"]}, goto="waiting")

    async def waiting(state):
        state["items"].append("node-only")
        started.set()
        await release_node.wait()
        return {"items": ["late-update"]}

    graph = (
        GraphManager()
        .add_nodes([commit, waiting])
        .compile_graph(entry_point="commit", no_snapshot=True)
    )
    context = graph.create_context({"items": ["initial"]})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        context.abort()
        result = await asyncio.wait_for(invocation, timeout=1)

        assert result == {"items": ["committed"]}
        assert context.status == "aborted"
        assert context.context_snapshot == []
        with pytest.raises(RuntimeError, match="without a snapshot"):
            graph.restore_context(context)
        result["items"].append("caller-only")
        assert context.state == {"items": ["committed"]}
    finally:
        release_node.set()
        if not invocation.done():
            invocation.cancel()
        await asyncio.gather(invocation, return_exceptions=True)


@pytest.mark.parametrize(
    ("using_namespace", "expected"),
    [
        (GLOBALNS, GLOBALNS),
        ("agent-runtime", "agent-runtime"),
    ],
)
def test_compile_forwards_listener_namespace(using_namespace, expected):
    """GraphManager exposes NodeGraph's listener namespace selection."""
    graph = (
        GraphManager()
        .add_node(source)
        .compile_graph(entry_point="source", using_namespace=using_namespace)
    )

    assert graph.namespace == expected
    assert expected in namespace_set
    assert _namespace_graphs[expected] is graph


@pytest.mark.parametrize("using_namespace", [None, ""])
def test_compile_generates_namespace_for_empty_value(using_namespace):
    """GraphManager forwards empty values as requests for unique namespaces."""
    first = (
        GraphManager()
        .add_node(source)
        .compile_graph(entry_point="source", using_namespace=using_namespace)
    )
    second = (
        GraphManager()
        .add_node(target)
        .compile_graph(entry_point="target", using_namespace=using_namespace)
    )

    assert first.namespace != second.namespace
    assert first.namespace.isdecimal()
    assert second.namespace.isdecimal()
    assert _namespace_graphs[first.namespace] is first
    assert _namespace_graphs[second.namespace] is second


def test_compile_rejects_occupied_namespace_by_default():
    """An existing compiled graph is retained when replacement is disabled."""
    first_graph = (
        GraphManager()
        .add_node(source)
        .compile_graph(entry_point="source", using_namespace="occupied")
    )

    with pytest.raises(ValueError, match="already in use"):
        (
            GraphManager()
            .add_node(target)
            .compile_graph(entry_point="target", using_namespace="occupied")
        )

    assert first_graph._decomposed is False
    assert _namespace_graphs["occupied"] is first_graph


@pytest.mark.parametrize("namespace", [GLOBALNS, "replaceable"])
def test_compile_exist_ok_decomposes_and_replaces_original_graph(namespace):
    """Replacement unregisters the old listeners before installing new ones."""
    first_graph = (
        GraphManager()
        .add_node(source, "shared")
        .compile_graph(entry_point="shared", using_namespace=namespace)
    )
    first_callbacks = {
        get_handler_registry().get_handler(handler_name).core_func
        for handler_name in get_handler_registry().get_handlers_chain_for_event(
            first_graph.dispatch_name
        )
    }

    replacement = (
        GraphManager()
        .add_node(target, "shared")
        .compile_graph(entry_point="shared",
            using_namespace=namespace,
            exist_ok=True,
        )
    )
    replacement_callbacks = {
        get_handler_registry().get_handler(handler_name).core_func
        for handler_name in get_handler_registry().get_handlers_chain_for_event(
            replacement.dispatch_name
        )
    }

    assert first_graph._decomposed is True
    assert first_graph._handlers == {}
    assert first_callbacks.isdisjoint(replacement_callbacks)
    assert len(replacement_callbacks) == 1
    assert namespace_set == {namespace}
    assert _namespace_graphs[namespace] is replacement

    release_namespace(first_graph)
    assert namespace_set == {namespace}
    assert _namespace_graphs[namespace] is replacement


def test_decompose_releases_namespace_by_contextmanager():
    graph = (
        GraphManager()
        .add_node(source)
        .compile_graph(entry_point="source", using_namespace="reusable")
    )

    assert "reusable" in namespace_set
    assert "reusable" in _namespace_graphs

    with graph:
        pass

    assert "reusable" not in namespace_set
    assert "reusable" not in _namespace_graphs
    assert graph._decomposed is True


def test_decompose_releases_namespace_for_later_compile():
    """Direct decomposition keeps GraphManager's global index synchronized."""
    first_graph = (
        GraphManager()
        .add_node(source)
        .compile_graph(entry_point="source", using_namespace="reusable")
    )

    assert "reusable" in namespace_set
    assert "reusable" in _namespace_graphs

    first_graph.decompose()

    assert "reusable" not in namespace_set
    assert "reusable" not in _namespace_graphs

    replacement = (
        GraphManager()
        .add_node(target)
        .compile_graph(entry_point="target", using_namespace="reusable")
    )
    assert _namespace_graphs["reusable"] is replacement


def test_none_and_empty_string_create_independent_namespaces():
    """Empty namespace requests do not force unrelated graphs to coordinate."""
    first = (
        GraphManager()
        .add_node(source)
        .compile_graph(entry_point="source", using_namespace=None)
    )

    second = (
        GraphManager()
        .add_node(target)
        .compile_graph(entry_point="target", using_namespace="")
    )

    assert first.namespace != second.namespace
    assert namespace_set == {first.namespace, second.namespace}


@pytest.mark.parametrize("namespace", ["*", "agent?", "agent[ab]", "agent[", "agent]"])
@pytest.mark.parametrize("exist_ok", [False, True])
def test_compile_rejects_glob_namespace_without_changing_registry(namespace, exist_ok):
    """Invalid names cannot acquire ownership or replace existing listeners."""
    manager = GraphManager().add_node(source)
    original = manager.compile_graph(entry_point="source")
    handlers = dict(get_handler_registry().registry)

    with pytest.raises(ValueError, match="glob characters"):
        manager.compile_graph(entry_point="source", using_namespace=namespace, exist_ok=exist_ok)

    assert _namespace_graphs == {original.namespace: original}
    assert get_handler_registry().registry == handlers
    assert original._decomposed is False


@pytest.mark.parametrize("namespace", [None, "", GLOBALNS])
def test_global_event_name_and_ownership_check_use_canonical_namespace(namespace):
    """Global aliases share event naming and the strict ownership check."""
    event_name = f"{GRAPH_DISPATCH}_{GLOBALNS}"
    assert get_graph_dispatch_name(namespace) == event_name
    with pytest.raises(KeyError, match="<global>"):
        get_graph_dispatch_name(namespace, missing_ok=False)

    graph = GraphManager().compile_graph(entry_point=None, using_namespace=GLOBALNS)
    assert (
        get_graph_dispatch_name(
            namespace,
            missing_ok=False,
        )
        == graph.dispatch_name
        == event_name
    )
