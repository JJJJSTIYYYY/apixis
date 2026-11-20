"""Graph lifecycle cleanup delegates membership to the handler group index."""

from unittest.mock import AsyncMock, patch

import pytest

from apixis import (
    ApixEventHandler, GLOBALNS, NodeGraph, get_graph_interrupted_name,
    get_handler, get_handler_meta, get_handler_registry,
    interrupted_hook, release_namespace, subscribe, unsubscribe,
)


@pytest.mark.parametrize("namespace", [None, GLOBALNS, "global", "agent"])
def test_graph_listeners_share_a_group_distinct_from_default(namespace):
    graph = NodeGraph({}, None, using_namespace=namespace)
    event_name = get_graph_interrupted_name(graph)
    registry = get_handler_registry()
    [fallback_name] = registry.get_handlers_chain_for_event(event_name)

    @graph.add_interrupted_hook
    async def owned_hook(block):
        pass

    @interrupted_hook(graph.namespace)
    async def independent_hook(block):
        pass

    assert graph.handler_group != "global"
    for name in (graph.dispatch_name, fallback_name, event_name):
        assert get_handler_meta(name)["group"] == graph.handler_group
    assert get_handler_meta("independent_hook")["group"] == "global"
    unsubscribe(group="global")
    assert registry.get_handlers_chain_for_event(event_name) == [event_name, fallback_name]
    assert get_handler(graph.dispatch_name) is not None
    with patch.object(registry, "unregister_group", wraps=registry.unregister_group) as remove:
        graph.decompose()
        remove.assert_called_once_with(graph.handler_group, missing_ok=True)
    assert graph.handler_group not in registry._group_handlers
    assert not registry.registry


def test_explicit_group_plugins_are_cleaned_with_graph():
    graph = NodeGraph({}, None)
    other = NodeGraph({}, None)
    registry = get_handler_registry()

    @subscribe(graph.dispatch_name, group=graph.handler_group)
    async def owned_plugin(event):
        pass

    @subscribe(graph.dispatch_name)
    async def independent_plugin(event):
        pass

    other_dispatch = get_handler(other.dispatch_name)
    graph.decompose()
    assert get_handler("owned_plugin") is None
    assert get_handler("independent_plugin") is not None
    assert registry.get_handlers_chain_for_event(graph.dispatch_name) == ["independent_plugin"]
    assert get_handler(other.dispatch_name) is other_dispatch
    assert graph.handler_group not in registry._group_handlers


@pytest.mark.parametrize("replace_hook", [False, True])
def test_foreign_same_name_replacement_survives_graph_decomposition(replace_hook):
    graph = NodeGraph({}, None)
    name = graph.dispatch_name
    if replace_hook:
        graph.add_interrupted_hook(AsyncMock(), name="hook")
        name = "hook"
    foreign = ApixEventHandler(AsyncMock(), name=name).register("foreign")
    graph.decompose()
    assert get_handler(name) is foreign
    assert get_handler_registry().get_handlers_chain_for_event("foreign") == [name]


def test_late_cleanup_cannot_remove_new_graph_using_same_namespace():
    old = NodeGraph({}, None, using_namespace="reused")
    old.add_interrupted_hook(AsyncMock(), name="old.hook")
    registry = get_handler_registry()
    event_name = get_graph_interrupted_name(old)
    fallback_name = next(
        name for name in registry.get_handlers_chain_for_event(event_name)
        if name != "old.hook"
    )
    # Registry-only release leaves the old graph live, with an owned hook.
    release_namespace(old, decompose_immediately=False)
    unsubscribe(old.dispatch_name)
    unsubscribe(fallback_name)
    new = NodeGraph({}, None, using_namespace=old.namespace)
    new.add_interrupted_hook(AsyncMock(), name="new.hook")
    new_dispatch = get_handler(new.dispatch_name)
    new_chain = registry.get_handlers_chain_for_event(event_name)
    assert old.handler_group != new.handler_group
    old.decompose()
    assert get_handler("old.hook") is None
    assert get_handler("new.hook") is not None
    assert get_handler(new.dispatch_name) is new_dispatch
    assert new_chain == ["old.hook", "new.hook", fallback_name]
    assert registry.get_handlers_chain_for_event(event_name) == ["new.hook", fallback_name]
    assert old.handler_group not in registry._group_handlers


def test_failed_graph_initialization_removes_all_group_members(monkeypatch):
    registry = get_handler_registry()
    retained = ApixEventHandler(AsyncMock(), name="retained").register("other")
    before_groups = {group: names.copy() for group, names in registry._group_handlers.items()}
    before_memberships = registry._handler_groups.copy()
    register = NodeGraph._register_interrupted_handler

    def register_then_fail(graph):
        register(graph)
        ApixEventHandler(AsyncMock(), name="partial.plugin", group=graph.handler_group).register("other")
        raise RuntimeError("listener setup failed")

    monkeypatch.setattr(NodeGraph, "_register_interrupted_handler", register_then_fail)
    with pytest.raises(RuntimeError, match="listener setup failed"):
        NodeGraph({}, None, using_namespace="partial")
    assert registry.registry == {"retained": retained}
    assert registry._group_handlers == before_groups
    assert registry._handler_groups == before_memberships


def test_replacement_gets_its_own_group_and_repeated_cleanup_is_harmless():
    old = NodeGraph({}, None, using_namespace="replacement")
    old.add_interrupted_hook(AsyncMock(), name="old.hook")
    old_group = old.handler_group
    new = NodeGraph({}, None, using_namespace=old.namespace, exist_ok=True)
    new.add_interrupted_hook(AsyncMock(), name="new.hook")
    registry = get_handler_registry()
    dispatch = get_handler(new.dispatch_name)
    assert old_group != new.handler_group
    assert old_group not in registry._group_handlers
    assert get_handler("old.hook") is None
    old.decompose()
    release_namespace(old)
    assert get_handler(new.dispatch_name) is dispatch
    assert get_handler("new.hook").group == new.handler_group
    new.decompose()
    assert new.handler_group not in registry._group_handlers
