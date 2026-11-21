"""Application entry points cannot expose mutable runtime registration records."""

import asyncio
import importlib

import pytest

from apixis import (
    CoreState, create_core, get_core, get_handler_meta, is_registered,
    post_and_wait, subscribe, unsubscribe,
)
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.event.factory import get_handler_registry


INTERNAL_EVENT_NAMES = (
    "ApixisCore", "ApixEventHandler", "ApixEventLoop", "ApixEventPipe",
    "ApixEventRegistry", "ApixHandlerRegistry", "get_handler",
    "get_event_registry", "get_event_pipe", "get_handler_registry", "get_event_loop",
)
INTERNAL_GRAPH_NAMES = (
    "GRAPH_DISPATCH", "GRAPH_INTERRUPTED", "GLOBALNS", "namespace_set",
    "acquire_namespace", "release_namespace", "validate_namespace",
    "get_graph_namespace", "copy_state", "parse_state_schema", "validate_graph_definition",
)


@pytest.mark.parametrize("module_name", ["apixis", "apixis.core", "apixis.core.event"])
def test_event_implementation_is_absent_from_application_facades(module_name):
    module = importlib.import_module(module_name)
    for name in INTERNAL_EVENT_NAMES:
        assert name not in module.__all__
        assert not hasattr(module, name)
    for name in ("create_core", "get_core", "start_core", "stop_core", "close_core",
                 "post_event", "post_and_wait", "subscribe", "unsubscribe",
                 "get_handler_meta", "get_event_runtime_status", "expect_event", "wait_event"):
        assert name in module.__all__ and callable(getattr(module, name))


@pytest.mark.parametrize("module_name", ["apixis", "apixis.core", "apixis.core.graph"])
def test_graph_management_internals_are_absent_from_application_facades(module_name):
    module = importlib.import_module(module_name)
    for name in INTERNAL_GRAPH_NAMES:
        assert name not in module.__all__
        assert not hasattr(module, name)
    for name in ("GraphManager", "NodeGraph", "Node", "BaseNode", "ParallelNode",
                 "Command", "GraphContext", "get_stream_writer", "interrupt",
                 "get_graph_dispatch_name", "get_graph_interrupted_name"):
        assert name in module.__all__ and callable(getattr(module, name))


@pytest.mark.parametrize("name", [
    "id", "name", "group", "subscribe", "filter_event", "priority", "between_handlers",
    "core_func", "on_accepted", "on_has_error", "on_error", "on_cancelled",
    "stop_when_error", "time_out", "background",
])
def test_handler_fields_have_no_public_writable_alias(name):
    async def receive(event):
        pass

    handler = ApixEventHandler(receive, name="original").register("alpha")
    registry = get_handler_registry()
    assert registry.get_handlers_chain_for_event("alpha") == ["original"]
    assert not hasattr(handler, name)
    with pytest.raises(AttributeError):
        setattr(handler, name, "renamed")
    handler.register("beta")
    handler.unregister()
    assert registry.get_handlers_for_event("alpha") == []
    assert registry.get_handlers_for_event("beta") == []


def test_core_handle_exposes_state_and_callbacks_without_public_components():
    core = get_core()
    assert create_core() is core
    assert core.state is CoreState.RUNNING
    for name in ("event_registry", "event_pipe", "handler_registry", "event_loop"):
        assert not hasattr(core, name)
        with pytest.raises(AttributeError):
            setattr(core, name, object())
    assert not hasattr(core, "create_core")
    def callback(task):
        pass

    core.add_close_task_done_callback(callback)
    assert core.remove_close_task_done_callback(callback) == 1


async def test_public_subscription_options_preserve_all_handler_notifications():
    notifications = []

    async def accepted(event):
        notifications.append("accepted")

    async def upstream_error(event):
        notifications.append("upstream")

    async def own_error(event, error):
        assert isinstance(error, ValueError)
        notifications.append("error")

    async def cancelled(event):
        notifications.append("cancelled")

    @subscribe("public.job", priority=10)
    async def prepare(event):
        if event.context == "upstream":
            raise ValueError("upstream failure")
        if event.context == "accepted":
            event.accept()

    @subscribe("public.job", name="worker", group="workers", priority=3,
               time_out=1, on_accepted=accepted, on_has_error=upstream_error,
               on_error=own_error, on_cancelled=cancelled)
    async def receive(event):
        if event.context == "error":
            raise ValueError("own failure")
        if event.context == "cancelled":
            raise asyncio.CancelledError

    receive.__name__ = "changed_function_name"
    meta = get_handler_meta("worker")
    assert meta["name"] == "worker" and meta["group"] == "workers"
    assert meta["priority"] == 3 and meta["time_out"] == 1
    meta["name"] = "fake"
    meta["subscribe"].append("fake")
    assert get_handler_meta("worker")["subscribe"] == ["public.job"]
    for context in ("error", "upstream", "accepted", "cancelled"):
        await post_and_wait(event_name="public.job", context=context)
    assert notifications == ["error", "upstream", "accepted", "cancelled"]
    unsubscribe(group="workers")
    assert not is_registered("worker")
    assert get_handler_meta("worker") is None


@pytest.mark.parametrize("options", [
    {"name": ""}, {"name": 3}, {"on_error": object()}, {"on_cancelled": object()},
    {"on_has_error": object()}, {"on_accepted": object()},
])
def test_invalid_public_subscription_options_preserve_existing_registration(options):
    async def receive(event):
        pass

    subscribe("old", name="worker")(receive)
    with pytest.raises((TypeError, ValueError)):
        subscribe("new", **({"name": "worker"} | options))(receive)
    assert get_handler_meta("worker")["subscribe"] == ["old"]
