"""Explicit construction, lifecycle transitions and pure synchronous getters."""

import asyncio
import importlib
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from apixis import (
    ApixisCore,
    CoreClosedError,
    CoreState,
    GraphManager,
    close_core,
    create_core,
    get_core,
    get_event_loop,
    get_event_pipe,
    get_event_registry,
    get_event_runtime_status,
    get_handler_registry,
    post_event,
    start_core,
    stop_core,
    subscribe,
)
from apixis.core.event import factory


@pytest_asyncio.fixture
async def fresh_core(monkeypatch):
    monkeypatch.setattr(factory, "_core", None)
    yield
    if factory._core is not None:
        await factory._core.close_core()


@pytest.mark.parametrize("module_name", ["apixis", "apixis.core", "apixis.core.event"])
def test_lifecycle_exports(module_name):
    module = importlib.import_module(module_name)
    for name in (
        "ApixisCore",
        "CoreState",
        "create_core",
        "get_core",
        "start_core",
        "stop_core",
        "close_core",
    ):
        assert getattr(module, name) is globals()[name]
        assert name in module.__all__
    assert not hasattr(module, "EventCore")
    assert not any(name.startswith("aget_") for name in module.__all__)


@pytest.mark.parametrize(
    "getter",
    [
        get_core,
        get_event_registry,
        get_event_pipe,
        get_handler_registry,
        get_event_loop,
    ],
)
def test_getters_require_explicit_creation(fresh_core, getter):
    with pytest.raises(RuntimeError, match="create_core"):
        getter()
    assert factory._core is None


async def test_start_requires_creation_but_stop_and_close_are_safe(fresh_core):
    with pytest.raises(RuntimeError, match="create_core"):
        await start_core()
    await stop_core()
    await close_core()
    assert factory._core is None


def test_create_is_synchronous_idempotent_and_does_not_start(fresh_core):
    core = create_core()
    assert isinstance(core, ApixisCore)
    assert core.state is CoreState.CREATED
    assert create_core() is core
    assert get_core() is core
    assert not core.event_loop.is_running
    assert not core.event_pipe.closed
    assert core.event_loop._event_consumer_task is None
    assert core.event_pipe._blocked_puts == set()


async def test_getters_neither_create_tasks_nor_start_consumption(
    fresh_core, monkeypatch
):
    core = create_core()
    startup = AsyncMock()
    monkeypatch.setattr(core.event_loop, "start", startup)
    before = asyncio.all_tasks()
    assert get_event_registry() is core.event_registry
    assert get_event_pipe() is core.event_pipe
    assert get_handler_registry() is core.handler_registry
    assert get_event_loop() is core.event_loop
    assert asyncio.all_tasks() == before
    await asyncio.sleep(0)
    startup.assert_not_awaited()
    assert core.state is CoreState.CREATED


def test_creation_uses_configured_cache_size(fresh_core, monkeypatch):
    core_module = importlib.import_module("apixis.core.event.core")
    monkeypatch.setattr(core_module, "HANDLER_CHAIN_CACHE_SIZE", 3)
    core = create_core()
    assert core.handler_registry._cache_size == 3


async def test_start_stop_and_getters_preserve_components_and_queued_events(
    fresh_core, wait_for_dispatch
):
    core = create_core()
    components = (
        core.event_registry,
        core.event_pipe,
        core.handler_registry,
        core.event_loop,
    )
    seen = []

    @subscribe("queued.*")
    async def receive(event):
        seen.append(event.event_name)

    await post_event(event_name="queued.before_start")
    assert core.event_pipe.qsize() == 1
    assert seen == []
    await asyncio.gather(*(start_core() for _ in range(8)))
    assert core.state is CoreState.RUNNING
    await wait_for_dispatch(core.event_loop)
    await asyncio.gather(*(stop_core() for _ in range(8)))
    assert core.state is CoreState.STOPPED
    await post_event(event_name="queued.paused")
    for getter in (
        get_core,
        get_event_registry,
        get_event_pipe,
        get_handler_registry,
        get_event_loop,
    ):
        getter()
    await asyncio.sleep(0)
    assert seen == ["queued.before_start"]
    assert core.event_pipe.qsize() == 1
    assert core.state is CoreState.STOPPED
    await start_core()
    await wait_for_dispatch(core.event_loop)
    assert seen == ["queued.before_start", "queued.paused"]
    assert components == (
        core.event_registry,
        core.event_pipe,
        core.handler_registry,
        core.event_loop,
    )


@pytest.mark.parametrize(
    "failure", [RuntimeError("start failed"), asyncio.CancelledError()]
)
async def test_failed_start_can_retry_without_recreating_components(
    fresh_core, monkeypatch, failure
):
    core = create_core()
    original = core.event_loop.start
    monkeypatch.setattr(core.event_loop, "start", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)):
        await start_core()
    assert core.state is CoreState.STOPPED
    assert get_core() is core
    monkeypatch.setattr(core.event_loop, "start", original)
    await start_core()
    assert core.state is CoreState.RUNNING


async def test_start_waits_for_a_concurrent_stop(fresh_core, monkeypatch):
    core = create_core()
    await start_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = core.event_loop.stop

    async def stop():
        entered.set()
        await release.wait()
        await original()

    monkeypatch.setattr(core.event_loop, "stop", stop)
    stopping = asyncio.create_task(stop_core())
    await entered.wait()
    starting = asyncio.create_task(start_core())
    await asyncio.sleep(0)
    assert not starting.done()
    release.set()
    await asyncio.gather(stopping, starting)
    assert core.state is CoreState.RUNNING


async def test_consumer_exit_updates_state_and_requires_explicit_restart(fresh_core):
    core = create_core()
    await start_core()
    task = core.event_loop._event_consumer_task
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert core.state is CoreState.STOPPED
    get_event_loop()
    assert core.event_loop._event_consumer_task is None
    await start_core()
    assert core.state is CoreState.RUNNING


async def test_close_is_terminal_and_create_replaces_only_closed_instances(fresh_core):
    core = create_core()
    await start_core()
    await asyncio.gather(*(close_core() for _ in range(8)))
    assert core.state is CoreState.CLOSED
    assert get_core() is core
    await stop_core()
    with pytest.raises(CoreClosedError):
        await start_core()
    replacement = create_core()
    assert replacement is not core
    assert replacement.state is CoreState.CREATED
    assert replacement.event_pipe is not core.event_pipe
    assert core.event_pipe.closed
    assert core.handler_registry.closed


async def test_graph_invocation_never_implicitly_starts_consumption(fresh_core):
    core = create_core()
    graph = (
        GraphManager()
        .add_node(lambda state: {"value": state["value"] + 1}, "increment")
        .compile_graph("increment")
    )
    invocation = asyncio.create_task(graph.invoke({"value": 1}))
    try:
        await asyncio.sleep(0)
        assert not invocation.done()
        assert not core.event_loop.is_running
        await start_core()
        assert await asyncio.wait_for(invocation, 1) == {"value": 2}
        assert get_event_runtime_status().state is CoreState.RUNNING
    finally:
        graph.decompose()
        await asyncio.gather(invocation, return_exceptions=True)
