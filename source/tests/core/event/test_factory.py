"""Public core lifecycle, dependency isolation, and startup regressions."""

import asyncio

import pytest

from apixis.core.event import (
    ApixEvent,
    ApixEventHandler,
    ApixEventLoop,
    ApixEventPipe,
    ApixEventRegistry,
    ApixHandlerRegistry,
    EventType,
    start_core,
    get_event_loop,
    get_event_pipe,
    get_event_registry,
    get_handler_registry,
    subscribe,
    unsubscribe,
)
from apixis.core.event import factory


@pytest.fixture
async def fresh_core(monkeypatch):
    """Keep test-owned factory components separate from the shared test core."""
    monkeypatch.setattr(factory, "_core", None)
    yield
    core = factory._core
    if core is not None:
        if core.start_task is not None:
            await asyncio.gather(core.start_task, return_exceptions=True)
        await core.event_loop.stop()
        await core.event_pipe.stop()


def components():
    return (
        get_event_registry(), get_event_pipe(),
        get_handler_registry(), get_event_loop(),
    )


def event(name="factory.event"):
    return ApixEvent(name, EventType.INFO, name, None, 0)


def test_default_core_uses_configured_chain_cache_limit(fresh_core, monkeypatch):
    monkeypatch.setattr(factory, "HANDLER_CHAIN_CACHE_SIZE", 3)
    handlers = get_handler_registry()
    for index in range(12):
        assert handlers.get_handlers_chain_for_event(f"cache.{index}") == []
    assert list(handlers.cached_chain) == ["cache.9", "cache.10", "cache.11"]


@pytest.mark.parametrize("exit_mode", ["before_run", "waiting", "failure"])
@pytest.mark.parametrize("async_getter", [False, True])
async def test_getters_restart_an_exited_consumer(
    fresh_core, monkeypatch, exit_mode, async_getter,
):
    """Consumer completion clears startup state without an explicit stop call."""
    await start_core()
    loop = get_event_loop()
    pipe = get_event_pipe()
    consumer = loop._event_consumer_task
    if exit_mode == "failure":
        get = pipe.get

        async def fail_once():
            monkeypatch.setattr(pipe, "get", get)
            raise RuntimeError("consumer input failed")

        monkeypatch.setattr(pipe, "get", fail_once)
    else:
        if exit_mode == "waiting":
            await asyncio.sleep(0)
        consumer.cancel()
    await asyncio.gather(consumer, return_exceptions=True)

    if async_getter:
        restarted_pipe = await factory.aget_event_pipe()
    else:
        restarted_pipe = get_event_pipe()
    assert restarted_pipe is pipe

    received = asyncio.Event()

    @subscribe("factory.restarted")
    async def receive(message):
        received.set()

    try:
        await pipe.post_event(event_type=EventType.INFO, event_name="factory.restarted")
        await asyncio.wait_for(received.wait(), 1)
    finally:
        unsubscribe(receive.__name__)


def test_getters_construct_without_running_asyncio(fresh_core):
    first = components()
    assert all(a is b for a, b in zip(first, components()))
    assert not get_event_loop()._started
    get_event_pipe().put_nowait(event())
    assert get_event_pipe().get_nowait().event_name == "factory.event"
    get_event_pipe().task_done()


async def test_construction_order_and_no_recursive_getters(fresh_core, monkeypatch):
    order = []
    with monkeypatch.context() as patch:
        for name in ("ApixEventRegistry", "ApixEventPipe", "ApixHandlerRegistry", "ApixEventLoop"):
            constructor = getattr(factory, name)

            def construct(*args, _name=name, _constructor=constructor, **kwargs):
                order.append(_name)
                return _constructor(*args, **kwargs)

            patch.setattr(factory, name, construct)

        def unexpected_getter():
            raise AssertionError("Construction and startup must use injected dependencies")

        for name in ("get_event_registry", "get_event_pipe", "get_handler_registry", "get_event_loop"):
            patch.setattr(factory, name, unexpected_getter)
        await start_core()
    assert order == ["ApixEventRegistry", "ApixEventPipe", "ApixHandlerRegistry", "ApixEventLoop"]
    assert get_event_loop()._started


async def test_repeated_create_preserves_components_subscriptions_and_pending_events(fresh_core, wait_for_dispatch):
    first = components()
    calls = []

    @subscribe("factory.*")
    async def receive(message):
        calls.append(message.event_name)

    try:
        get_event_pipe().put_nowait(event("factory.before_start"))
        await start_core()
        workers = get_event_loop()._event_consumer_task
        await start_core()
        assert workers == get_event_loop()._event_consumer_task
        await wait_for_dispatch(get_event_loop())
        registry, pipe, handlers, loop = first
        await loop.stop()
        await pipe.stop()
        await pipe.put(event("factory.after_stop"))
        assert not loop._started
        await start_core()
        await wait_for_dispatch(get_event_loop())
        assert all(a is b for a, b in zip(first, components()))
        assert calls == ["factory.before_start", "factory.after_stop"]
        assert get_event_registry().get_registered_events() == frozenset(calls)
    finally:
        unsubscribe(receive.__name__)


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_failed_start_can_retry_with_the_same_components(fresh_core, monkeypatch, failure):
    first = components()
    original_start = get_event_pipe().start
    attempts = 0

    async def start():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise failure("startup failed")
        await original_start()

    monkeypatch.setattr(get_event_pipe(), "start", start)
    with pytest.raises(failure):
        await start_core()
    assert not get_event_loop()._started
    await start_core()
    assert all(a is b for a, b in zip(first, components()))
    assert get_event_loop()._started


async def test_independent_components_keep_dispatch_and_observations_isolated(wait_for_dispatch):
    cores = []
    observed = [[], []]
    for index in range(2):
        registry = ApixEventRegistry()
        pipe = ApixEventPipe()
        handlers = ApixHandlerRegistry(registry)
        loop = ApixEventLoop(handlers, pipe, registry)

        async def receive(message, target=observed[index]):
            target.append(message.event_name)

        handler = ApixEventHandler(receive)
        handler.subscribe = ["isolated.*"]
        handler.priority = 1
        handlers.register_handler(handler)
        cores.append((registry, pipe, handlers, loop))
    try:
        for index, (registry, pipe, handlers, loop) in enumerate(cores):
            await pipe.start()
            await loop.start()
            await pipe.put(event(f"isolated.{index}"))
        for index, (registry, pipe, handlers, loop) in enumerate(cores):
            await wait_for_dispatch(loop)
            assert registry.get_registered_events() == frozenset({f"isolated.{index}"})
            assert handlers.get_unmatched_subscriptions("receive") == []
        assert observed == [["isolated.0"], ["isolated.1"]]
    finally:
        for registry, pipe, handlers, loop in cores:
            await loop.stop()
            await pipe.stop()


@pytest.mark.parametrize("getter_name", [
    "get_event_registry", "get_event_pipe", "get_handler_registry", "get_event_loop",
])
async def test_each_getter_restarts_the_same_core(getter_name, fresh_core, wait_for_dispatch):
    """One synchronous getter is sufficient to resume queued local events."""
    await start_core()
    registry, pipe, handlers, loop = components()
    await loop.stop()
    await pipe.stop()
    await pipe.put(event("factory.automatic"))
    component = getattr(factory, getter_name)()
    await wait_for_dispatch(loop)
    assert registry.get_registered_events() == frozenset({"factory.automatic"})
    assert getattr(factory, getter_name)() is component


async def test_getters_share_one_startup_attempt(fresh_core, monkeypatch, wait_for_dispatch):
    """Repeated getters do not schedule a second startup during slow startup."""
    core = factory._get_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original_start = core.event_pipe.start
    attempts = 0

    async def slow_start():
        nonlocal attempts
        attempts += 1
        entered.set()
        await release.wait()
        await original_start()

    monkeypatch.setattr(core.event_pipe, "start", slow_start)
    try:
        components()
        await asyncio.wait_for(entered.wait(), 1)
        for _ in range(8):
            components()
        release.set()
        await core.event_pipe.put(event("factory.shared_start"))
        await wait_for_dispatch(core.event_loop)
        assert attempts == 1
    finally:
        release.set()


async def test_graph_invocation_starts_core_through_getters(fresh_core):
    """Graph invocation and restart require no explicit runtime startup."""
    from apixis.core.graph import NodeGraph

    graph = NodeGraph({}, None)
    try:
        assert await asyncio.wait_for(graph.invoke({"value": 1}), 1) == {"value": 1}
        core = factory._core
        await core.event_loop.stop()
        await core.event_pipe.stop()
        assert await asyncio.wait_for(graph.invoke({"value": 2}), 1) == {"value": 2}
    finally:
        graph.decompose()


async def test_explicit_start_returns_when_restart_has_signalled_started(fresh_core, monkeypatch, wait_for_dispatch):
    """Startup is a signal, not a barrier for every ongoing startup task."""
    await start_core()
    core = factory._core
    await core.event_pipe.stop()
    entered, release = asyncio.Event(), asyncio.Event()
    original_start = core.event_pipe.start

    async def slow_start():
        await original_start()
        entered.set()
        await release.wait()

    monkeypatch.setattr(core.event_pipe, "start", slow_start)
    waiter = None
    try:
        get_event_pipe()
        await asyncio.wait_for(entered.wait(), 1)
        waiter = asyncio.create_task(start_core())
        await asyncio.wait_for(waiter, 1)
        # The startup signal is sufficient to use the already-running core.
        assert not release.is_set()
        await core.event_pipe.put(event("factory.restart_signalled"))
        await wait_for_dispatch(core.event_loop)
        assert "factory.restart_signalled" in core.event_registry.get_registered_events()
    finally:
        release.set()
        if waiter is not None:
            await asyncio.gather(waiter, return_exceptions=True)
