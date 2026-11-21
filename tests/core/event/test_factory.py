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
    Logger,
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


@pytest_asyncio.fixture(
    params=[None, asyncio.eager_task_factory], ids=["default", "eager"],
)
async def lifecycle_core(fresh_core, request):
    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    loop.set_task_factory(request.param)
    try:
        yield create_core()
    finally:
        loop.set_task_factory(original_factory)


async def wait_for_state(core, state):
    async with asyncio.timeout(1):
        while core.state is not state:
            await asyncio.sleep(0)


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


@pytest.mark.parametrize("name", ["GraphNodeError", "InvalidNodeReturnsError", "InvalidContextError"])
def test_graph_exceptions_belong_to_graph_facade(name):
    event = importlib.import_module("apixis.core.event")
    graph = importlib.import_module("apixis.core.graph")
    root = importlib.import_module("apixis")
    assert name not in event.__all__
    assert not hasattr(event, name)
    assert name in graph.__all__
    assert getattr(graph, name) is getattr(root, name)


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


def test_core_state_covers_each_lifecycle_phase():
    assert [state.value for state in CoreState] == [
        "created", "starting", "started", "running", "stopping", "stopped",
        "closing", "closed",
    ]


async def test_start_reports_created_task_before_confirming_consumption(
    fresh_core, monkeypatch,
):
    core = create_core()
    original_start = core.event_loop.start

    async def start_consumer():
        assert core.state is CoreState.STARTING
        return await original_start()

    monkeypatch.setattr(core.event_loop, "start", start_consumer)
    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    loop.set_task_factory(None)
    try:
        starting = asyncio.create_task(start_core())
        await wait_for_state(core, CoreState.STARTED)
        assert core.state is CoreState.STARTED
        assert not starting.done()
        status = get_event_runtime_status()
        assert status.state is CoreState.STARTED
        assert not status.running
        await starting
        assert core.state is CoreState.RUNNING
        assert get_event_runtime_status().running
    finally:
        loop.set_task_factory(original_factory)


@pytest.mark.parametrize("action", ["cancel", "close"])
async def test_start_caller_cancel_and_concurrent_close(fresh_core, action):
    core = create_core()
    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    loop.set_task_factory(None)
    try:
        starting = asyncio.create_task(start_core())
        await wait_for_state(core, CoreState.STARTED)
        assert core.state is CoreState.STARTED
        if action == "cancel":
            starting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await starting
            await core._start_task
            assert core.state is CoreState.RUNNING
        else:
            await close_core()
            await starting
            assert core.state is CoreState.CLOSED
            assert not core.event_loop.is_running
    finally:
        loop.set_task_factory(original_factory)


@pytest.mark.parametrize("failure", [False, True])
async def test_start_callers_share_operation_and_cancellation_is_local(
    fresh_core, monkeypatch, failure,
):
    core = create_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = core.event_loop.start
    calls = 0

    async def blocked_start():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        if failure:
            raise RuntimeError("startup failed")
        return await original()

    monkeypatch.setattr(core.event_loop, "start", blocked_start)
    first = asyncio.create_task(start_core())
    await entered.wait()
    operation = core._start_task
    second = asyncio.create_task(start_core())
    try:
        await asyncio.sleep(0)
        assert core._start_task is operation
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not operation.done()
        assert not operation.cancelling()
        release.set()
        if failure:
            with pytest.raises(RuntimeError, match="startup failed"):
                await second
            assert core.state is CoreState.STOPPED
        else:
            await second
            assert core.state is CoreState.RUNNING
        assert calls == 1
    finally:
        release.set()
        await asyncio.gather(first, second, return_exceptions=True)


@pytest.mark.parametrize("outcome", ["success", "failure", "closed_before_creation"])
async def test_close_joins_startup_and_callers_share_cleanup(
    fresh_core, monkeypatch, outcome,
):
    core = create_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = core.event_loop.start
    cleanup = AsyncMock(wraps=core.event_loop.close)

    async def blocked_start():
        consumer = None
        if outcome != "closed_before_creation":
            consumer = await original()
        entered.set()
        await release.wait()
        if outcome == "failure":
            raise RuntimeError("startup failed")
        return consumer if consumer is not None else await original()

    monkeypatch.setattr(core.event_loop, "start", blocked_start)
    monkeypatch.setattr(core.event_loop, "close", cleanup)
    starting = asyncio.create_task(start_core())
    await entered.wait()
    first = asyncio.create_task(close_core())
    await wait_for_state(core, CoreState.CLOSING)
    operation = core._close_task
    second = asyncio.create_task(close_core())
    try:
        await asyncio.sleep(0)
        assert core.event_pipe.closed
        assert core.handler_registry.closed
        assert core._close_task is operation
        assert not operation.done()
        cleanup.assert_not_awaited()
        with pytest.raises(CoreClosedError):
            await start_core()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not operation.cancelling()
        release.set()
        result = await asyncio.gather(starting, return_exceptions=True)
        if outcome == "success":
            assert result == [None]
        else:
            expected = RuntimeError if outcome == "failure" else CoreClosedError
            assert isinstance(result[0], expected)
        await second
        assert core.state is CoreState.CLOSED
        assert not core.event_loop.is_running
        assert core._start_task is None
        assert core._stop_task is None
        assert core._close_task is None
        cleanup.assert_awaited_once()
        await close_core()
        cleanup.assert_awaited_once()
    finally:
        release.set()
        await asyncio.gather(starting, first, second, return_exceptions=True)


async def test_stop_joins_startup_before_a_queued_restart(fresh_core, monkeypatch):
    core = create_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = core.event_loop.start
    calls = 0

    async def blocked_start():
        nonlocal calls
        calls += 1
        consumer = await original()
        if calls == 1:
            entered.set()
            await release.wait()
        return consumer

    monkeypatch.setattr(core.event_loop, "start", blocked_start)
    starting = asyncio.create_task(start_core())
    await entered.wait()
    stopping = asyncio.create_task(stop_core())
    await wait_for_state(core, CoreState.STOPPING)
    restart = asyncio.create_task(start_core())
    try:
        await asyncio.sleep(0)
        assert not restart.done()
        assert calls == 1
        release.set()
        await asyncio.gather(starting, stopping, restart)
        assert calls == 2
        assert core.state is CoreState.RUNNING
        assert core.event_loop.is_running
    finally:
        release.set()
        await asyncio.gather(starting, stopping, restart, return_exceptions=True)


async def test_start_stop_steps_are_serialized_despite_cancelled_callers(
    lifecycle_core, monkeypatch,
):
    core = lifecycle_core
    start_entered, start_release = asyncio.Event(), asyncio.Event()
    stop_entered, stop_release = asyncio.Event(), asyncio.Event()
    original_start, original_stop = core.event_loop.start, core.event_loop.stop
    steps = []

    async def blocked_start():
        assert core._stop_task is None or core._stop_task.done()
        steps.append("start.enter")
        start_entered.set()
        await start_release.wait()
        consumer = await original_start()
        steps.append("start.exit")
        return consumer

    async def blocked_stop():
        assert core._start_task.done()
        steps.append("stop.enter")
        stop_entered.set()
        await stop_release.wait()
        await original_stop()
        steps.append("stop.exit")

    monkeypatch.setattr(core.event_loop, "start", blocked_start)
    monkeypatch.setattr(core.event_loop, "stop", blocked_stop)
    starting = asyncio.create_task(core.start_core())
    await start_entered.wait()
    cancelled_stop = asyncio.create_task(core.stop_core())
    await wait_for_state(core, CoreState.STOPPING)
    stopping = asyncio.create_task(core.stop_core())
    cancelled_restart = asyncio.create_task(core.start_core())
    restart = asyncio.create_task(core.start_core())
    tasks = starting, cancelled_stop, stopping, cancelled_restart, restart
    try:
        await asyncio.sleep(0)
        assert steps == ["start.enter"]
        cancelled_stop.cancel()
        cancelled_restart.cancel()
        for task in (cancelled_stop, cancelled_restart):
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not core._start_task.cancelling()
        assert not core._stop_task.cancelling()
        start_release.set()
        await stop_entered.wait()
        assert steps == ["start.enter", "start.exit", "stop.enter"]
        assert not restart.done()
        stop_release.set()
        await asyncio.gather(starting, stopping, restart)
        assert steps == [
            "start.enter", "start.exit", "stop.enter", "stop.exit",
            "start.enter", "start.exit",
        ]
        assert core.state is CoreState.RUNNING
        assert core.event_loop.is_running
    finally:
        start_release.set()
        stop_release.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("operation", ["start", "stop"])
@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
async def test_lifecycle_cleanup_failure_does_not_prevent_restart(
    lifecycle_core, monkeypatch, operation, failure_type,
):
    core = lifecycle_core
    if operation == "stop":
        await core.start_core()
    failure = failure_type("stop logging failed")

    def fail_logging(message):
        if message == "Worker started." and operation == "start":
            raise RuntimeError("startup logging failed")
        if message == "Worker stopped.":
            raise failure

    with monkeypatch.context() as logging_patch:
        logging_patch.setattr(Logger, "_info", fail_logging)
        with pytest.raises(failure_type):
            await getattr(core, f"{operation}_core")()
    assert core.state is CoreState.STOPPED
    assert not core.event_loop.is_running
    await core.start_core()
    assert core.state is CoreState.RUNNING
    assert core.event_loop.is_running
    await core.stop_core()
    assert core.state is CoreState.STOPPED
    await core.start_core()
    assert core.state is CoreState.RUNNING
    assert core.event_loop.is_running


async def test_repeated_pause_resume_preserves_queued_events(
    lifecycle_core, wait_for_dispatch,
):
    core = lifecycle_core
    seen = []

    @subscribe("lifecycle.resume")
    async def receive(event):
        seen.append(event.context)

    await core.start_core()
    for cycle in range(10):
        previous_consumer = core.event_loop._event_consumer_task
        await asyncio.gather(*(core.stop_core() for _ in range(4)))
        assert core.state is CoreState.STOPPED
        assert previous_consumer.done()
        await post_event(event_name="lifecycle.resume", context=cycle)
        await asyncio.sleep(0)
        assert seen == list(range(cycle))
        assert core.event_pipe.qsize() == 1
        await asyncio.gather(*(core.start_core() for _ in range(4)))
        assert core.state is CoreState.RUNNING
        assert core.event_loop.is_running
        assert core.event_loop._event_consumer_task is not previous_consumer
        await wait_for_dispatch(core.event_loop)
        assert seen == list(range(cycle + 1))


async def test_stop_before_first_start_preserves_queued_work(
    lifecycle_core, wait_for_dispatch,
):
    core = lifecycle_core
    seen = []

    @subscribe("lifecycle.created")
    async def receive(event):
        seen.append(event.context)

    await post_event(event_name="lifecycle.created", context=42)
    await core.stop_core()
    assert core.state is CoreState.STOPPED
    assert core._stop_task.done()
    assert core.event_loop._event_consumer_task is None
    assert core.event_pipe.qsize() == 1
    assert seen == []
    await core.start_core()
    await wait_for_dispatch(core.event_loop)
    assert core.state is CoreState.RUNNING
    assert seen == [42]


async def test_close_waits_for_shared_stop_and_rejects_queued_restart(
    fresh_core, monkeypatch,
):
    core = create_core()
    await start_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = core.event_loop.stop

    async def blocked_stop():
        entered.set()
        await release.wait()
        await original()

    monkeypatch.setattr(core.event_loop, "stop", blocked_stop)
    stopping = asyncio.create_task(stop_core())
    await entered.wait()
    operation = core._stop_task
    restart = asyncio.create_task(start_core())
    closing = asyncio.create_task(close_core())
    try:
        await wait_for_state(core, CoreState.CLOSING)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        assert not operation.cancelling()
        assert not closing.done()
        release.set()
        await closing
        with pytest.raises(CoreClosedError):
            await restart
        assert core.state is CoreState.CLOSED
    finally:
        release.set()
        await asyncio.gather(stopping, restart, closing, return_exceptions=True)


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
    assert core.state is CoreState.STOPPING
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


async def test_consumer_failure_updates_state_and_allows_restart(fresh_core, monkeypatch):
    core = create_core()
    original_get = core.event_pipe.get
    failure = RuntimeError("consumer failed")
    release = asyncio.Event()

    async def fail_get():
        await release.wait()
        raise failure

    monkeypatch.setattr(core.event_pipe, "get", fail_get)

    await start_core()
    consumer = core.event_loop._event_consumer_task
    release.set()
    assert await asyncio.gather(consumer, return_exceptions=True) == [failure]
    assert core.state is CoreState.STOPPED
    assert not core.event_loop.is_running

    monkeypatch.setattr(core.event_pipe, "get", original_get)
    await start_core()
    assert core.state is CoreState.RUNNING


async def test_start_replaces_finished_consumer_before_its_completion_callbacks(
    fresh_core, monkeypatch,
):
    core = create_core()
    original_get = core.event_pipe.get
    release = asyncio.Event()

    async def fail_get():
        await release.wait()
        raise RuntimeError("consumer failed")

    monkeypatch.setattr(core.event_pipe, "get", fail_get)
    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    loop.set_task_factory(None)
    try:
        await start_core()
        consumer = core.event_loop._event_consumer_task
        release.set()
        await asyncio.sleep(0)
        assert consumer.done()
        assert core.state is CoreState.RUNNING
        monkeypatch.setattr(core.event_pipe, "get", original_get)
        await start_core()
        assert core.event_loop._event_consumer_task is not consumer
        assert core.event_loop.is_running
        assert core.state is CoreState.RUNNING
    finally:
        loop.set_task_factory(original_factory)


async def test_stop_publishes_task_before_logging_can_request_restart(
    fresh_core, monkeypatch,
):
    core = create_core()
    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    restarts = []

    def request_restart(message):
        if message == "Worker stopped.":
            restarts.append(asyncio.create_task(core.start_core()))

    try:
        loop.set_task_factory(None)
        await core.start_core()
        consumer = core.event_loop._event_consumer_task
        consumer.cancel()
        await asyncio.sleep(0)
        assert consumer.done()
        assert core.state is CoreState.RUNNING
        loop.set_task_factory(asyncio.eager_task_factory)
        with monkeypatch.context() as logging_patch:
            logging_patch.setattr(Logger, "_info", request_restart)
            await core.stop_core()
            await asyncio.gather(*restarts)
        await core.start_core()
        assert core.state is CoreState.RUNNING
        assert core.event_loop.is_running
    finally:
        loop.set_task_factory(original_factory)
        await asyncio.gather(*restarts, return_exceptions=True)


@pytest.mark.parametrize("failure", [None, RuntimeError("consumer failed during start")])
async def test_consumer_finishing_during_start_leaves_core_stopped(
    fresh_core, monkeypatch, failure,
):
    core = create_core()
    original_consumer = core.event_loop._event_consumer_loop
    monkeypatch.setattr(
        core.event_loop, "_event_consumer_loop",
        AsyncMock(side_effect=failure, return_value=None),
    )
    loop = asyncio.get_running_loop()
    original_factory = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        await start_core()
    finally:
        loop.set_task_factory(original_factory)

    assert core.state is CoreState.STOPPED
    assert not core.event_loop.is_running
    # Delayed completion callbacks must not stop the replacement consumer.
    monkeypatch.setattr(core.event_loop, "_event_consumer_loop", original_consumer)
    await start_core()
    await asyncio.sleep(0)
    assert core.state is CoreState.RUNNING
    assert core.event_loop.is_running


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
