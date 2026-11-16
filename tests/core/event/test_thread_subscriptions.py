"""Owner-loop registration, readiness and cancellation from worker loops."""

import asyncio
import importlib
from functools import partial
from threading import Event, get_ident

import pytest

from apixis import (
    ApixEventHandler, ApixEventPipe,
    EventHandlerAlreadyRegisteredError, EventHandlerNotRegisteredError,
    post_and_wait, post_event, start_core, stop_core,
    subscribe, subscribe_threadsafe, unsubscribe, unsubscribe_threadsafe,
    asubscribe, unasubscribe,
)
from apixis.core.event import factory


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    return 1


@pytest.fixture
async def runtime(cleanup_event_runtime):
    core = cleanup_event_runtime
    try:
        yield core
    finally:
        await core.thread_bridge.close()
        assert not core.thread_bridge._active
        assert not core.thread_bridge._async_admission


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.parametrize("module_name", [
    "apixis", "apixis.core", "apixis.core.event",
    "apixis.core.event.subscription", "apixis.core.event.subscription.subscriber",
])
def test_thread_subscription_exports(module_name):
    module = importlib.import_module(module_name)
    assert module.subscribe_threadsafe is subscribe_threadsafe
    assert module.unsubscribe_threadsafe is unsubscribe_threadsafe
    assert module.asubscribe is asubscribe
    assert module.unasubscribe is unasubscribe
    assert {
        "subscribe_threadsafe", "unsubscribe_threadsafe", "asubscribe", "unasubscribe",
    } <= set(module.__all__)


@pytest.mark.parametrize("register, remove", [
    (subscribe, unsubscribe), (subscribe_threadsafe, unsubscribe_threadsafe),
    (asubscribe, unasubscribe),
])
@pytest.mark.parametrize("use_instance", [False, True])
async def test_worker_registration_and_removal_complete_on_owner_loop(
    runtime, monkeypatch, register, remove, use_instance,
):
    owner = get_ident()
    mutations, received = [], []
    registry = runtime.handler_registry
    original_register = registry.register_handler
    original_remove = registry.unregister_handler

    def record_register(*args, **kwargs):
        mutations.append(("register", get_ident()))
        return original_register(*args, **kwargs)

    def record_remove(*args, **kwargs):
        mutations.append(("remove", get_ident()))
        return original_remove(*args, **kwargs)

    monkeypatch.setattr(registry, "register_handler", record_register)
    monkeypatch.setattr(registry, "unregister_handler", record_remove)

    async def receive(event):
        received.append((event.event_name, get_ident()))

    handler = ApixEventHandler(receive) if use_instance else receive
    # Creation and application can happen on different threads.
    decorator = register(
        "worker.*", priority=7, filter_event=["worker.hidden"],
        stop_when_error=False, time_out=0, background=False, group="workers",
    )

    async def worker():
        assert get_ident() != owner
        assert await decorator(handler) is handler
        await post_and_wait(event_name="worker.visible")
        await post_and_wait(event_name="worker.hidden")
        await remove("receive", missing_ok=False)
        await post_and_wait(event_name="worker.visible")

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)
    assert received == [("worker.visible", owner)]
    assert all(thread == owner for _, thread in mutations)
    assert ("register", owner) in mutations and ("remove", owner) in mutations
    assert not registry.registry
    if use_instance:
        assert handler.group == "workers"
        assert handler.priority == 7
        assert handler.filter_event == ["worker.hidden"]
        assert not handler.stop_when_error
        assert handler.time_out is None
        assert not handler.background


async def test_decorator_selects_thread_when_applied_and_main_calls_stay_sync(runtime):
    async def receive(event):
        pass

    decorator = await asyncio.to_thread(lambda: subscribe("owner.event"))
    assert decorator(receive) is receive
    assert runtime.handler_registry.get_handler("receive") is not None
    assert unsubscribe("receive") is None
    assert not runtime.handler_registry.registry


async def test_async_subscription_main_calls_work_without_thread_bridge(runtime, monkeypatch):
    async def receive(event):
        pass

    with monkeypatch.context() as patch:
        patch.setattr(runtime, "thread_bridge", None)
        # The factory may be created on a worker and awaited on the main loop.
        decorator = await asyncio.to_thread(lambda: asubscribe("owner.event", group="owner"))
        assert await decorator(receive) is receive
        assert runtime.handler_registry.get_handler("receive").group == "owner"
        with pytest.raises(EventHandlerAlreadyRegisteredError):
            await asubscribe("replacement", exist_ok=False)(receive)
        with pytest.raises(ValueError):
            await unasubscribe("receive", group="owner")
        assert await unasubscribe(group="owner", missing_ok=False) is None
        with pytest.raises(EventHandlerNotRegisteredError):
            await unasubscribe("receive", missing_ok=False)
        assert not runtime.handler_registry.registry


@pytest.mark.parametrize("same_group", [False, True])
async def test_handler_instance_methods_forward_worker_awaitables(runtime, same_group):
    async def receive(event):
        pass

    instance = ApixEventHandler(receive)

    @subscribe("other", group="workers")
    async def other(event):
        pass

    async def worker():
        assert await instance.register("worker.*", group="workers") is instance
        await instance.unregister(same_group=same_group, missing_ok=False)

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)
    assert set(runtime.handler_registry.registry) == (set() if same_group else {"other"})


@pytest.mark.parametrize("register, remove", [
    (subscribe, unsubscribe), (subscribe_threadsafe, unsubscribe_threadsafe),
    (asubscribe, unasubscribe),
])
async def test_worker_validation_replacement_and_group_removal(runtime, register, remove):
    async def receive(event):
        pass

    instance = ApixEventHandler(receive)
    subscribe("original", group="original", priority=3)(instance)
    registry = runtime.handler_registry
    original_chain = registry.get_handlers_chain_for_event("original")

    async def worker():
        with pytest.raises(EventHandlerAlreadyRegisteredError):
            await register("replacement", group="new", exist_ok=False)(instance)
        with pytest.raises(ValueError):
            await register("replacement", group="")(instance)
        with pytest.raises(ValueError):
            await register("replacement", priority=float("nan"))(instance)
        with pytest.raises(ValueError):
            await register()(instance)
        with pytest.raises(EventHandlerNotRegisteredError):
            await remove(group="missing", missing_ok=False)
        with pytest.raises(EventHandlerNotRegisteredError):
            await remove("missing", missing_ok=False)
        with pytest.raises(ValueError):
            await remove("receive", group="original")
        with pytest.raises(ValueError):
            await remove()
        assert await remove("missing") is None

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)
    assert registry.get_handler("receive") is instance
    assert registry.get_handlers_chain_for_event("original") is original_chain
    assert instance.subscribe == ["original"]
    assert instance.group == "original"
    assert instance.priority == 3

    async def replace():
        assert await register("replacement", group="new")(instance) is instance
        await remove(group="original")
        await remove(group="new", missing_ok=False)

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(replace())), 3)
    assert not registry.registry


@pytest.mark.parametrize("operation", ["subscribe", "unsubscribe"])
@pytest.mark.parametrize("waiting_for_admission", [False, True])
@pytest.mark.parametrize("shutdown", [False, True], ids=["cancel", "stop"])
@pytest.mark.parametrize("register, remove", [
    (subscribe, unsubscribe), (asubscribe, unasubscribe),
])
async def test_pending_worker_mutations_can_be_cancelled_or_stopped(
    runtime, monkeypatch, operation, waiting_for_admission, shutdown, register, remove,
):
    entered, release = asyncio.Event(), asyncio.Event()
    cancel_now, worker_ticked = Event(), Event()

    async def blocked(event):
        entered.set()
        await release.wait()

    async def receive(event):
        pass

    if operation == "unsubscribe":
        subscribe("original")(receive)
    original_names = set(runtime.handler_registry.registry)
    monkeypatch.setattr(runtime.event_pipe, "put", blocked)
    bridge = runtime.thread_bridge
    first = await bridge.asubmit(partial(post_event, event_name="blocked"))
    await asyncio.wait_for(entered.wait(), 3)
    queued = None
    if waiting_for_admission:
        queued = await bridge.asubmit(partial(post_event, event_name="queued"))

    async def worker():
        pending = (
            register("worker.event")(receive) if operation == "subscribe"
            else remove("receive")
        )
        task = asyncio.create_task(pending)
        await asyncio.sleep(0)
        worker_ticked.set()
        if not shutdown:
            while not cancel_now.is_set():
                await asyncio.sleep(0)
            task.cancel()
        with pytest.raises(RuntimeError if shutdown else asyncio.CancelledError):
            await task

    task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(worker())))
    try:
        await until(worker_ticked.is_set)
        if waiting_for_admission:
            await until(lambda: bool(bridge._async_admission))
        else:
            await until(lambda: bool(bridge._requests))
        if shutdown:
            await stop_core()
        else:
            cancel_now.set()
        await asyncio.wait_for(task, 3)
    finally:
        cancel_now.set()
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        for request in (first, queued):
            if request is not None:
                await asyncio.gather(asyncio.wrap_future(request.result), return_exceptions=True)
    await until(lambda: not bridge._active)
    assert set(runtime.handler_registry.registry) == original_names


@pytest.mark.parametrize("register, remove", [
    (subscribe, unsubscribe), (subscribe_threadsafe, unsubscribe_threadsafe),
    (asubscribe, unasubscribe),
])
@pytest.mark.parametrize("state", ["uninitialized", "stopped"])
async def test_worker_mutations_require_started_core(runtime, monkeypatch, register, remove, state):
    await stop_core()
    if state == "uninitialized":
        monkeypatch.setattr(factory, "_core", None)

    async def receive(event):
        pass

    async def worker():
        with pytest.raises(RuntimeError, match="start_core"):
            await register("worker.event")(receive)
        with pytest.raises(RuntimeError, match="start_core"):
            await remove("receive")

    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(worker())), 3)


async def test_threadsafe_mutations_work_after_core_restart(runtime):
    await stop_core()
    await start_core()

    async def receive(event):
        pass

    assert await subscribe_threadsafe("owner.event")(receive) is receive
    await unsubscribe_threadsafe("receive", missing_ok=False)
    assert not runtime.handler_registry.registry


@pytest.mark.parametrize("register, remove", [
    (subscribe, unsubscribe), (asubscribe, unasubscribe),
])
async def test_concurrent_workers_register_publish_and_remove(runtime, register, remove):
    owner = get_ident()
    received = []

    async def worker(index):
        async def receive(event):
            received.append((event.context, get_ident()))

        receive.__name__ = f"worker_{index}"
        name = f"worker.{index}"
        await register(name, group=name)(receive)
        await post_and_wait(event_name=name, context=index)
        await remove(group=name, missing_ok=False)

    await asyncio.wait_for(asyncio.gather(*(
        asyncio.to_thread(lambda index=index: asyncio.run(worker(index)))
        for index in range(4)
    )), 5)
    assert sorted(received) == [(index, owner) for index in range(4)]
    assert not runtime.handler_registry.registry
