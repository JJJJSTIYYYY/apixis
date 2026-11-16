"""Local registration and explicit routing to independent thread cores."""

import asyncio
import importlib
from concurrent.futures import ThreadPoolExecutor
from threading import get_ident

import pytest

from apixis import (
    ApixEventHandler, EventHandlerAlreadyRegisteredError, EventHandlerNotRegisteredError,
    create_core, get_handler_registry, get_thread_bridge, post_and_wait,
    post_and_wait_threadsafe, start_core, stop_core, subscribe, subscribe_threadsafe,
    unsubscribe, unsubscribe_threadsafe,
)


@pytest.mark.parametrize("module_name", [
    "apixis", "apixis.core", "apixis.core.event",
    "apixis.core.event.subscription", "apixis.core.event.subscription.subscriber",
])
def test_subscription_exports(module_name):
    module = importlib.import_module(module_name)
    for function in (subscribe_threadsafe, unsubscribe_threadsafe):
        assert getattr(module, function.__name__) is function
        assert function.__name__ in module.__all__


async def test_local_subscription_and_instance_methods_in_worker(cleanup_event_runtime):
    main_registry = get_handler_registry()
    owner = get_ident()

    async def worker():
        core = create_core("local-worker")
        seen = []

        async def receive(event):
            seen.append((event.event_name, get_ident()))

        instance = ApixEventHandler(receive)
        try:
            assert subscribe("local.*", group="local")(instance) is instance
            assert get_handler_registry() is core.handler_registry
            await post_and_wait(event_name="local.first")
            unsubscribe(group="local", missing_ok=False)
            assert instance.register("local.*") is instance
            assert instance.unregister(same_group=True) is None
            assert [name for name, _ in seen] == ["local.first"]
            assert all(thread != owner for _, thread in seen)
            assert not core.handler_registry.registry
        finally:
            await stop_core()

    with ThreadPoolExecutor(max_workers=1) as pool:
        await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(pool, lambda: asyncio.run(worker())), 5)
    assert get_handler_registry() is main_registry
    assert not main_registry.registry


@pytest.mark.parametrize("use_instance", [False, True])
async def test_remote_subscription_confirms_readiness_and_target_ownership(thread_cores, use_instance):
    await thread_cores("first")
    await thread_cores("second")
    seen = []

    async def receive(event):
        seen.append((get_thread_id(), event.event_name))

    def get_thread_id():
        from apixis.core.event import factory
        return factory._get_core().thread_id

    handler = ApixEventHandler(receive) if use_instance else receive
    result = await subscribe_threadsafe(
        "job.*", filter_event=["job.hidden"], priority=7, group="workers",
        stop_when_error=False, time_out=0, thread_ids=["first", "second"],
    )(handler)
    assert set(result) == {"first", "second"}
    if use_instance:
        assert result["first"] is not result["second"]
        assert all(registered is not handler for registered in result.values())
        assert handler.subscribe == []
        assert all(registered.group == "workers" for registered in result.values())
    else:
        assert all(registered is handler for registered in result.values())
    await post_and_wait_threadsafe(event_name="job.visible", thread_ids=["first", "second"])
    await post_and_wait_threadsafe(event_name="job.hidden", thread_ids=["first", "second"])
    assert sorted(seen) == [("first", "job.visible"), ("second", "job.visible")]
    await unsubscribe_threadsafe(group="workers", thread_ids="first", missing_ok=False)
    await post_and_wait_threadsafe(event_name="job.again", thread_ids=["first", "second"])
    assert seen[-1] == ("second", "job.again")
    await unsubscribe_threadsafe("receive", thread_ids="second", missing_ok=False)


async def test_remote_errors_preserve_existing_registration(thread_cores):
    await thread_cores("worker")

    async def receive(event):
        pass

    registered = await subscribe_threadsafe("original", group="original", thread_ids="worker")(
        ApixEventHandler(receive),
    )
    with pytest.raises(EventHandlerAlreadyRegisteredError):
        await subscribe_threadsafe("new", group="new", exist_ok=False, thread_ids="worker")(registered)
    with pytest.raises(ValueError):
        await subscribe_threadsafe("new", group="", thread_ids="worker")(registered)
    assert registered.subscribe == ["original"]
    assert registered.group == "original"
    assert await get_thread_bridge().call_sync(
        lambda: get_handler_registry().get_handler("receive") is registered, thread_ids="worker",
    )
    with pytest.raises(EventHandlerNotRegisteredError):
        await unsubscribe_threadsafe(group="missing", missing_ok=False, thread_ids="worker")
    with pytest.raises(ValueError):
        await unsubscribe_threadsafe("receive", group="original", thread_ids="worker")
    await unsubscribe_threadsafe(group="original", thread_ids="worker")


def test_local_subscriptions_do_not_need_remote_bridge(cleanup_event_runtime, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Local operations must not resolve remote routes")

    monkeypatch.setattr(get_thread_bridge(), "targets", unexpected)

    async def receive(event):
        pass

    assert subscribe("local")(receive) is receive
    unsubscribe("receive", missing_ok=False)


async def test_remote_registration_defaults_to_main_core(thread_cores):
    await thread_cores("worker")

    async def receive(event):
        event.context["seen"] = True

    async def register_from_worker():
        return await subscribe_threadsafe("main.event")(receive)

    assert await get_thread_bridge().call(register_from_worker, thread_ids="worker") is receive
    event = await post_and_wait(event_name="main.event", context={})
    assert event.context == {"seen": True}
    assert await get_thread_bridge().call_sync(lambda: not get_handler_registry().registry, thread_ids="worker")
    await unsubscribe_threadsafe("receive")


@pytest.mark.parametrize("routed_stop", [False, True])
async def test_remote_target_must_be_started_and_can_restart(thread_cores, routed_stop):
    handle = await thread_cores("worker")
    from concurrent.futures import Future
    paused, resume, restarted = Future(), Future(), Future()

    if routed_stop:
        with pytest.raises(RuntimeError, match="stopped"):
            await asyncio.wait_for(get_thread_bridge().call(stop_core, thread_ids="worker"), 3)

    async def lifecycle():
        await stop_core()
        paused.set_result(None)
        await asyncio.wrap_future(resume)
        await start_core()
        restarted.set_result(None)

    handle.loop.call_soon_threadsafe(lambda: asyncio.create_task(lifecycle()))
    await asyncio.wait_for(asyncio.wrap_future(paused), 3)
    try:
        with pytest.raises(RuntimeError, match="not running"):
            await post_and_wait_threadsafe(event_name="no", thread_ids="worker")
    finally:
        resume.set_result(None)
    await asyncio.wait_for(asyncio.wrap_future(restarted), 3)
    assert (await post_and_wait_threadsafe(event_name="yes", thread_ids="worker")).event_name == "yes"
