"""Named routing and thread/loop ownership with independently running cores."""

import asyncio
from contextvars import ContextVar
from threading import get_ident

import pytest

from apixis import (
    ApixEventPipe, EventWaiter, create_core, expect_event, expect_event_threadsafe,
    get_event_pipe, get_event_runtime_status, get_handler_registry, get_thread_bridge,
    post_and_wait, post_and_wait_threadsafe, post_event_threadsafe,
    start_core, stop_core, subscribe, wait_event_threadsafe,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_token_context, suspend_process


publisher_context = ContextVar("multicore_publisher", default=None)


@pytest.fixture
def event_runtime_capacity(monkeypatch):
    monkeypatch.setattr(factory, "ApixEventPipe", lambda: ApixEventPipe(maxsize=1))
    return 1


async def test_create_core_exports_and_rejects_conflicting_names(thread_cores):
    assert create_core("main") is factory._get_core()
    with pytest.raises(ValueError, match="already owns"):
        create_core("another")
    await thread_cores("worker")
    with pytest.raises(ValueError, match="already registered"):
        await thread_cores("worker")


async def test_default_target_follows_explicit_main_core_name(monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(factory._local, "core", None)
        core = create_core("ui")
        await start_core()
        try:
            @subscribe("ui.request")
            async def receive(event):
                event.context.append(core.thread_id)

            event = await post_and_wait_threadsafe(event_name="ui.request", context=[])
            assert event.context == ["ui"]
        finally:
            await stop_core()


async def test_unregistered_worker_never_inherits_core_from_context(cleanup_event_runtime):
    main_pipe = get_event_pipe()

    async def worker():
        assert factory._local.core is None
        assert not get_event_runtime_status().initialized
        with pytest.raises(RuntimeError, match="create_core"):
            get_event_pipe()
        core = create_core("local")
        assert core.event_pipe is not main_pipe
        assert get_event_pipe() is core.event_pipe
        await start_core()
        try:
            first = asyncio.get_running_loop()
            assert core.loop is first
        finally:
            await stop_core()
        return core

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as pool:
        core = await asyncio.get_running_loop().run_in_executor(pool, lambda: asyncio.run(worker()))
    assert core.event_pipe is not main_pipe
    assert get_event_pipe() is main_pipe


async def test_same_thread_different_loop_getters_cannot_leak_components():
    def worker():
        async def initialize():
            create_core("loop-owner")
            await start_core()
            await stop_core()

        async def wrong_loop():
            for getter in (
                factory.get_event_pipe, factory.get_event_loop,
                factory.get_handler_registry, factory.get_event_registry,
                factory.get_event_runtime_status,
            ):
                with pytest.raises(RuntimeError, match="different asyncio loop"):
                    getter()
            for getter in (factory.aget_event_pipe, factory.aget_event_loop, factory.aget_event_registry,
                           factory.aget_handler_registry, factory.start_core, factory.stop_core):
                with pytest.raises(RuntimeError, match="different asyncio loop"):
                    await getter()

        asyncio.run(initialize())
        asyncio.run(wrong_loop())

    # A dedicated Thread prevents executor thread reuse from affecting this contract.
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as pool:
        await asyncio.get_running_loop().run_in_executor(pool, worker)


async def test_local_subscription_publication_and_waiting_stay_in_each_core(thread_cores):
    observed = []

    def setup():
        @subscribe("same.name")
        async def receive(event):
            observed.append((factory._get_core().thread_id, event.context))

    setup()
    await thread_cores("a", setup)
    await thread_cores("b", setup)

    async def local_flow():
        name = factory._get_core().thread_id
        async with expect_event("same.name", point="processed") as waiter:
            assert isinstance(waiter, EventWaiter)
            event = await post_and_wait(event_name="same.name", context=name)
            assert await waiter.get_result() is event
        assert get_event_runtime_status().wait_listeners == 0
        return name

    assert await local_flow() == "main"
    assert await get_thread_bridge().call(local_flow, thread_ids=["a", "b"], background=True) == {"a": "a", "b": "b"}
    assert sorted(observed) == [("a", "a"), ("b", "b"), ("main", "main")]


@pytest.mark.parametrize("event_names", ["reply", ["reply", "reply*"]])
async def test_multitarget_wait_confirms_registration_and_preserves_result_shape(thread_cores, event_names):
    await thread_cores("a")
    await thread_cores("b")
    ids = ["a", "b", "a"]
    async with expect_event_threadsafe(event_names, point="processed", thread_ids=ids, time_out=2) as waiter:
        events = await post_and_wait_threadsafe(event_name="reply", thread_ids=ids)
        await post_event_threadsafe(event_name="reply.more", thread_ids=ids)
        result = await waiter.wait()
    assert set(result) == {"a", "b"}
    assert events["a"].event_id != events["b"].event_id
    for name in ("a", "b"):
        if isinstance(event_names, str):
            assert result[name] is events[name]
        else:
            assert result[name] == {"reply": events[name], "reply*": events[name]}
    assert await get_thread_bridge().call_sync(
        lambda: get_event_runtime_status().wait_listeners, thread_ids=["a", "b"],
    ) == {"a": 0, "b": 0}


async def test_collection_of_one_target_returns_thread_dictionary(thread_cores):
    await thread_cores("worker")
    async with expect_event_threadsafe("reply", thread_ids=["worker"]) as waiter:
        await post_event_threadsafe(event_name="reply", thread_ids="worker")
        result = await waiter.get_result()
    assert result["worker"].event_name == "reply"


@pytest.mark.parametrize("targets", ["a", ["a", "b"]])
@pytest.mark.parametrize("patterns", ["reply", ["reply", "reply*"]])
async def test_completed_remote_waiter_retains_results_after_target_shutdown(
    thread_cores, targets, patterns,
):
    await thread_cores("a")
    await thread_cores("b")
    async with expect_event_threadsafe(patterns, thread_ids=targets) as waiter:
        events = await post_and_wait_threadsafe(event_name="reply", context=42, thread_ids=targets)
        result = await waiter.get_result()

    async def shutdown():
        await stop_core()

    for target in [targets] if isinstance(targets, str) else targets:
        with pytest.raises(RuntimeError, match="stopped"):
            await get_thread_bridge().call(shutdown, thread_ids=target)
    assert await waiter.wait() is result
    assert await waiter.get_result() is result
    if isinstance(targets, str) and isinstance(patterns, str):
        assert await waiter.wait() is events


async def test_multitarget_failure_is_retained_and_cleans_siblings_without_a_reader(thread_cores):
    await thread_cores("a")
    await thread_cores("b")
    failure = ValueError("predicate failed")

    def predicate(event):
        raise failure

    async with expect_event_threadsafe(
        "reply", thread_ids=["a", "b"], predicate=predicate, time_out=2,
    ) as waiter:
        future = waiter._future
        await post_event_threadsafe(event_name="reply", thread_ids="b")
        async with asyncio.timeout(2):
            while not future.done():
                await asyncio.sleep(0)
        assert await get_thread_bridge().call_sync(
            lambda: get_event_runtime_status().wait_listeners, thread_ids=["a", "b"],
        ) == {"a": 0, "b": 0}
        for _ in range(2):
            with pytest.raises(ValueError) as raised:
                await waiter.wait()
            assert raised.value is failure
        assert waiter._future is future


@pytest.mark.parametrize("targets", ["a", ["a", "b"]])
async def test_remote_future_cancellation_cleans_targets_without_a_reader(thread_cores, targets):
    await thread_cores("a")
    await thread_cores("b")
    async with expect_event_threadsafe("never", thread_ids=targets, time_out=60) as waiter:
        waiter._future.cancel()
        async with asyncio.timeout(2):
            await asyncio.gather(*(target._wait_closed() for target in waiter._targets.values()))
        assert await get_thread_bridge().call_sync(
            lambda: get_event_runtime_status().wait_listeners, thread_ids=["a", "b"],
        ) == {"a": 0, "b": 0}
        with pytest.raises(asyncio.CancelledError):
            await waiter.wait()


@pytest.mark.parametrize("cancel", [False, True])
async def test_multitarget_wait_timeout_or_cancellation_cleans_every_core(thread_cores, cancel):
    await thread_cores("a")
    await thread_cores("b")
    async with expect_event_threadsafe("reply", thread_ids=["a", "b"], time_out=None if cancel else 0.1) as waiter:
        await post_event_threadsafe(event_name="reply", thread_ids="a")
        if cancel:
            task = asyncio.create_task(waiter.get_result())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(TimeoutError):
                await waiter.wait()
        assert await get_thread_bridge().call_sync(
            lambda: get_event_runtime_status().wait_listeners, thread_ids=["a", "b"],
        ) == {"a": 0, "b": 0}


async def test_failed_registration_on_later_target_cleans_earlier_listeners(thread_cores):
    await thread_cores("a")

    def broken_registration():
        registry = get_handler_registry()

        def reject(*args, **kwargs):
            raise ValueError("target registration failed")

        registry.register_wait_handler = reject

    await thread_cores("b", broken_registration)
    with pytest.raises(ValueError, match="target registration failed"):
        async with expect_event_threadsafe("reply", thread_ids=["a", "b"]):
            pytest.fail("Context entry must fail")
    assert await get_thread_bridge().call_sync(
        lambda: get_event_runtime_status().wait_listeners, thread_ids=["a", "b"],
    ) == {"a": 0, "b": 0}


async def test_multitarget_predicate_error_cleans_unfinished_collection(thread_cores):
    await thread_cores("a")
    await thread_cores("b")

    def predicate(event):
        raise LookupError("invalid reply")

    async with expect_event_threadsafe("reply", thread_ids=["a", "b"], predicate=predicate) as waiter:
        await post_event_threadsafe(event_name="reply", thread_ids="a")
        with pytest.raises(LookupError, match="invalid reply"):
            await waiter.get_result()
        assert await get_thread_bridge().call_sync(
            lambda: get_event_runtime_status().wait_listeners, thread_ids=["a", "b"],
        ) == {"a": 0, "b": 0}


async def test_wait_threadsafe_accepts_targets_and_runs_predicates_there(thread_cores):
    await thread_cores("worker")
    owner = get_ident()
    seen = []

    def predicate(event):
        seen.append((factory._get_core().thread_id, get_ident()))
        return event.context == 2

    task = asyncio.create_task(wait_event_threadsafe("reply", thread_ids="worker", predicate=predicate, time_out=2))
    async with asyncio.timeout(2):
        while not await get_thread_bridge().call_sync(
            lambda: get_event_runtime_status().wait_listeners, thread_ids="worker",
        ):
            await asyncio.sleep(0)
    await post_event_threadsafe(event_name="reply", context=1, thread_ids="worker")
    await post_event_threadsafe(event_name="reply", context=2, thread_ids="worker")
    assert (await task).context == 2
    assert all(name == "worker" and ident != owner for name, ident in seen)


async def test_cross_core_roundtrip_lends_only_each_callers_permit(thread_cores):
    def setup_a():
        @subscribe("start")
        async def start(event):
            chain = handler_chain_token_context.get()
            assert chain.loop is asyncio.get_running_loop()
            reply = await post_and_wait_threadsafe(event_name="middle", context=[], thread_ids="b")
            assert chain.held
            event.context.extend(reply.context)

        @subscribe("finish")
        async def finish(event):
            event.context.append(factory._get_core().thread_id)

    def setup_b():
        @subscribe("middle")
        async def middle(event):
            chain = handler_chain_token_context.get()
            assert chain.loop is asyncio.get_running_loop()
            assert publisher_context.get() == "source"
            reply = await post_and_wait_threadsafe(event_name="finish", context=[], thread_ids="a")
            assert chain.held
            event.context.extend(["b", *reply.context])

    await thread_cores("a", setup_a)
    await thread_cores("b", setup_b)
    token = publisher_context.set("source")
    try:
        event = await asyncio.wait_for(post_and_wait_threadsafe(event_name="start", context=[], thread_ids="a"), 3)
    finally:
        publisher_context.reset(token)
    assert not event.has_error
    assert event.context == ["b", "a"]


async def test_to_thread_context_cannot_release_another_loops_permit(thread_cores):
    done = []

    @subscribe("parent")
    async def parent(event):
        chain = handler_chain_token_context.get()

        async def foreign_loop():
            assert handler_chain_token_context.get() is chain
            async with suspend_process():
                assert chain.held
                done.append(True)

        await asyncio.to_thread(lambda: asyncio.run(foreign_loop()))
        assert chain.held

    event = await post_and_wait(event_name="parent")
    assert not event.has_error
    assert done == [True]


@pytest.mark.parametrize("targets, error", [
    ([], ValueError), ([""], ValueError), ([1], TypeError), (123, TypeError),
    ("missing", RuntimeError), (["main", "missing"], RuntimeError),
])
async def test_invalid_targets_fail_before_any_publication(targets, error):
    seen = []

    @subscribe("request")
    async def receive(event):
        seen.append(event)

    with pytest.raises(error):
        await post_event_threadsafe(event_name="request", thread_ids=targets)
    assert not seen
