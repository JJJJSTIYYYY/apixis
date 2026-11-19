"""Business correlation, predicate failures and owner-loop execution."""

import asyncio
from contextlib import AsyncExitStack
from functools import partial
from threading import get_ident

import pytest

from apixis import (
    EventWaiter, expect_event, expect_event_threadsafe, post_event, subscribe,
    wait_event, wait_event_threadsafe,
)
from apixis.core.event.subscription import waiter as waiter_module


@pytest.mark.parametrize("point", ["received", "processed"])
@pytest.mark.parametrize("patterns", ["job.reply", ["job.*"], {"job.*", "*.reply"}])
async def test_predicates_isolate_concurrent_same_name_replies(
    cleanup_event_runtime, point, patterns,
):
    async with AsyncExitStack() as stack:
        waiters = [
            await stack.enter_async_context(expect_event(
                patterns, point=point,
                predicate=lambda event, wanted=index: event.context["request_id"] == wanted,
            ))
            for index in range(4)
        ]
        for index in reversed(range(4)):
            await post_event(event_name="job.reply", context={"request_id": index})
        results = await asyncio.wait_for(asyncio.gather(*(waiter.wait() for waiter in waiters)), 1)
        for index, result in enumerate(results):
            if isinstance(patterns, str):
                assert result.context["request_id"] == index
            else:
                assert set(result) == set(patterns)
                assert all(event.context["request_id"] == index for event in result.values())
                assert len({id(event) for event in result.values()}) == 1
    assert not cleanup_event_runtime.handler_registry.registry


async def test_name_filters_run_before_predicate(cleanup_event_runtime):
    called = []

    def predicate(event):
        called.append(event.event_name)
        return True

    async with expect_event("job.*", filter="job.internal", predicate=predicate) as waiter:
        for name in ("other", "job.internal", "job.reply"):
            await post_event(event_name=name)
        assert (await asyncio.wait_for(waiter.get_result(), 1)).event_name == "job.reply"
    assert called == ["job.reply"]


async def test_predicate_preserves_first_matches_across_rejected_events(cleanup_event_runtime):
    waiter = EventWaiter(
        ["job.*", "saved"], predicate=lambda event: event.context["request_id"] == 7,
        time_out=1,
    )
    try:
        for name, request_id, value in (
            ("job.reply", 0, "wrong"), ("job.reply", 7, "first"),
            ("job.reply", 7, "later"), ("saved", 0, "wrong"), ("saved", 7, "saved"),
        ):
            await post_event(event_name=name, context={"request_id": request_id, "value": value})
        result = await asyncio.wait_for(waiter.get_result(), 1)
        assert result["job.*"].context["value"] == "first"
        assert result["saved"].context["value"] == "saved"
        assert not cleanup_event_runtime.handler_registry.registry
    finally:
        waiter.close()


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_predicate_failure_reaches_waiter_without_failing_event_dispatch(
    cleanup_event_runtime, wait_for_dispatch, point,
):
    failure = ValueError("predicate failed")
    handled = []

    def predicate(event):
        raise failure

    @subscribe("job.reply")
    async def ordinary(event):
        handled.append(event)

    async with expect_event("job.reply", point=point, predicate=predicate) as failing:
        async with expect_event("job.reply", point="processed") as independent:
            await post_event(event_name="job.reply")
            with pytest.raises(ValueError) as raised:
                await asyncio.wait_for(failing.wait(), 1)
            assert raised.value is failure
            result = await asyncio.wait_for(independent.get_result(), 1)
            assert not result.has_error
    await wait_for_dispatch(cleanup_event_runtime.event_loop)
    assert handled == [result]
    assert set(cleanup_event_runtime.handler_registry.registry) == {"ordinary"}


async def async_predicate(event):
    return True


class AsyncPredicate:
    async def __call__(self, event):
        return True


@pytest.mark.parametrize("predicate", [0, "predicate", async_predicate, partial(async_predicate), AsyncPredicate()])
async def test_invalid_predicate_fails_before_registration(monkeypatch, predicate):
    monkeypatch.setattr(waiter_module, "get_handler_registry", lambda: pytest.fail("registry accessed"))
    with pytest.raises(TypeError, match="predicate"):
        EventWaiter("job.reply", predicate=predicate)


@pytest.mark.parametrize("use_context", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("fail", [False, True])
async def test_worker_predicate_runs_on_owner_loop_and_propagates_result_or_error(
    cleanup_event_runtime, use_context, explicit, fail,
):
    core = cleanup_event_runtime
    owner = get_ident()
    seen = []
    failure = LookupError("worker predicate failed")
    expect = expect_event_threadsafe if explicit else expect_event
    wait = wait_event_threadsafe if explicit else wait_event

    def predicate(event):
        seen.append((get_ident(), event.context))
        if event.context == 7 and fail:
            raise failure
        return event.context == 7

    async def worker():
        if use_context:
            async with expect("job.reply", predicate=predicate, time_out=3) as waiter:
                return await waiter.wait()
        return await wait("job.reply", predicate=predicate, time_out=3)

    task = asyncio.create_task(
        asyncio.to_thread(lambda: asyncio.run(worker())) if explicit else worker(),
    )
    try:
        async with asyncio.timeout(3):
            while not core.handler_registry.waiter_count:
                await asyncio.sleep(0)
        await post_event(event_name="job.reply", context=0)
        await post_event(event_name="job.reply", context=7)
        if fail:
            with pytest.raises(LookupError) as raised:
                await asyncio.wait_for(task, 3)
            assert raised.value is failure
        else:
            assert (await asyncio.wait_for(task, 3)).context == 7
        assert seen == [(owner, 0), (owner, 7)]
        async with asyncio.timeout(3):
            while core.endpoint.get_status().active_requests:
                await asyncio.sleep(0)
        assert not core.handler_registry.registry
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
