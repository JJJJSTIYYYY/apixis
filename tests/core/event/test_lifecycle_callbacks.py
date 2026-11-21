"""Ordered close callbacks observe the native Task after resource cleanup."""

import asyncio

import pytest

from apixis import ApixisCore, CoreClosedError, CoreState


@pytest.mark.parametrize("phase", ["created", "running", "stopped"])
async def test_close_callbacks_run_once_in_registration_order(phase):
    core = ApixisCore.create_core()
    seen = []
    core.add_close_task_done_callback(lambda task: seen.append((1, task, core.state)))
    core.add_close_task_done_callback(lambda task: seen.append((2, task, core.state)))
    try:
        for _ in range(2):
            if phase != "created":
                await core.start_core()
            if phase == "stopped":
                await core.stop_core()
        assert seen == []
        await asyncio.gather(core.close_core(), core.close_core())
        assert [entry[0] for entry in seen] == [1, 2]
        assert seen[0][1] is seen[1][1]
        assert all(task.done() and state is CoreState.CLOSED for _, task, state in seen)
        await core.close_core()
        assert len(seen) == 2
    finally:
        await core.close_core()


async def test_close_callbacks_can_be_added_and_removed_during_cleanup(monkeypatch):
    core = ApixisCore.create_core()
    entered, release = asyncio.Event(), asyncio.Event()
    original = core.event_loop.close

    async def blocked():
        entered.set()
        await release.wait()
        await original()

    monkeypatch.setattr(core.event_loop, "close", blocked)
    seen = []

    def removed(task):
        seen.append("removed")

    core.add_close_task_done_callback(lambda task: seen.append("first"))
    caller = asyncio.create_task(core.close_core())
    try:
        await entered.wait()
        core.add_close_task_done_callback(removed)
        core.add_close_task_done_callback(removed)
        core.add_close_task_done_callback(lambda task: seen.append("last"))
        assert core.remove_close_task_done_callback(removed) == 2
        assert core.remove_close_task_done_callback(removed) == 0
        assert seen == []
        release.set()
        await caller
        assert seen == ["first", "last"]
    finally:
        release.set()
        await asyncio.gather(caller, return_exceptions=True)


async def test_callback_exception_does_not_skip_later_callbacks_or_closure():
    core = ApixisCore.create_core()
    errors, seen = [], []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(context))
    error = ValueError("callback failed")

    def broken(task):
        assert task.done()
        raise error

    core.add_close_task_done_callback(broken)
    core.add_close_task_done_callback(lambda task: seen.append(task))
    try:
        await core.close_core()
        assert core.state is CoreState.CLOSED
        assert len(seen) == 1
        assert [entry["exception"] for entry in errors] == [error]
    finally:
        loop.set_exception_handler(previous)


async def test_failed_close_notifies_once_and_late_registration_is_scheduled(monkeypatch):
    core = ApixisCore.create_core()
    original = core.handler_registry.close
    error = RuntimeError("cleanup failed")
    seen = []

    async def fail():
        raise error

    monkeypatch.setattr(core.handler_registry, "close", fail)
    core.add_close_task_done_callback(lambda task: seen.append(task.exception()))
    try:
        with pytest.raises(RuntimeError) as raised:
            await core.close_core()
        assert raised.value is error
        assert core.state is CoreState.CLOSING
        assert seen == [error]
        with pytest.raises(RuntimeError):
            await core.close_core()
        assert seen == [error]
        core.add_close_task_done_callback(lambda task: seen.append("late"))
        assert seen == [error]
        await asyncio.sleep(0)
        assert seen == [error, "late"]
    finally:
        await original()


async def test_only_close_callback_registration_is_exposed_and_creates_no_tasks():
    core = ApixisCore.create_core()
    before = asyncio.all_tasks()
    with pytest.raises(TypeError):
        core.add_close_task_done_callback(None)
    core.add_close_task_done_callback(lambda task: None)
    assert asyncio.all_tasks() == before
    assert core.state is CoreState.CREATED
    for operation in ("start", "stop"):
        assert not hasattr(core, f"add_{operation}_task_done_callback")
        assert not hasattr(core, f"remove_{operation}_task_done_callback")
    await core.close_core()
    with pytest.raises(CoreClosedError):
        core.add_close_task_done_callback(lambda task: None)
