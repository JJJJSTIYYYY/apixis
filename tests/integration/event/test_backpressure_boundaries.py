"""Exercise registry changes at the smallest real queue and dispatch limits."""

import asyncio

import pytest

from apixis.core.event import (
    ApixEvent,
    ApixEventHandler,
    ApixEventPipe,
    BuiltinChannel,
    EventType,
    await_for,
    subscribe,
    unsubscribe,
    wait_for_event,
)
from apixis.core.event import factory
from apixis.core.event.base import handler_chain_context, suspend_process
from apixis.core.graph import GraphManager


pytestmark = pytest.mark.parametrize(
    "event_runtime_capacity",
    [(capacity, queue) for capacity in (1, 2, 3) for queue in (1, 2, 3)],
    indirect=True,
)


@pytest.fixture
def event_runtime_capacity(request, monkeypatch):
    """Let centralized cleanup own a fresh core with both limits set explicitly."""
    capacity, queue_capacity = request.param
    monkeypatch.setattr(
        factory,
        "ApixEventPipe",
        lambda: ApixEventPipe(builtin=BuiltinChannel(maxsize=queue_capacity)),
    )
    return capacity


@pytest.fixture
async def boundary_runtime(cleanup_event_runtime, request, wait_for_dispatch):
    core = cleanup_event_runtime
    capacity, queue_capacity = request.node.callspec.params["event_runtime_capacity"]
    assert core.event_loop._event_semaphore._bound_value == capacity
    assert core.event_loop._event_semaphore._value == capacity
    assert core.event_pipe.maxsize == queue_capacity
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    callback_errors = []
    loop.set_exception_handler(lambda _, context: callback_errors.append(context))
    try:
        yield core
    finally:
        try:
            async with asyncio.timeout(3):
                await wait_for_dispatch(core.event_loop)
                await core.event_pipe.join()
            assert not core.event_loop._dispatch_tasks
            assert not core.event_loop._background_handler_tasks
            assert core.event_loop._pending_dispatch is None
            assert core.event_pipe.empty()
            assert core.event_loop._event_semaphore._value == capacity
            assert not any(
                not waiter.done()
                for waiter in (core.event_loop._event_semaphore._waiters or ())
            )
            assert not callback_errors, callback_errors
        finally:
            loop.set_exception_handler(previous_handler)


def _event(name):
    return ApixEvent(name, EventType.TRANSIENT, name, None, 0)


def _observe_capture(monkeypatch, core, event_name):
    """Signal after handler references are captured, before capacity can wait."""
    captured = asyncio.Event()
    resolve = core.handler_registry.get_handlers_for_event

    def observe(name):
        handlers = resolve(name)
        if name == event_name:
            captured.set()
        return handlers

    monkeypatch.setattr(core.handler_registry, "get_handlers_for_event", observe)
    return captured


async def _fill_waiting_queue(core, captured):
    """Reserve the consumer's pending event, then fill every queue position."""
    await core.event_pipe.post_event(
        event_type=EventType.TRANSIENT, event_name="boundary.pending",
    )
    await captured.wait()
    assert core.event_loop._pending_dispatch[0].event_name == "boundary.pending"
    for index in range(core.event_pipe.maxsize):
        core.event_pipe.put_nowait(_event(f"boundary.queued.{index}"))
    assert core.event_pipe.full()
    assert core.event_loop._event_semaphore._value == 0


async def _cancel(tasks):
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_full_queue_and_saturated_parents_can_publish_children(
    boundary_runtime, monkeypatch, wait_for_dispatch,
):
    """Blocked external producers and handler posts must all make progress."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    parents_ready = asyncio.Barrier(capacity + 1)
    release = asyncio.Event()
    producer_entered = asyncio.Event()
    completed = []
    tasks = []
    captured = _observe_capture(monkeypatch, core, "boundary.pending")

    @subscribe("boundary.parent.*")
    async def parent(event):
        await parents_ready.wait()
        await release.wait()
        for index in range(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT,
                event_name=f"boundary.child.{event.context}.{index}",
            )
        completed.append(event.event_name)

    @subscribe("boundary.child.*", "boundary.pending", "boundary.queued.*", "boundary.external")
    async def child(event):
        completed.append(event.event_name)

    async def external_producer():
        producer_entered.set()
        await core.event_pipe.post_event(
            event_type=EventType.TRANSIENT, event_name="boundary.external",
        )

    try:
        async with asyncio.timeout(3):
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT,
                    event_name=f"boundary.parent.{index}", context=index,
                )
            await parents_ready.wait()
            await _fill_waiting_queue(core, captured)
            producer = asyncio.create_task(external_producer())
            tasks.append(producer)
            await producer_entered.wait()
            assert not producer.done()
            release.set()
            await producer
            await wait_for_dispatch(core.event_loop)
        expected = {
            *(f"boundary.parent.{index}" for index in range(capacity)),
            *(f"boundary.child.{parent}.{index}" for parent in range(capacity) for index in range(3)),
            *(f"boundary.queued.{index}" for index in range(core.event_pipe.maxsize)),
            "boundary.pending", "boundary.external",
        }
        assert set(completed) == expected
        assert len(completed) == len(expected)
    finally:
        release.set()
        await _cancel(tasks)


@pytest.mark.parametrize("restart", [False, True])
async def test_captured_handlers_survive_eviction_and_registration_with_a_full_queue(
    boundary_runtime, monkeypatch, wait_for_dispatch, restart,
):
    """Eviction and replacement cannot change a dequeued event's frozen chain."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    core.handler_registry._cache_size = 2
    blockers_ready = asyncio.Barrier(capacity + 1)
    release = asyncio.Event()
    captured = _observe_capture(monkeypatch, core, "boundary.work.captured")
    calls = []

    @subscribe("boundary.block.*")
    async def blocker(event):
        await blockers_ready.wait()
        await release.wait()

    @subscribe("boundary.work.*", priority=20)
    async def target(event):
        calls.append((event.event_name, "old"))

    @subscribe("boundary.work.*", priority=10)
    async def tail(event):
        calls.append((event.event_name, "tail"))

    try:
        async with asyncio.timeout(3):
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.block.{index}",
                )
            await blockers_ready.wait()
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.work.captured",
            )
            await captured.wait()
            assert core.event_loop._pending_dispatch[0].event_name == "boundary.work.captured"
            for index in range(core.event_pipe.maxsize):
                core.event_pipe.put_nowait(_event(f"boundary.queued.{index}"))
            assert core.event_pipe.full()
            if restart:
                await core.event_loop.stop()
                assert core.event_loop._pending_dispatch[0].event_name == "boundary.work.captured"

            # The captured list must outlive eviction of its names from the LRU.
            for index in range(3):
                core.handler_registry.get_handlers_chain_for_event(f"boundary.churn.{index}")
            assert len(core.handler_registry.cached_chain) == 2
            assert "boundary.work.captured" not in core.handler_registry.cached_chain

            @subscribe("boundary.work.*", priority=30)
            async def target(event):  # noqa: F811 - Replace the registered identity.
                calls.append((event.event_name, "new"))

            @subscribe("boundary.work.*", priority=40)
            async def late(event):
                calls.append((event.event_name, "late"))

            unsubscribe("tail")
            assert calls == []
            release.set()
            if restart:
                await core.event_loop.start()
            await wait_for_dispatch(core.event_loop)
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.work.after",
            )
            await wait_for_dispatch(core.event_loop)
        assert calls == [
            ("boundary.work.captured", "old"),
            ("boundary.work.captured", "tail"),
            ("boundary.work.after", "late"),
            ("boundary.work.after", "new"),
        ]
        assert len(core.handler_registry.cached_chain) <= 2
    finally:
        release.set()


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_nested_parallel_graphs_and_request_reply_drain_a_full_queue(
    boundary_runtime, monkeypatch, wait_for_dispatch, point,
):
    """Child graph waits and temporary reply listeners share the smallest slots."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    parents_ready = asyncio.Barrier(capacity + 1)
    release = asyncio.Event()
    captured = _observe_capture(monkeypatch, core, "boundary.pending")
    requests = []
    tasks = []

    @subscribe("boundary.tool.request")
    async def tool(event):
        requests.append(event.context)
        await core.event_pipe.post_event(
            event_type=EventType.TRANSIENT,
            event_name=f"boundary.tool.reply.{event.context}", context=event.context,
        )

    async def leaf(state):
        key = state["key"]
        async with wait_for_event(
            f"boundary.tool.reply.{key}", point=point, time_out=2,
        ) as waiter:
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.tool.request", context=key,
            )
            reply = await waiter.wait()
        return {"value": reply.context}

    leaf_graph = GraphManager().add_node(leaf).compile_graph("leaf")

    async def left(state):
        result = await leaf_graph.invoke({"key": f"{state['run']}.left"})
        return {"left": result["value"]}

    async def right(state):
        result = await leaf_graph.invoke({"key": f"{state['run']}.right"})
        return {"right": result["value"]}

    parallel_graph = GraphManager().add_node(left).add_node(right).compile_graph(["left", "right"])

    async def parent(state):
        await parents_ready.wait()
        await release.wait()
        return await parallel_graph.invoke(state)

    parent_graph = GraphManager().add_node(parent).compile_graph("parent")
    try:
        async with asyncio.timeout(3):
            for index in range(capacity):
                tasks.append(asyncio.create_task(parent_graph.invoke({"run": index})))
            await parents_ready.wait()
            await _fill_waiting_queue(core, captured)
            release.set()
            results = await asyncio.gather(*tasks)
            await wait_for_dispatch(core.event_loop)
        assert results == [
            {"run": index, "left": f"{index}.left", "right": f"{index}.right"}
            for index in range(capacity)
        ]
        assert sorted(requests) == sorted(
            f"{index}.{branch}" for index in range(capacity) for branch in ("left", "right")
        )
        assert not any(name.startswith("resolve_future-") for name in core.handler_registry.registry)
    finally:
        release.set()
        await _cancel(tasks)
        parent_graph.decompose(force=True)
        parallel_graph.decompose(force=True)
        leaf_graph.decompose(force=True)


@pytest.mark.parametrize("cancel_at", ["suspended", "reacquiring"])
async def test_cancellation_while_suspended_or_reacquiring_restores_capacity(
    boundary_runtime, monkeypatch, wait_for_dispatch, cancel_at,
):
    """Cancellation cannot double release a lent slot or resume business work."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    suspended = asyncio.Event()
    leave_body = asyncio.Event()
    body_exited = asyncio.Event()
    holders_ready = asyncio.Barrier(capacity + 1)
    release_holders = asyncio.Event()
    resumed = asyncio.Event()
    parent_task = None
    chain = None
    captured = _observe_capture(monkeypatch, core, "boundary.pending")

    @subscribe("boundary.cancel")
    async def parent(event):
        nonlocal parent_task, chain
        parent_task = asyncio.current_task()
        chain = handler_chain_context.get()
        async with suspend_process():
            suspended.set()
            try:
                await leave_body.wait()
            finally:
                body_exited.set()
        resumed.set()

    @subscribe("boundary.holder.*")
    async def holder(event):
        await holders_ready.wait()
        await release_holders.wait()

    try:
        async with asyncio.timeout(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.cancel",
            )
            await suspended.wait()
            assert chain.suspended == 1
            assert not chain.held
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.holder.{index}",
                )
            await holders_ready.wait()
            await _fill_waiting_queue(core, captured)
            if cancel_at == "reacquiring":
                leave_body.set()
                await body_exited.wait()
                # The signal is set immediately before __aexit__ waits for a slot.
                assert chain.suspended == 0
                assert chain.resume_lock.locked()
                assert not parent_task.done()
                parent_task.cancel()
                await asyncio.gather(parent_task, return_exceptions=True)
            else:
                parent_task.cancel()
                await body_exited.wait()
                assert chain.suspended == 0
                assert not resumed.is_set()
            release_holders.set()
            await wait_for_dispatch(core.event_loop)
        assert parent_task.cancelled()
        assert chain.closed
        assert not chain.held
        assert not resumed.is_set()
    finally:
        leave_body.set()
        release_holders.set()


@pytest.mark.parametrize("outcome", ["error", "timeout"])
async def test_suspended_failure_recovers_permits_and_allows_later_traffic(
    boundary_runtime, monkeypatch, wait_for_dispatch, outcome,
):
    """Exceptions and real timeout cancellation restore a chain after saturation."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    suspended = asyncio.Event()
    leave_body = asyncio.Event()
    body_exited = asyncio.Event()
    holders_ready = asyncio.Barrier(capacity + 1)
    release_holders = asyncio.Event()
    recovered = asyncio.Event()
    captured = _observe_capture(monkeypatch, core, "boundary.pending")
    chain = None
    deadline = None
    probes = []

    @subscribe("boundary.failure")
    async def parent(event):
        nonlocal chain, deadline
        chain = handler_chain_context.get()
        try:
            async with asyncio.timeout(None) as timeout:
                deadline = timeout
                async with suspend_process():
                    suspended.set()
                    try:
                        await leave_body.wait()
                        raise ValueError("suspended operation failed")
                    finally:
                        body_exited.set()
        except (ValueError, TimeoutError):
            assert chain.held
            recovered.set()

    @subscribe("boundary.holder.*")
    async def holder(event):
        await holders_ready.wait()
        await release_holders.wait()

    @subscribe("boundary.probe.*")
    async def probe(event):
        probes.append(event.event_name)

    try:
        async with asyncio.timeout(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.failure",
            )
            await suspended.wait()
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.holder.{index}",
                )
            await holders_ready.wait()
            await _fill_waiting_queue(core, captured)
            if outcome == "timeout":
                deadline.reschedule(asyncio.get_running_loop().time())
            else:
                leave_body.set()
            await body_exited.wait()
            assert chain.suspended == 0
            assert not recovered.is_set()
            release_holders.set()
            await recovered.wait()
            await wait_for_dispatch(core.event_loop)
            for index in range(2 * capacity + 1):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.probe.{index}",
                )
            await wait_for_dispatch(core.event_loop)
        assert probes == [f"boundary.probe.{index}" for index in range(2 * capacity + 1)]
        assert chain.closed
        assert not chain.held
    finally:
        leave_body.set()
        release_holders.set()


@pytest.mark.parametrize("cancel_owner", ["never", "body", "resume"])
@pytest.mark.parametrize("previous_cancel", [False, True])
async def test_parent_catching_repeated_parallel_child_cancellation_keeps_its_permit(
    boundary_runtime, monkeypatch, wait_for_dispatch, cancel_owner, previous_cancel,
):
    """A surviving dispatch cannot continue after children abandon reacquisition."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    entered = asyncio.Queue()
    leaving = asyncio.Queue()
    holders_ready = asyncio.Barrier(capacity + 1)
    release_holders = asyncio.Event()
    release_parent = asyncio.Event()
    continued = asyncio.Event()
    branches = []
    chain = None
    parent_task = None
    captured = _observe_capture(monkeypatch, core, "boundary.pending")

    async def branch():
        async with suspend_process():
            entered.put_nowait(None)
            try:
                await asyncio.Future()
            finally:
                leaving.put_nowait(None)

    @subscribe("boundary.parallel.cancel")
    async def parent(event):
        nonlocal chain, parent_task
        parent_task = asyncio.current_task()
        chain = handler_chain_context.get()
        if previous_cancel:
            parent_task.cancel()
            try:
                # A deliberately suppressed cancellation must also clear its
                # request count before this owner starts further async work.
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                assert parent_task.uncancel() == 0
            assert parent_task.cancelling() == 0
        branches.extend(asyncio.create_task(branch()) for _ in range(3))
        results = await asyncio.gather(*branches, return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        continued.set()
        await release_parent.wait()

    @subscribe("boundary.holder.*")
    async def holder(event):
        await holders_ready.wait()
        await release_holders.wait()

    try:
        async with asyncio.timeout(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.parallel.cancel",
            )
            for _ in range(3):
                await entered.get()
            assert chain.suspended == 3
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.holder.{index}",
                )
            await holders_ready.wait()
            await _fill_waiting_queue(core, captured)
            if cancel_owner == "body":
                # First cancellation is delivered in the child bodies; the
                # owner must finish without needing a second cancellation.
                parent_task.cancel()
                await asyncio.gather(parent_task, return_exceptions=True)
                assert parent_task.cancelled()
                assert all(task.cancelled() for task in branches)
                assert not continued.is_set()
                release_holders.set()
                await wait_for_dispatch(core.event_loop)
                return
            for task in branches:
                task.cancel()
            for _ in branches:
                await leaving.get()
            assert chain.suspended == 0
            assert chain.resume_lock.locked()
            assert not continued.is_set()
            if cancel_owner == "resume":
                # Cancelling the dispatch must finish while holders still own
                # every permit, even if its child is already reacquiring.
                parent_task.cancel()
                await asyncio.gather(parent_task, return_exceptions=True)
                assert parent_task.cancelled()
                assert not continued.is_set()
                release_holders.set()
            else:
                # The final child is now waiting to reacquire; cancel it again.
                for task in branches:
                    task.cancel()
                # Give cancellation and gather's completion callbacks separate
                # scheduling turns; a correct reacquisition may remain blocked.
                for _ in range(2):
                    turn = asyncio.Event()
                    asyncio.get_running_loop().call_soon(turn.set)
                    await turn.wait()
                assert not continued.is_set()
                release_holders.set()
                await asyncio.gather(*branches, return_exceptions=True)
                await continued.wait()
                assert chain.held
                release_parent.set()
            await wait_for_dispatch(core.event_loop)
    finally:
        release_holders.set()
        release_parent.set()
        await _cancel(branches)


async def test_dispatch_finish_cancels_detached_child_already_reacquiring(
    boundary_runtime, monkeypatch, wait_for_dispatch,
):
    """Closing a dispatch removes a child's waiter while all slots stay busy."""
    core = boundary_runtime
    semaphore = core.event_loop._event_semaphore
    capacity = semaphore._bound_value
    entered = asyncio.Event()
    leave_body = asyncio.Event()
    body_exited = asyncio.Event()
    finish_parent = asyncio.Event()
    holders_ready = asyncio.Barrier(capacity + 1)
    release_holders = asyncio.Event()
    captured = _observe_capture(monkeypatch, core, "boundary.pending")
    child_task = None
    parent_task = None
    chain = None

    async def child():
        async with suspend_process():
            entered.set()
            await leave_body.wait()
            body_exited.set()

    @subscribe("boundary.detached.parent")
    async def parent(event):
        nonlocal child_task, parent_task, chain
        parent_task = asyncio.current_task()
        chain = handler_chain_context.get()
        child_task = asyncio.create_task(child())
        await entered.wait()
        await finish_parent.wait()

    @subscribe("boundary.holder.*")
    async def holder(event):
        await holders_ready.wait()
        await release_holders.wait()

    try:
        async with asyncio.timeout(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.detached.parent",
            )
            await entered.wait()
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.holder.{index}",
                )
            await holders_ready.wait()
            await _fill_waiting_queue(core, captured)
            consumer_waiters = tuple(semaphore._waiters or ())
            assert len(consumer_waiters) == 1
            leave_body.set()
            await body_exited.wait()
            assert chain.resuming_task is child_task
            assert chain.resume_lock.locked()
            assert not child_task.done()
            assert len(semaphore._waiters) == len(consumer_waiters) + 1
            finish_parent.set()
            # Both tasks must end before any saturated holder is released.
            await parent_task
            [result] = await asyncio.gather(child_task, return_exceptions=True)
            assert isinstance(result, asyncio.CancelledError)
            assert child_task.cancelled()
            assert chain.closed
            assert not chain.held
            assert chain.owner is None
            assert chain.resuming_task is None
            assert semaphore._value == 0
            assert tuple(semaphore._waiters) == consumer_waiters
            assert len(core.event_loop._dispatch_tasks) == capacity
            release_holders.set()
            await wait_for_dispatch(core.event_loop)
    finally:
        leave_body.set()
        finish_parent.set()
        release_holders.set()
        if child_task is not None:
            await _cancel([child_task])


async def test_recoverable_owner_timeout_suspends_the_complete_parallel_wait(
    boundary_runtime, monkeypatch, wait_for_dispatch,
):
    """An outer suspension restores capacity before catching a parallel timeout."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    entered = asyncio.Queue()
    leaving = asyncio.Queue()
    holders_ready = asyncio.Barrier(capacity + 1)
    release_holders = asyncio.Event()
    recovered = asyncio.Event()
    captured = _observe_capture(monkeypatch, core, "boundary.pending")
    deadline = None
    chain = None
    branches = []

    async def branch():
        entered.put_nowait(None)
        try:
            await asyncio.Future()
        finally:
            leaving.put_nowait(None)

    @subscribe("boundary.recoverable.timeout")
    async def parent(event):
        nonlocal deadline, chain
        chain = handler_chain_context.get()
        try:
            async with suspend_process():
                async with asyncio.timeout(None) as timeout:
                    deadline = timeout
                    branches.extend(asyncio.create_task(branch()) for _ in range(3))
                    await asyncio.gather(*branches)
        except TimeoutError:
            assert chain.held
            assert asyncio.current_task().cancelling() == 0
            recovered.set()

    @subscribe("boundary.holder.*")
    async def holder(event):
        await holders_ready.wait()
        await release_holders.wait()

    try:
        async with asyncio.timeout(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.recoverable.timeout",
            )
            for _ in range(3):
                await entered.get()
            assert chain.suspended == 1
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.holder.{index}",
                )
            await holders_ready.wait()
            await _fill_waiting_queue(core, captured)
            deadline.reschedule(asyncio.get_running_loop().time())
            for _ in range(3):
                await leaving.get()
            assert not recovered.is_set()
            release_holders.set()
            await recovered.wait()
            await wait_for_dispatch(core.event_loop)
        assert all(task.cancelled() for task in branches)
        assert chain.closed
        assert not chain.held
    finally:
        release_holders.set()
        await _cancel(branches)


@pytest.mark.parametrize("recovery_point", ["on_error", "next_handler"])
async def test_handler_timeout_reacquires_before_its_error_callback(
    boundary_runtime, monkeypatch, wait_for_dispatch, recovery_point,
):
    """Handler-managed timeout restores capacity before error recovery continues."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    entered = asyncio.Queue()
    leaving = asyncio.Queue()
    holders_ready = asyncio.Barrier(capacity + 1)
    release_holders = asyncio.Event()
    callback_started = asyncio.Event()
    finish_callback = asyncio.Event()
    captured = _observe_capture(monkeypatch, core, "boundary.pending")
    timeout_factory = asyncio.timeout
    deadline = None
    chain = None
    branches = []
    observed = []

    def observe_timeout(delay):
        nonlocal deadline
        timeout = timeout_factory(delay)
        if delay == 60 and deadline is None:
            deadline = timeout
        return timeout

    monkeypatch.setattr(asyncio, "timeout", observe_timeout)

    async def branch():
        async with suspend_process():
            entered.put_nowait(None)
            try:
                await asyncio.Future()
            finally:
                leaving.put_nowait(None)

    async def parent(event):
        nonlocal chain
        chain = handler_chain_context.get()
        branches.extend(asyncio.create_task(branch()) for _ in range(3))
        await asyncio.gather(*branches)

    async def on_error(event, error):
        observed.append((type(error).__name__, chain.held))
        callback_started.set()
        await finish_callback.wait()

    subscribe("boundary.handler.timeout")(
        ApixEventHandler(
            parent, on_error=on_error if recovery_point == "on_error" else None, time_out=60,
        ),
    )

    if recovery_point == "next_handler":
        @subscribe("boundary.handler.timeout", priority=0, stop_when_error=False)
        async def recover(event):
            observed.append((event.error_stack[-1].exception_type, chain.held))
            callback_started.set()
            await finish_callback.wait()

    @subscribe("boundary.holder.*")
    async def holder(event):
        await holders_ready.wait()
        await release_holders.wait()

    try:
        async with asyncio.timeout(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.handler.timeout",
            )
            for _ in range(3):
                await entered.get()
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.holder.{index}",
                )
            await holders_ready.wait()
            await _fill_waiting_queue(core, captured)
            deadline.reschedule(asyncio.get_running_loop().time())
            for _ in range(3):
                await leaving.get()
            for _ in range(2):
                turn = asyncio.Event()
                asyncio.get_running_loop().call_soon(turn.set)
                await turn.wait()
            assert not callback_started.is_set()
            release_holders.set()
            await callback_started.wait()
            assert observed == [("TimeoutError", True)]
            finish_callback.set()
            await wait_for_dispatch(core.event_loop)
        assert all(task.cancelled() for task in branches)
        assert chain.closed
        assert not chain.held
    finally:
        release_holders.set()
        finish_callback.set()
        await _cancel(branches)


@pytest.mark.parametrize("point", ["received", "processed"])
async def test_waiter_expiry_while_saturated_restores_capacity_and_removes_listener(
    boundary_runtime, monkeypatch, wait_for_dispatch, point,
):
    """Both waiter positions recover after timer expiry during a full queue."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    waiting = asyncio.Event()
    expired = asyncio.Event()
    recovered = asyncio.Event()
    holders_ready = asyncio.Barrier(capacity + 1)
    release_holders = asyncio.Event()
    captured = _observe_capture(monkeypatch, core, "boundary.pending")
    waiter = None
    chain = None

    @subscribe("boundary.waiter.expiry")
    async def parent(event):
        nonlocal waiter, chain
        chain = handler_chain_context.get()
        try:
            async with wait_for_event(
                "boundary.reply.never", point=point, time_out=60,
            ) as active_waiter:
                waiter = active_waiter
                waiting.set()
                await waiter.wait(time_out=60)
        except TimeoutError:
            assert chain.held
            recovered.set()

    @subscribe("boundary.holder.*")
    async def holder(event):
        await holders_ready.wait()
        await release_holders.wait()

    try:
        async with asyncio.timeout(3):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name="boundary.waiter.expiry",
            )
            await waiting.wait()
            assert chain.suspended == 1
            for index in range(capacity):
                await core.event_pipe.post_event(
                    event_type=EventType.TRANSIENT, event_name=f"boundary.holder.{index}",
                )
            await holders_ready.wait()
            await _fill_waiting_queue(core, captured)
            # Expire through the actual timer callback without a wall-clock
            # readiness delay. The task resumes before this added callback.
            waiter._future.add_done_callback(lambda _: expired.set())
            waiter._timeout_handle.cancel()
            waiter._start_timeout(0)
            await expired.wait()
            assert chain.suspended == 0
            assert chain.resume_lock.locked()
            assert not recovered.is_set()
            release_holders.set()
            await recovered.wait()
            await wait_for_dispatch(core.event_loop)
        assert waiter._closed
        assert not any(name.startswith("resolve_future-") for name in core.handler_registry.registry)
        assert core.handler_registry.get_handlers_chain_for_event("boundary.reply.never") == []
    finally:
        release_holders.set()


@pytest.mark.parametrize("interface", ["wait", "await_for"])
@pytest.mark.parametrize("point", ["received", "processed"])
@pytest.mark.parametrize("resume", ["wait", "get_result"])
async def test_deferred_results_release_and_restore_saturated_parent_permits(
    boundary_runtime, wait_for_dispatch, interface, point, resume,
):
    """Detached collectors finish with every parent slot and a bounded queue."""
    core = boundary_runtime
    capacity = core.event_loop._event_semaphore._bound_value
    parents_ready = asyncio.Barrier(capacity)
    done = asyncio.Queue()

    @subscribe("boundary.deferred.request")
    async def worker(event):
        prefix = event.context
        for suffix in ("item.first", "item.duplicate", "saved"):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT, event_name=f"{prefix}.{suffix}",
            )

    @subscribe("boundary.deferred.parent")
    async def parent(event):
        chain = handler_chain_context.get()
        prefix = f"boundary.result.{event.context}"
        names = [f"{prefix}.item.*", f"{prefix}.saved"]
        waiter = None
        try:
            await parents_ready.wait()
            if interface == "wait":
                async with wait_for_event(names, point=point) as waiter:
                    assert await waiter.wait(time_out=0, keep_listening=True) is waiter
            else:
                waiter = await await_for(names, point=point, time_out=0, keep_listening=True)
            assert chain.held
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT,
                event_name="boundary.deferred.request", context=prefix,
            )
            result = (
                await waiter.wait(time_out=2) if resume == "wait"
                else await waiter.get_result()
            )
            assert chain.held
            assert result[f"{prefix}.item.*"].event_name == f"{prefix}.item.first"
            assert result[f"{prefix}.saved"].event_name == f"{prefix}.saved"
            done.put_nowait(event.context)
        finally:
            if waiter is not None:
                waiter.close()

    async with asyncio.timeout(3):
        for index in range(capacity):
            await core.event_pipe.post_event(
                event_type=EventType.TRANSIENT,
                event_name="boundary.deferred.parent", context=index,
            )
        assert sorted([await done.get() for _ in range(capacity)]) == list(range(capacity))
        await wait_for_dispatch(core.event_loop)
    assert not any(name.startswith("resolve_future-") for name in core.handler_registry.registry)
