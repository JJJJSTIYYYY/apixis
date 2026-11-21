import asyncio
import itertools
import json

from apixis import ApixisCore, CoreClosedError, CoreState


async def scenario(sequence, spacing, cancel_callers):
    core = ApixisCore.create_core()
    active = set()
    original_start = core.event_loop.start
    original_stop = core.event_loop.stop

    async def start():
        assert not active, (sequence, active, "start overlap")
        active.add("start")
        try:
            await asyncio.sleep(0)
            return await original_start()
        finally:
            active.remove("start")

    async def stop():
        assert not active, (sequence, active, "stop overlap")
        active.add("stop")
        try:
            await asyncio.sleep(0)
            return await original_stop()
        finally:
            active.remove("stop")

    core.event_loop.start = start
    core.event_loop.stop = stop
    callers = []
    for index, operation in enumerate(sequence):
        task = asyncio.create_task(getattr(core, operation + "_core")())
        callers.append(task)
        for _ in range(spacing):
            await asyncio.sleep(0)
        if cancel_callers and index % 2 == 0:
            task.cancel()
    results = await asyncio.gather(*callers, return_exceptions=True)
    for result in results:
        assert result is None or isinstance(result, (CoreClosedError, asyncio.CancelledError)), repr(result)
    if core.state not in (CoreState.CLOSING, CoreState.CLOSED):
        await core.stop_core()
        assert core.state is CoreState.STOPPED
        await core.start_core()
        assert core.state is CoreState.RUNNING
        assert core.event_loop.is_running
    await core.close_core()
    assert core.state is CoreState.CLOSED
    assert not core.event_loop.is_running
    assert not core.event_loop._dispatch_tasks
    assert not core.event_loop._background_handler_tasks
    assert not active


async def main():
    loop = asyncio.get_running_loop()
    cases = 0
    for factory in (None, asyncio.eager_task_factory):
        loop.set_task_factory(factory)
        for sequence in itertools.product(("start", "stop", "close"), repeat=4):
            for spacing in (0, 1, 3):
                for cancel_callers in (False, True):
                    async with asyncio.timeout(2):
                        await scenario(sequence, spacing, cancel_callers)
                    cases += 1
    loop.set_task_factory(None)
    print(json.dumps({"scenarios": cases, "factories": ["default", "eager"], "failures": 0}))


asyncio.run(main())
