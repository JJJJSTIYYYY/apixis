import asyncio
import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def use_eager_factory(cleanup_event_runtime):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        yield
    finally:
        loop.set_task_factory(previous)
