"""Breakpoint and external-resume tests for graph interruption."""

import asyncio
import time

import pytest

from apixis.core.graph import Command

from apixis.core.event import ApixEvent, EventType, unsubscribe
from apixis.core.event.factory import get_handler_registry
from apixis.core.graph import GLOBALNS, GraphManager
from apixis.core.graph.context import apix_graph_context
from apixis.core.graph.interrupter import Block, interrupt, interrupted_hook
from apixis.core.graph.utils.namespace import get_graph_interrupted_name


pytestmark = pytest.mark.asyncio(loop_scope="session")


def _block(*, data=None) -> Block:
    """Create one block on the currently running test loop."""
    return Block(
        run_id="graph-run",
        block_id="block-id",
        namespace="unit",
        with_data=data,
        _future=asyncio.get_running_loop().create_future(),
    )


async def test_block_resolves_once_and_exposes_completion_state():
    """The first external answer wins and duplicate answers are harmless."""
    block = _block(data={"question": "continue?"})

    async def wait_for_block():
        return await block

    waiter = asyncio.create_task(wait_for_block())
    await asyncio.sleep(0)

    assert block.done is False
    assert block.cancelled is False

    block.resolve("yes")
    block.resolve("ignored")
    block.cancel()

    assert await waiter == "yes"
    assert block.done is True
    assert block.cancelled is False


async def test_block_rejects_an_invalid_future():
    """A malformed public Block fails immediately instead of on first await."""
    with pytest.raises(TypeError, match="must be an asyncio.Future"):
        Block(
            run_id="graph-run",
            block_id="block-id",
            namespace="unit",
            with_data=None,
            _future=None,  # type: ignore[arg-type]
        )


async def test_interrupt_requires_an_active_graph_node_context():
    """A breakpoint cannot be created outside an active node invocation."""
    with pytest.raises(RuntimeError, match="only available while a graph is invoked"):
        await interrupt()

    with apix_graph_context(
        GraphManager().compile_graph(entry_point=None).create_context({})
    ):
        with pytest.raises(RuntimeError, match="active graph node"):
            await interrupt()


async def test_interrupted_hook_rejects_non_block_event_context():
    """The public hook boundary validates the event transport payload."""
    received = []

    async def invalid_context_hook(block: Block) -> None:
        received.append(block)

    decorated = interrupted_hook()(invalid_context_hook)
    assert decorated is invalid_context_hook

    try:
        [handler_name] = get_handler_registry().get_handlers_chain_for_event(
            get_graph_interrupted_name(GLOBALNS, missing_ok=True)
        )
        handler = get_handler_registry().get_handler(handler_name)
        event = ApixEvent(
            event_id="event-id",
            event_type=EventType.WORKFLOW,
            event_name=get_graph_interrupted_name(GLOBALNS, missing_ok=True),
            context={},
            timestamp=time.time(),
        )

        await handler.execute(event)
        [error] = event.error_stack
        assert error.exception_type == "TypeError"
        assert "must carry a Block" in error.message
        assert received == []
    finally:
        unsubscribe(invalid_context_hook.__name__)


@pytest.mark.parametrize("namespace", [None, "", "<global>", "review-flow"])
async def test_graph_pauses_and_resumes_at_multiple_breakpoints(namespace):
    """One node may pause repeatedly without mixing block identity or input."""
    blocks: asyncio.Queue[Block] = asyncio.Queue()

    async def review(state):
        first = await interrupt(data={"step": 1})
        second = await interrupt(data={"step": 2, "first": first})
        return {"answers": [first, second]}

    graph = (
        GraphManager()
        .add_node(review)
        .compile_graph(entry_point="review", using_namespace=namespace)
    )

    @graph.add_interrupted_hook
    async def capture_review_block(block: Block) -> None:
        # Claim responsibility before handing the block to an external consumer.
        block.accept()
        await blocks.put(block)

    context = graph.create_context({})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))

    first = await asyncio.wait_for(blocks.get(), timeout=1)
    assert invocation.done() is False
    assert first.run_id == context.run_id
    assert first.namespace == graph.namespace
    assert first.with_data == {"step": 1}
    first.resolve("approved")

    second = await asyncio.wait_for(blocks.get(), timeout=1)
    assert invocation.done() is False
    assert second.run_id == first.run_id
    assert second.block_id != first.block_id
    assert second.with_data == {"step": 2, "first": "approved"}
    second.resolve({"edited": True})

    assert await asyncio.wait_for(invocation, timeout=1) == {
        "answers": ["approved", {"edited": True}],
    }

    handler_name = get_graph_interrupted_name(graph)
    assert handler_name in get_handler_registry().registry
    graph.decompose()
    assert handler_name not in get_handler_registry().registry

    with pytest.raises(RuntimeError, match="NodeGraph has been decomposed"):
        graph.add_interrupted_hook(capture_review_block)


@pytest.mark.parametrize("namespace", [None, "", GLOBALNS])
async def test_public_global_hook_resumes_default_graph(namespace):
    """A standalone global hook resumes an explicitly global graph."""

    async def review(state):
        return {"answer": await interrupt()}

    graph = (
        GraphManager()
        .add_node(review)
        .compile_graph(entry_point="review", using_namespace=GLOBALNS)
    )

    @interrupted_hook(namespace=namespace, exist_ok=False)
    async def resolve_global_block(block):
        assert block.namespace == GLOBALNS
        block.resolve("approved")

    try:
        result = await asyncio.wait_for(graph.invoke({}), timeout=1)
        assert result == {"answer": "approved"}
    finally:
        unsubscribe(resolve_global_block.__name__)


async def test_external_block_cancel_aborts_graph_at_saved_snapshot():
    """Cancelling a breakpoint stops its node and all downstream routing."""
    blocks: asyncio.Queue[Block] = asyncio.Queue()
    continued = asyncio.Event()
    downstream_called = asyncio.Event()

    def prepare(state):
        return Command(update={"checkpoint": "prepared"}, goto="wait_for_decision")

    async def wait_for_decision(state):
        await interrupt(data="optional")
        continued.set()
        return Command(update={"decision": "continued"}, goto="downstream")

    def downstream(state):
        downstream_called.set()
        return {"downstream": True}

    graph = (
        GraphManager()
        .add_nodes([prepare, wait_for_decision, downstream])

        .compile_graph(entry_point="prepare", using_namespace="cancel-flow")
    )

    @graph.add_interrupted_hook
    async def capture_cancelled_block(block: Block) -> None:
        # Claim responsibility before handing the block to an external consumer.
        block.accept()
        await blocks.put(block)

    context = graph.create_context({"initial": True})
    invocation = asyncio.create_task(graph.invoke(graph_context=context))
    block = await asyncio.wait_for(blocks.get(), timeout=1)
    block.cancel()

    assert await asyncio.wait_for(invocation, timeout=1) == {
        "initial": True,
        "checkpoint": "prepared",
    }
    await asyncio.sleep(0)

    assert block.done is True
    assert block.cancelled is True
    assert context.status == "aborted"
    assert context.target_node_name == "wait_for_decision"
    assert continued.is_set() is False
    assert downstream_called.is_set() is False


async def test_interrupt_timeout_resumes_graph_and_cancels_block():
    """A breakpoint deadline resumes with None and closes late resolution."""
    blocks: asyncio.Queue[Block] = asyncio.Queue()

    async def wait_briefly(state):
        return {"answer": await interrupt(data="timed", timeout=0.01)}

    graph = (
        GraphManager()
        .add_node(wait_briefly)
        .compile_graph(entry_point="wait_briefly", using_namespace="timeout-flow")
    )

    @graph.add_interrupted_hook
    async def capture_timed_block(block: Block) -> None:
        # Claim responsibility before handing the block to an external consumer.
        block.accept()
        await blocks.put(block)

    invocation = asyncio.create_task(graph.invoke({}))
    block = await asyncio.wait_for(blocks.get(), timeout=1)

    assert await asyncio.wait_for(invocation, timeout=1) == {"answer": None}
    assert block.cancelled is True
    block.resolve("too late")


async def test_node_timeout_is_not_swallowed_by_interrupt_cancellation():
    """Runtime cancellation remains distinct from external Block.cancel()."""
    blocks: asyncio.Queue[Block] = asyncio.Queue()

    async def wait_forever(state):
        await interrupt(data="timed node")
        return {}

    graph = (
        GraphManager()
        .add_node(wait_forever, timeout=0.02)
        .compile_graph(entry_point="wait_forever", using_namespace="node-timeout-flow")
    )

    @graph.add_interrupted_hook
    async def capture_node_timeout_block(block: Block) -> None:
        # Claim responsibility before handing the block to an external consumer.
        block.accept()
        await blocks.put(block)

    invocation = asyncio.create_task(graph.invoke({}))
    block = await asyncio.wait_for(blocks.get(), timeout=1)

    with pytest.raises(
        TimeoutError,
        match=r"Graph node `wait_forever` timed out after 0.02 seconds",
    ):
        await asyncio.wait_for(invocation, timeout=1)
    assert block.cancelled is True


async def test_block_fail_propagates_exception_and_preserves_first_completion():
    """Failure completes a pending block once and retains the original error."""
    loop = asyncio.get_running_loop()
    block = Block("run", "block", "", None, loop.create_future())
    error = ValueError("interruption failed")
    block.fail(error)
    block.fail(RuntimeError("later error"))
    block.resolve("late result")
    with pytest.raises(ValueError) as raised:
        await block
    assert raised.value is error
    assert block.done and not block.cancelled

    resolved = Block("run", "resolved", "", None, loop.create_future())
    resolved.resolve("first result")
    resolved.fail(error)
    assert await resolved == "first result"
