import asyncio
from collections.abc import Awaitable
from functools import wraps
from uuid import uuid4

from typing import Any, Callable

from apixis.core.event.base import suspend_process
from apixis.core.graph.context.manager import get_graph_context
from apixis.core.graph.context.graph_context import GraphContext
from apixis.core.graph.utils.namespace import (
    get_graph_interrupted_name,
    get_graph_namespace,
)
from apixis.core.event import ApixEvent, ApixEventHandler, EventType, subscribe
from apixis.core.event.factory import aget_event_pipe
from apixis.core.graph.interrupter.block import Block
from apixis.core.utils.exception import GraphNodeError


InterruptedHandler = Callable[[Block], Awaitable[None]]


async def interrupt(
    *,
    data: Any = None,
    timeout: float | None = None,
    context: GraphContext | None = None,
) -> Any:
    """
    Send data and while in graph loop and block the agent graph at the same time.
    Can be called from inside any graph node.

    Args:
        data: Optional chunk data.
        timeout: Optional timeout in seconds for the blocking wait. If None, wait indefinitely.
        context: Optional graph context, if the interruption is not within a :class:`NodeFunction`,
            it is required.

    This method will post a :class:`Block` by event pipe, not by stream writer.
    To receive a :class:`Block` posted by this method, it is required to register a handler
    for get_graph_interrupted_name(namespace) and get the Block from event context.

    Raises:
        BlockHookNotRegisteredError: If the graph's default interruption handler
            finds no preceding core execution in event.seen. Register a hook
            through graph.add_interrupted_hook() or interrupted_hook() so it
            executes before the default handler.
    """

    if context is None:
        try:
            context = get_graph_context()
        except RuntimeError as exc:
            raise RuntimeError(
                "interrupt() is only available while a graph is invoked."
            ) from exc

    if not context.is_active:
        raise RuntimeError(
            "interrupt() is only available while an active graph node is executed."
        )

    run_id = context.run_id
    namespace = get_graph_namespace(context.graph_id)
    assert run_id is not None

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    block = Block(
        run_id=run_id,
        block_id=uuid4().hex,
        namespace=namespace,
        with_data=data,
        _future=future,
        graph_id=context.graph_id,
    )
    completion = context.completion
    if completion is None:
        raise RuntimeError("Completion in GraphContext could not be None.")

    def close_block(_):
        """A waiting interruption cannot outlive its invocation."""
        block.cancel()

    completion.add_done_callback(close_block)
    try:
        await (await aget_event_pipe()).post_event(
            event_type=EventType.WORKFLOW,
            event_name=get_graph_interrupted_name(namespace, missing_ok=True),
            context=block,
        )
        async with suspend_process():
            if timeout is None:
                return await block
            timeout_scope = asyncio.timeout(timeout)
            try:
                async with timeout_scope:
                    return await block
            except TimeoutError:
                # A hook may fail the block with TimeoutError itself. Only this
                # interruption's own deadline is converted into a None result.
                if not timeout_scope.expired():
                    raise
                return None
    except asyncio.CancelledError:
        # External ``Block.cancel()`` aborts the owning graph attempt at its
        # last committed snapshot. The CancelledError is then re-raised to
        # stop the interrupted node immediately, so neither its remaining
        # code nor a downstream node can run. Runtime task cancellation is
        # left to the surrounding graph timeout/cancellation machinery.
        current_task = asyncio.current_task()
        if block.cancelled and (current_task is None or current_task.cancelling() == 0):
            if context.status == "running":
                context.abort()
        raise
    finally:
        completion.remove_done_callback(close_block)
        if not block.done:
            block.cancel()


class BlockEventHandler(ApixEventHandler):
    """Adapt a Block callback and its event lifecycle notifications together.

    Both graph defaults and user hooks use this adapter for lifecycle cleanup.
    The default handler uses event.seen to detect prior core execution,
    regardless of whether the preceding handler uses this adapter.
    """

    def __init__(self, func: InterruptedHandler) -> None:
        if not callable(func):
            raise TypeError("Interrupted hook must be callable.")

        @wraps(func)
        async def dispatch_block(event: ApixEvent) -> None:
            block = event.context
            if not isinstance(block, Block):
                raise TypeError("Interrupted graph events must carry a Block context.")
            if not block.done:
                await func(block)

        async def on_failure(event: ApixEvent, error: Exception) -> None:
            """Deliver this handler's own failure to its waiting node."""
            block = event.context
            if isinstance(block, Block):
                block.fail(error)

        async def on_has_error(event: ApixEvent) -> None:
            """Deliver upstream failures to the node awaiting this block."""
            block = event.context
            if isinstance(block, Block):
                block.fail(
                    GraphNodeError(
                        "Graph interruption failed in a preceding event handler",
                        errors=list(event.error_stack),
                    )
                )

        async def on_accepted(event: ApixEvent) -> None:
            """Cancel an unhandled block when its event is accepted upstream."""
            block = event.context
            if isinstance(block, Block) and not block.done:
                block.cancel()

        async def on_cancelled(event: ApixEvent) -> None:
            """Propagate runtime cancellation without treating it as a user abort."""
            block = event.context
            if isinstance(block, Block):
                block.fail(asyncio.CancelledError())

        super().__init__(
            dispatch_block,
            on_accepted=on_accepted,
            on_has_error=on_has_error,
            on_error=on_failure,
            on_cancelled=on_cancelled,
        )


def interrupted_hook(
    namespace: str | None = None,
    *,
    exist_ok: bool = True,
) -> Callable[
    [InterruptedHandler],
    InterruptedHandler,
]:
    """Register a callback receiving the :class:`Block` for a namespace.

    The event runtime dispatches :class:`ApixEvent` objects internally. This
    decorator hides that transport detail and gives application callbacks the
    ``Block`` promised by the public API.

    Usage:
        @interrupted_hook(namespace="agent")
        async def on_interrupted(block: Block):
            ...
    """
    event_name = get_graph_interrupted_name(namespace, missing_ok=True)

    def decorator(func: InterruptedHandler) -> InterruptedHandler:
        subscribe(event_name, exist_ok=exist_ok)(BlockEventHandler(func))
        return func

    return decorator
