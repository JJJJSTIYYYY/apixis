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
from apixis.core.event import ApixEvent, EventType, post_and_wait, subscribe
from apixis.core.event.base.handler import ApixEventHandler
from apixis.core.graph.interrupter.block import Block
from apixis.core.utils.exception import (
    BlockHookNotRegisteredError, BlockNotResolvedError, GraphNodeError,
)
from apixis.core.utils.validation import validate_number


InterruptedHandler = Callable[[Block], Awaitable[None]]


async def interrupt(
    *,
    data: Any = None,
    timeout: float | None = None,
    context: GraphContext | None = None,
) -> Any:
    """Publish an interruption, await foreground hooks, then await its Block.

    A hook must resolve/fail/cancel the Block or accept it before returning.
    Accepted Blocks may be completed later by external application code.
    No fallback handler is installed: post_and_wait supplies the processed
    observation and this function validates the completed foreground dispatch.

    Args:
        data: Optional chunk data.
        timeout: Optional timeout in seconds covering publication, foreground
            hook processing and the final Block response. None waits indefinitely.
        context: Optional graph context, if the interruption is not within a :class:`NodeFunction`,
            it is required.

    Register hooks through graph.add_interrupted_hook() or interrupted_hook().
    Ordinary event handlers may also consume the Block from event.context at
    get_graph_interrupted_name(namespace).

    Raises:
        BlockHookNotRegisteredError: If foreground processing finishes without
            a business handler executing. Register a hook through
            graph.add_interrupted_hook() or interrupted_hook().
        BlockNotResolvedError: If hooks return without completing or accepting
            the Block for deferred processing.
        GraphNodeError: If an upstream event handler fails before the hook runs.
            A hook's own failure propagates as its original exception.
        asyncio.CancelledError: If this node or its Block is cancelled.
            External Block.cancel() also aborts this graph attempt at its last
            checkpoint. Only this function's own deadline returns None.
    """

    if timeout is not None:
        timeout = validate_number(timeout, "timeout")
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
        timeout_scope = asyncio.timeout(timeout)
        try:
            async with timeout_scope:
                event = await post_and_wait(
                    event_type=EventType.WORKFLOW,
                    event_name=get_graph_interrupted_name(namespace, missing_ok=True),
                    context=block,
                )
                if event is not None and not block.done:
                    if event.has_error:
                        block.fail(GraphNodeError(
                            "Graph interruption failed in an event handler",
                            errors=list(event.error_stack),
                        ))
                    elif event.accepted:
                        block.cancel()
                    elif not event.seen:
                        block.fail(BlockHookNotRegisteredError(
                            f"Graph namespace `{namespace}` emitted a Block without a "
                            "registered interruption hook. Register graph.add_interrupted_hook() "
                            "or interrupted_hook(namespace=...) before calling interrupt()."
                        ))
                    elif not block.accepted:
                        block.fail(BlockNotResolvedError(
                            f"Graph namespace `{namespace}` emitted a Block that was neither "
                            "resolved nor accepted by its interruption handlers."
                        ))
                if not block.done:
                    async with suspend_process():
                        return await block
                return await block
        except TimeoutError:
            # A hook can fail the Block with its own TimeoutError.
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
        if not future.cancelled():
            # Dispatch cancellation may win after a hook has failed the Block.
            future.exception()


def create_interrupted_handler(func: InterruptedHandler) -> ApixEventHandler:
    """Adapt a Block callback and preserve its own exception for the waiting node."""
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
        if isinstance(event.context, Block):
            event.context.fail(error)

    return ApixEventHandler(dispatch_block, on_error=on_failure)


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
        subscribe(event_name, exist_ok=exist_ok)(create_interrupted_handler(func))
        return func

    return decorator
