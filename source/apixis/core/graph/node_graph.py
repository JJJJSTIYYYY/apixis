"""Stateless, event-driven graph runtime."""

import asyncio
import copy
from uuid import uuid4
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from functools import wraps
from typing import Any

from apixis.core.event.factory import aget_event_pipe, get_handler_registry
from apixis.core.event.base import suspend_process
from apixis.core.event import (
    ApixEvent,
    ApixEventHandler,
    EventType,
    unsubscribe,
    subscribe,
    get_handler,
)
from apixis.core.utils.exception import (
    BlockHookNotRegisteredError,
    BlockNotResolvedError,
    GraphNodeError,
    InvalidContextError,
)
from apixis.core.utils.id_generator import idgen
from apixis.core.graph.base import (
    _END,
    Command,
    Reset,
)
from apixis.core.graph.utils.namespace import (
    get_graph_interrupted_name,
    release_namespace,
    acquire_namespace,
    get_graph_dispatch_name,
)
from apixis.core.graph.utils.state import copy_state, parse_state_schema
from apixis.core.graph.utils.validate import validate_graph_definition
from apixis.core.graph.context.graph_context import GraphContext, GraphContextSnapshot
from apixis.core.graph.context.manager import apix_graph_context
from apixis.core.graph.interrupter.block import Block
from apixis.core.graph.interrupter.graph_interrupter import (
    BlockEventHandler,
    interrupted_hook,
)
from apixis.core.graph.node import BaseNode
from apixis.core.graph.context.stream_writer import (
    StreamChannel,
    StreamWriter,
    noop_stream_writer,
)


class NodeGraph:
    """Compiled nodes and policies owning their contexts and registrations.

    Namespace registration, listener registration, and invocation management
    are coordinated here. Contexts and snapshots retain only this graph's ID.
    """

    def __init__(
        self,
        nodes: dict[str, BaseNode],
        entry_point: str | list[str] | None,
        *,
        max_steps: int = 1024,
        state_schema: type | None = None,
        using_namespace: str | None = None,
        no_snapshot: bool = False,
        exist_ok: bool = False,
    ):
        """Create a compiled graph and register dispatch and interruption listeners.

        Args:
            nodes: Nodes keyed by their graph names.
            entry_point: First node or concurrent batch; None or [] finishes immediately.
            max_steps: Maximum number of node-dispatch batches in one run.
                Completing without a next target consumes no additional
                step and remains allowed when the last batch uses the budget.
            state_schema: Annotated schema compiled once for this graph.
                Fields marked with ``Annotated[..., AutoMerge()]`` are
                combined through their current value's ``__add__`` method.
            using_namespace: Namespace used by the graph's event listeners.
                ``None`` and an empty string generate a process-local unique
                namespace. Pass ``GLOBALNS`` to explicitly select the global
                namespace.
                Glob characters (``*``, ``?``, ``[``, ``]``) are forbidden.
            no_snapshot: If ``True``, disable snapshotting for the graph.
            exist_ok: If ``True``, force decomposition of the previous owner
                and claim its namespace before registering the new listener.
                A later registration failure releases the new graph's resources
                without restoring the retired graph.
        """
        # Keep finalization safe if initialization fails before acquisition.
        self._decomposed = True
        validate_graph_definition(nodes)
        self._nodes = dict(nodes)
        self._max_steps = max_steps
        self._entry_point = self._normalise_target(entry_point)
        self._no_snapshot = no_snapshot
        # Compile both state policies once; contexts never parse a schema.
        self._auto_merge_keys, self._keep_ref_keys = parse_state_schema(state_schema)
        self._graph_id = uuid4().hex
        self._contexts: set[GraphContext] = set()  # Running attempts only.
        self._namespace = using_namespace or str(idgen.next_id())
        self._handlers: dict[str, ApixEventHandler] = {}

        # Acquisition validates ownership and synchronously retires the old graph.
        # Only an acquired namespace needs cleanup if registration fails.
        acquire_namespace(self, replace_existed=exist_ok)
        try:
            self._register_dispatch_handler()
            self._register_interrupted_handler()
            self._decomposed = False
        except BaseException:
            self._unregister_handlers()
            release_namespace(self, decompose_immediately=False)
            raise

    def __del__(self):
        """Release the graph's namespace and unregister its event listeners."""
        with suppress(Exception):
            self.decompose(force=True)

    @property
    def graph_id(self) -> str:
        """Return this compiled graph's immutable context/checkpoint identity."""
        return self._graph_id

    @property
    def namespace(self) -> str:
        """Namespace used by the graph's event listeners."""
        return self._namespace

    @property
    def dispatch_name(self) -> str:
        """Shared event and handler name for subscriptions and relative ordering."""
        return get_graph_dispatch_name(self.namespace)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.decompose(force=True)
        return False

    def _register_dispatch_handler(self) -> None:
        """Subscribe the graph's single namespace-scoped dispatch handler."""
        async def dispatch_node(event: ApixEvent) -> None:
            """Dispatch an active context to its currently targeted node."""
            context: GraphContext = event.context
            if not self._is_active_context(context):
                return

            target_node_name = context.target_node_name
            if target_node_name == _END:
                self._finish(context)
            else:
                await self._execute_node(target_node_name, context)

        async def on_dispatch_failure(event: ApixEvent, error: Exception) -> None:
            """Complete an invocation after this handler's own dispatch failure."""
            context: GraphContext = event.context
            if self._is_active_context(context):
                self._fail(context, error)

        async def on_dispatch_error(event: ApixEvent) -> None:
            """Fail the invocation when a preceding event handler failed."""
            context: GraphContext = event.context
            if self._is_active_context(context):
                self._fail(
                    context,
                    GraphNodeError(
                        "Graph dispatch failed in a preceding event handler",
                        errors=list(event.error_stack),
                    ),
                )

        async def on_dispatch_accepted(event: ApixEvent) -> None:
            """Abort an invocation whose dispatch was accepted upstream."""
            context: GraphContext = event.context
            if self._is_active_context(context):
                context.abort()

        async def on_dispatch_cancelled(event: ApixEvent) -> None:
            """Propagate dispatch cancellation to the waiting graph invocation."""
            context: GraphContext = event.context
            if self._is_active_context(context):
                context._cancel()

        dispatch_node.__name__ = self.dispatch_name
        handler = ApixEventHandler(
            dispatch_node,
            on_accepted=on_dispatch_accepted,
            on_has_error=on_dispatch_error,
            on_error=on_dispatch_failure,
            on_cancelled=on_dispatch_cancelled,
        )
        subscribe(self.dispatch_name, exist_ok=False)(handler)
        self._handlers[handler.name] = handler

    def _is_active_block(self, block: Block) -> bool:
        """Return whether the block belongs to an active attempt of this graph."""
        return block.graph_id == self.graph_id and any(
            c.run_id == block.run_id and self._is_active_context(c)
            for c in self._contexts
        )

    def _register_interrupted_handler(self) -> None:
        """Always provide interruption cleanup and reject missing user hooks."""
        event_name = get_graph_interrupted_name(self.namespace, missing_ok=True)
        handler_name = f"__graph_interrupted_fallback__{self.namespace}"

        async def temp_hook(_, *args, **kwargs) -> None:
            return

        async def require_interrupted_hook(event: ApixEvent, *args, **kwargs) -> None:
            block: Block = event.context
            if not self._is_active_block(block):
                return
            if not event.seen or event.seen == [handler_name]:
                raise BlockHookNotRegisteredError(
                    f"Graph namespace `{self.namespace}` emitted a Block without a "
                    "registered interruption hook. Register graph.add_interrupted_hook() "
                    "or interrupted_hook(namespace=...) before calling interrupt()."
                )
            if not block.accepted and not block.done:
                raise BlockNotResolvedError(
                    f"Graph namespace `{self.namespace}` emitted a Block and not be resolved by any "
                    "registered interruption hook."
                )

        temp_hook.__name__ = handler_name
        handler = BlockEventHandler(temp_hook)
        handler.set_core_func(require_interrupted_hook)
        # Use the reserved trailing priority, as await_for(point="processed")
        # does, so hooks at every public numeric priority can handle the block.
        handler.subscribe = [event_name]
        handler.priority = -10000
        get_handler_registry().register_handler(handler, exist_ok=False)
        self._handlers[handler.name] = handler

    def _unregister_handlers(self) -> None:
        """Remove only registrations still owned by this graph instance."""
        for name, handler in self._handlers.items():
            if get_handler(name) is handler:
                unsubscribe(name)
        self._handlers.clear()

    def set_max_steps(self, steps: int):
        self._ensure_not_decomposed()
        self._max_steps = steps
        return self

    def _ensure_not_decomposed(self) -> None:
        """Reject business operations on an invalidated graph."""
        if self._decomposed:
            raise RuntimeError("NodeGraph has been decomposed.")

    def decompose(self, *, force: bool = True) -> None:
        """Retire this graph and release its contexts, listeners, and namespace.

        With force=True, running contexts are aborted first. Their
        callers receive the last checkpoint; late node results cannot commit.
        With force=False, running contexts reject decomposition without any
        mutation. Pending contexts remain unchanged and cannot start after
        decomposition. Caller-held contexts remain inspectable.
        """
        if self._decomposed:
            return
        if self._contexts and not force:
            raise RuntimeError(
                "Cannot decompose a NodeGraph while contexts are running."
            )
        self._decomposed = True
        # Terminal transitions remove contexts from the running set immediately.
        for context in tuple(self._contexts):
            context.abort()
        self._unregister_handlers()
        release_namespace(self, decompose_immediately=False)

    def _is_active_context(self, context: object) -> bool:
        """Accept only active contexts managed by this live graph instance."""
        return (
            not self._decomposed
            and isinstance(context, GraphContext)
            and context.graph_id == self.graph_id
            and context in self._contexts
            and context.is_active
        )

    def create_context(self, state: dict) -> GraphContext:
        """Copy initial state and prepare an attempt without retaining it."""
        self._ensure_not_decomposed()
        prepared = copy_state(state, self._keep_ref_keys)
        context = GraphContext(self.graph_id)
        context.state = prepared
        context._set_target_node(self._entry_point)
        return context

    def restore_context(
        self,
        snapshot: GraphContextSnapshot | list[GraphContextSnapshot] | GraphContext,
        *,
        version: int = -1,
    ) -> GraphContext:
        """Restore this graph's checkpoint into an independent pending attempt.

        Context inputs select from that context's stored history. List versions
        use native indexing and retain the selected prefix. A single snapshot
        ignores version. Both history and live state are deep copies, including
        KeepRef fields; their mutable values never alias.
        """
        self._ensure_not_decomposed()
        if isinstance(snapshot, GraphContext):
            if snapshot.graph_id != self.graph_id:
                raise ValueError("GraphContext belongs to a different graph.")
            # Copy only the selected history below, after checking ownership.
            snapshot = snapshot.context_snapshot
        if not snapshot:
            raise RuntimeError("Cannot restore a GraphContext without a snapshot.")
        if isinstance(snapshot, list):
            selected = snapshot[version]
            history = snapshot[:version] + [selected]
        elif isinstance(snapshot, dict):
            history = [snapshot]
        else:
            raise TypeError("GraphContext snapshot must be a dict, a list of dicts or a GraphContext itself.")
        for checkpoint in history:
            self._validate_snapshot(checkpoint)
        restored_history = copy.deepcopy(history)
        restored = restored_history[-1]
        context = GraphContext(self.graph_id)
        context.state = copy.deepcopy(restored["state"])
        context.target_node_name = copy.deepcopy(restored["target_node_name"])
        context.steps = restored["steps"]
        context.context_snapshot = restored_history
        return context

    def _validate_snapshot(self, snapshot: GraphContextSnapshot) -> None:
        """Validate stored data and ownership before creating a context."""
        if not isinstance(snapshot, dict):
            raise TypeError("GraphContext snapshot must be a dict.")
        required = {"timestamp", "state", "target_node_name", "steps", "graph_id"}
        missing = required.difference(snapshot)
        if missing:
            raise ValueError(
                "GraphContext snapshot is missing required fields: "
                + ", ".join(sorted(missing))
                + "."
            )
        if not isinstance(snapshot["graph_id"], str):
            raise TypeError("GraphContext snapshot graph_id must be a string.")
        if snapshot["graph_id"] != self.graph_id:
            raise ValueError("GraphContext snapshot belongs to a different graph.")
        if not isinstance(snapshot["state"], dict):
            raise TypeError("GraphContext snapshot state must be a dict.")
        target = snapshot["target_node_name"]
        if not (
            isinstance(target, str)
            or (isinstance(target, list) and all(isinstance(n, str) for n in target))
        ):
            raise TypeError(
                "GraphContext snapshot target_node_name must be a string or list of strings."
            )
        if isinstance(snapshot["steps"], bool) or not isinstance(
            snapshot["steps"], int
        ):
            raise TypeError("GraphContext snapshot steps must be an int.")
        if snapshot["steps"] < 0:
            raise ValueError("GraphContext snapshot steps cannot be negative.")
        if isinstance(snapshot["timestamp"], bool) or not isinstance(
            snapshot["timestamp"], (int, float)
        ):
            raise TypeError("GraphContext snapshot timestamp must be a number.")

    def _begin_invocation(
        self,
        state: dict | None,
        context: GraphContext | None,
        writer: StreamWriter,
    ) -> GraphContext:
        """Resolve one state source and accept it without suspending.

        Rejected contexts remain untouched. Only successfully bound running
        attempts enter the graph's management set.
        """
        self._ensure_not_decomposed()
        if context is None:
            if not isinstance(state, dict):
                raise TypeError("Graph state must be a dict.")
            context = self.create_context(state)
        else:
            if state is not None:
                raise TypeError("Pass either state or graph_context, not both.")
            if not isinstance(context, GraphContext):
                raise TypeError("graph_context must be a GraphContext or None.")
        if context.graph_id != self.graph_id:
            raise InvalidContextError("GraphContext belongs to a different graph.")
        if context.status != "pending":
            raise InvalidContextError(
                "GraphContext must be pending before starting an invocation."
            )
        if not isinstance(context.state, dict):
            raise TypeError("Graph state must be a dict.")
        self._validate_target(context.target_node_name, context.steps)
        context._bind(
            run_id="graph-" + uuid4().hex,
            completion=asyncio.get_running_loop().create_future(),
            stream_writer=writer,
            on_terminal=self._contexts.discard,
        )
        self._contexts.add(context)
        return context

    async def invoke(
        self, state: dict | None = None, graph_context: GraphContext | None = None
    ) -> dict:
        """Execute initial state or one prepared context, using its existing state."""
        context = self._begin_invocation(state, graph_context, noop_stream_writer())
        return await self._invoke(context)

    async def stream(
        self, state: dict | None = None, graph_context: GraphContext | None = None
    ) -> AsyncIterator[Any]:
        """Yield chunks from one managed attempt; closing the stream aborts it."""
        channel = StreamChannel()
        context = self._begin_invocation(state, graph_context, channel.writer)
        execution_task = asyncio.create_task(
            self._invoke(context), name=f"graph-stream-{context.run_id}"
        )
        execution_task.add_done_callback(lambda task: channel.close())
        try:
            async for chunk in channel:
                yield chunk
            await execution_task
        finally:
            if not execution_task.done():
                execution_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await execution_task
            if context.status == "running":
                context.abort()
            channel.close()

    async def abort(self, graph_context: GraphContext) -> None:
        """Interrupt the invocation represented by ``graph_context``.

        The graph's terminal dispatch is skipped, so the invocation's
        completion future is resolved with the most recently saved state
        snapshot. Any queued chunks are yielded before a stream ends.

        When this method is called, the graph execution is not interrupted
        immediately. A snapshot is captured immediately before each ordinary
        node starts. The current node may continue running, but its result
        cannot be committed or scheduled after the abort. The :meth:`invoke` and
        :meth:`stream` interfaces return immediately with the captured state.

        The same operation is also available directly through
        :meth:`GraphContext.abort`.

        Raises:
            TypeError: If ``graph_context`` is not a GraphContext instance.
            ValueError: If the context belongs to a different graph.
        """
        self._ensure_not_decomposed()
        if not isinstance(graph_context, GraphContext):
            raise TypeError("NodeGraph.abort requires a GraphContext instance.")

        if not (graph_context.is_active or not graph_context.is_consumed):
            return  # Already finished, aborted, or cancelled; nothing to do.

        if graph_context.graph_id != self.graph_id:
            raise ValueError("GraphContext belongs to a different graph.")
        graph_context.abort()

    async def _invoke(self, context: GraphContext) -> dict:
        """Execute an admitted attempt and copy its result at the graph boundary."""
        completion = context.completion
        if completion is None:
            raise RuntimeError("Completion in GraphContext could not be None.")
        try:
            if self._is_active_context(context):
                await self._post_next(context.target_node_name, context)
            # A parent handler must lend its permit while child dispatches run.
            async with suspend_process():
                result = await completion
            # Checkpoint history must stay isolated even when output contains
            # KeepRef fields. Normal completion follows the graph's copy policy.
            if context.status == "aborted" and context.context_snapshot:
                return copy.deepcopy(result)
            return copy_state(result, self._keep_ref_keys)
        except asyncio.CancelledError:
            if context.status == "running":
                context._cancel()
            raise
        except Exception as exc:
            self._fail(context, exc)
            if completion.done() and not completion.cancelled():
                completion.exception()
            raise

    async def _execute_one_node(
        self,
        node_name: str,
        context: GraphContext,
    ) -> Command | list[Command]:
        """Execute one batch member with an isolated state copy."""
        node = self._nodes[node_name]
        execution = node.execute(copy_state(context.state, self._keep_ref_keys))
        timeout = node.timeout
        if timeout is None:
            return await execution

        timeout_scope = asyncio.timeout(timeout)
        try:
            async with timeout_scope:
                return await execution
        except TimeoutError as exc:
            if not timeout_scope.expired():
                raise
            raise TimeoutError(
                f"Graph node `{node_name}` timed out after {timeout:g} seconds."
            ) from exc

    async def _execute_node(
        self,
        node_name: str | list[str],
        context: GraphContext,
    ) -> None:
        """Execute one node or one concurrently scheduled node batch."""
        normalized_node_names = self._normalise_targets(node_name)
        if not self._no_snapshot:
            context.take_a_snapshot()
        try:
            with apix_graph_context(context):
                tasks = [
                    asyncio.create_task(
                        self._execute_one_node(current_name, context),
                        name=f"graph-node-{current_name}-{context.run_id}",
                    )
                    for current_name in normalized_node_names
                ]
                results = await BaseNode._gather_tasks_in_order(tasks)

            # An aborted attempt may finish its old node after a caller has
            # already received the saved snapshot. Its result must never
            # mutate a recovered context or enqueue another event.
            if not context.is_active:
                return

            next_node = self.apply_command(
                results,
                normalized_node_names,
                context,
            )

            context.steps += 1
            await self._post_next(next_node, context)
        except Exception as exc:
            if context.is_active:
                self._fail(context, exc)

    def apply_command(
        self,
        command: Command | list[Command] | list[Command | list[Command]],
        node_name: str | list[str],
        context: GraphContext,
    ) -> str | list[str]:
        """Apply a completed batch in order and collect its next targets.

        A string node name takes one Command or list[Command]. A list of node
        names takes one such result per node, even for a singleton batch.

        The checkpoint taken before node execution is the rollback boundary.
        Commands therefore update the live context state directly. If applying
        a later command fails, recovery starts from that checkpoint rather than
        rolling the live state back in place.
        """
        self._ensure_not_decomposed()
        if (
            not isinstance(context, GraphContext)
            or context.graph_id != self.graph_id
            or context.status not in ("pending", "running")
        ):
            raise InvalidContextError(
                "GraphContext must belong to this graph and be pending or running."
            )
        normalized_node_names = self._normalise_targets(node_name)
        # Adapt single-node calls once; internally every batch has one result
        # per source node, independently of the number of nodes or commands.
        command_groups = self._normalise_command_groups(
            [command] if isinstance(node_name, str) else command,
            normalized_node_names,
        )

        updated_normal_keys: set[str] = set()
        next_targets: dict[str, None] = {}
        concurrent = False
        for current_node, commands in zip(normalized_node_names, command_groups):
            current_node_normal_keys: set[str] = set()
            for current_command in commands:
                if not isinstance(current_command.update, dict):
                    raise TypeError("Command.update must be a dict.")
                update = copy_state(
                    current_command.update,
                    self._keep_ref_keys,
                )
                for key, value in update.items():
                    if key not in self._auto_merge_keys:
                        if (
                            key in updated_normal_keys
                            and key not in current_node_normal_keys
                        ):
                            raise ValueError(
                                f"Concurrent node `{current_node}` updates "
                                f"non-AutoMerge state field `{key}` more than once."
                            )
                        updated_normal_keys.add(key)
                        current_node_normal_keys.add(key)

                    if isinstance(value, Reset):
                        context.state[key] = value.value
                    elif key in self._auto_merge_keys and key in context.state:
                        current_value = context.state[key]
                        add_method = getattr(current_value, "__add__", None)
                        if not callable(add_method):
                            raise TypeError(
                                f"State field `{key}` is marked AutoMerge, "
                                f"but {type(current_value).__name__} does not "
                                "provide a callable __add__ method."
                            )
                        merged_value = add_method(value)
                        if merged_value is NotImplemented:
                            raise TypeError(
                                f"State field `{key}` could not add an update "
                                f"of type {type(value).__name__}."
                            )
                        context.state[key] = merged_value
                    else:
                        context.state[key] = value

                goto = current_command.goto
                if not BaseNode._is_valid_goto(goto):
                    raise TypeError(
                        "Command.goto must be a string or None, or a list of strings."
                    )
                if isinstance(goto, list):
                    for name in goto:
                        if name != _END:
                            concurrent = True
                            next_targets[name] = None
                elif goto is not None and goto != _END:
                    next_targets[goto] = None

        if not next_targets:
            return _END
        if concurrent or len(next_targets) > 1:
            return list(next_targets)
        return next(iter(next_targets))

    @staticmethod
    def _normalise_command_groups(
        results: object,
        normalized_node_names: list[str],
    ) -> list[list[Command]]:
        """Validate one result per source node and normalise each command group."""
        if not isinstance(results, list) or len(results) != len(normalized_node_names):
            raise TypeError("A concurrent batch must return one result per node.")
        groups: list[list[Command]] = []
        for result in results:
            if isinstance(result, Command):
                groups.append([result])
            elif isinstance(result, list) and all(
                isinstance(item, Command) for item in result
            ):
                groups.append(result)
            else:
                raise TypeError("Node.execute must return a Command or list[Command].")
        return groups

    @staticmethod
    def _normalise_targets(target: str | list[str]) -> list[str]:
        """Validate a non-empty execution target and return a batch list."""
        if isinstance(target, str):
            return [target]
        if (
            isinstance(target, list)
            and target
            and all(isinstance(item, str) for item in target)
        ):
            return list(target)
        raise TypeError("Graph target must be a string or non-empty list of strings.")

    def _normalise_target(
        self,
        target: str | list[str] | None,
    ) -> str | list[str]:
        """Resolve termination and duplicates, preserving a batch's shape."""
        if not BaseNode._is_valid_goto(target):
            raise TypeError("Graph target must be a string, None, or list of strings.")
        if target is None:
            return _END
        if isinstance(target, list):
            target = list(dict.fromkeys(name for name in target if name != _END))
            if not target:
                return _END
        if target == _END:
            return target
        for name in [target] if isinstance(target, str) else target:
            if name not in self._nodes:
                raise ValueError(f"Unknown graph node `{name}`.")
        return target

    def _validate_target(
        self, target: str | list[str] | None, steps: int
    ) -> str | list[str]:
        """Check a runtime target against its invocation's execution budget."""
        if isinstance(steps, bool) or not isinstance(steps, int):
            raise TypeError("Graph steps must be an int.")
        if steps < 0:
            raise ValueError("Graph steps cannot be negative.")
        target = self._normalise_target(target)
        # Completion does not execute nodes or consume a step.
        if target != _END and steps >= self._max_steps:
            raise RecursionError(
                f"Graph exceeded its maximum of {self._max_steps} steps."
            )
        return target

    async def _post_next(
        self,
        node_name: str | list[str],
        context: GraphContext,
    ) -> None:
        """Target one node or concurrent batch and post one dispatch."""
        target = self._validate_target(
            node_name, context.steps
        )
        context._set_target_node(target)
        await (await aget_event_pipe()).post_event(
            event_type=EventType.WORKFLOW,
            event_name=self.dispatch_name,
            context=context,
        )

    def _finish(self, context: GraphContext) -> None:
        """Resolve an invocation with the state carried by its terminal event."""
        if self._is_active_context(context):
            context._finish()

    def _fail(self, context: GraphContext, error: Exception) -> None:
        """Resolve an invocation with the exception raised by a graph node."""
        if self._is_active_context(context):
            context._fail(error)

    def add_interrupted_hook(
        self,
        afunc: Callable[[Block], Awaitable[None]],
        name: str | None = None,
    ) -> Callable[[Block], Awaitable[None]]:
        """Register a graph-owned interruption callback.

        The graph's namespace is selected automatically. The callback is
        unregistered by :meth:`decompose`, preventing a replacement graph from
        accidentally dispatching blocks to a stale callback.

        Args:
            afunc: Asynchronous callback accepting one :class:`Block`.
            name: Optional handler name for registration and diagnostics.
                Defaults to the name generated by :func:`get_graph_interrupted_name`.
        """
        self._ensure_not_decomposed()

        @wraps(afunc)
        async def dispatch_owned_block(block: Block) -> None:
            """Namespace reuse cannot transfer another graph's interruption."""
            if self._is_active_block(block):
                await afunc(block)

        dispatch_owned_block.__name__ = name or get_graph_interrupted_name(
            self.namespace, missing_ok=True
        )
        interrupted_hook(self.namespace, exist_ok=False)(dispatch_owned_block)
        handler = get_handler(dispatch_owned_block.__name__)
        if handler is None:
            raise RuntimeError("Interrupted hook registration failed; handler not found.")
        self._handlers[handler.name] = handler
        return afunc
