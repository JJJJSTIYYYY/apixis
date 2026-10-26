"""Graph construction utilities."""

from apixis.core.graph.base import (
    _END,
    NodeFunction,
)
from apixis.core.graph.node import BaseNode, Node, _NodeBinding
from apixis.core.graph.node_graph import NodeGraph
from apixis.core.graph.utils.validate import validate_graph_definition


class GraphManager:
    """Register nodes and state policies for a Command-driven graph.

    The entry point is selected when compiling. Every subsequent step is
    defined exclusively by the commands returned by the executed nodes.
    """

    def __init__(
        self,
        state_schema: type | None = None,
    ):
        """Create an empty graph definition.

        Args:
            state_schema:
                Optional annotated state schema used as the default for each
                invocation's :class:`GraphContext`. Omitting it preserves
                normal dictionary overwrite behaviour.
        """
        self._state_schema = state_schema
        self._nodes: dict[str, BaseNode] = {}

    def has_node(self, node_name: str) -> bool:
        """Returns whether this graph manager has a node named `node_name`."""
        return node_name in self._nodes

    def add_node(
        self,
        node_func: NodeFunction | BaseNode,
        node_name: str | None = None,
        *,
        timeout: float | None = None,
    ):
        """Register a user-defined state-processing node.

        Args:
            node_func: Synchronous or asynchronous node callable.
            node_name: Unique node name, defaulting to the callable's name.
            timeout: Maximum node execution time in seconds. ``None`` and
                values less than or equal to zero wait indefinitely.

        Returns:
            This manager, allowing fluent graph construction.

        Raises:
            ValueError: If the name is reserved, already registered, or the
                timeout is not finite.
            TypeError: If timeout is not a number or ``None``.
        """
        node = node_func if isinstance(node_func, BaseNode) else Node(node_func, node_name)
        name = node.name if node_name is None else node_name
        if name == _END:
            raise ValueError(f"`{name}` is a reserved graph node name.")
        validate_graph_definition({name: node})
        if name in self._nodes:
            raise ValueError(f"Node `{name}` is already registered.")
        self._nodes[name] = _NodeBinding.capture(node, name, timeout)
        return self

    def add_nodes(self, node_list: list[NodeFunction | BaseNode]):
        """Register several nodes using each callable's name.

        Args:
            node_list: Synchronous or asynchronous node callables.

        Returns:
            This manager, allowing fluent graph construction.
        """
        for node_func in node_list:
            self.add_node(node_func)
        return self

    def compile_graph(
        self,
        entry_point: str | list[str] | None,
        *,
        max_steps: int = 1024,
        max_snapshots: int | None = None,
        using_namespace: str | None = None,
        no_snapshot: bool = False,
        exist_ok: bool = False,
    ) -> NodeGraph:
        """Compile this definition into an event-listening :class:`NodeGraph`.

        Args:
            entry_point: First node or concurrent batch. None or an empty
                list creates a graph that completes without executing nodes.
            max_steps: Non-negative number of node-dispatch batches allowed.
            max_snapshots: Positive retained checkpoint limit. None uses
                ``GRAPH.max_snapshots`` from config.yaml (default 16).
            using_namespace: Namespace used by the compiled graph's event
                listeners. ``None`` and an empty string generate a namespace
                unique within the process. Pass ``GLOBALNS`` to explicitly
                select the global namespace. Glob characters are forbidden.
            no_snapshot: Disable automatic pre-node snapshots when ``True``.
                Without a saved checkpoint, abort returns the committed live
                state and the context cannot be restored from its history.
            exist_ok: If ``False``, compiling into an occupied namespace
                raises ``ValueError``. If ``True``, the existing graph is
                retired before the new dispatch listener is registered.
                A later registration failure does not restore the old graph.

        Raises:
            ValueError: If an entry node is unknown, the namespace contains
                glob characters, or it is occupied and ``exist_ok`` is ``False``.
        """
        return NodeGraph(
            self._nodes,
            entry_point,
            max_steps=max_steps,
            max_snapshots=max_snapshots,
            state_schema=self._state_schema,
            using_namespace=using_namespace,
            no_snapshot=no_snapshot,
            exist_ok=exist_ok,
        )
