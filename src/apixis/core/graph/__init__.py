"""Public API for Apixis's event-driven graph execution module."""

from apixis.core.utils.exception import (
    GraphNodeError,
    InvalidNodeReturnsError,
    InvalidContextError,
)

from apixis.core.graph.base import (
    AutoMerge,
    Command,
    KeepRef,
    Reset,
    NodeFunction,
    NodeResult,
)

from apixis.core.graph.context import (
    GraphContext,
    GraphContextSnapshot,
    GraphContextStatus,
    StreamWriter,
    get_graph_context,
    get_stream_writer,
    get_current_namespace,
    get_current_run_id,
)

from apixis.core.graph.graph_manager import (
    GraphManager,
)

from apixis.core.graph.interrupter import (
    Block,
    interrupt,
    interrupted_hook,
)

from apixis.core.graph.node import (
    BaseNode,
    Node,
    ParallelNode,
)

from apixis.core.graph.node_graph import (
    NodeGraph,
)

from apixis.core.graph.utils import (
    get_graph_dispatch_name,
    get_graph_interrupted_name,
)

__all__ = [
    'GraphNodeError',
    'InvalidNodeReturnsError',
    'InvalidContextError',
    'AutoMerge',
    'Command',
    'KeepRef',
    'Reset',
    'NodeFunction',
    'NodeResult',
    'GraphContext',
    'GraphContextSnapshot',
    'GraphContextStatus',
    'StreamWriter',
    'get_graph_context',
    'get_stream_writer',
    'get_current_namespace',
    'get_current_run_id',
    'GraphManager',
    'Block',
    'interrupt',
    'interrupted_hook',
    'BaseNode',
    'Node',
    'ParallelNode',
    'NodeGraph',
    'get_graph_dispatch_name',
    'get_graph_interrupted_name',
]
