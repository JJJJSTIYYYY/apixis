"""Public API for Apixis's event-driven graph execution module."""

from apixis.core.graph.base import (
    GRAPH_DISPATCH,
    GRAPH_INTERRUPTED,
    GLOBALNS,
    AutoMerge,
    Command,
    KeepRef,
    Reset,
    NodeFunction,
    NodeResult,
    namespace_set,
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
    acquire_namespace,
    release_namespace,
    validate_namespace,
    get_graph_namespace,
    get_graph_dispatch_name,
    get_graph_interrupted_name,
    copy_state,
    parse_state_schema,
    validate_graph_definition
)

__all__ = [
    "GRAPH_DISPATCH",
    "GRAPH_INTERRUPTED",
    "GLOBALNS",
    "AutoMerge",
    "Command",
    "KeepRef",
    "Reset",
    "NodeFunction",
    "NodeResult",
    "namespace_set",
    "GraphContext",
    "GraphContextSnapshot",
    "GraphContextStatus",
    "StreamWriter",
    "get_graph_context",
    "get_stream_writer",
    "get_current_namespace",
    "get_current_run_id",
    "GraphManager",
    "Block",
    "interrupt",
    "interrupted_hook",
    "BaseNode",
    "Node",
    "ParallelNode",
    "NodeGraph",
    "acquire_namespace",
    "release_namespace",
    "validate_namespace",
    "get_graph_namespace",
    "get_graph_dispatch_name",
    "get_graph_interrupted_name",
    "copy_state",
    "parse_state_schema",
    "validate_graph_definition",
]
