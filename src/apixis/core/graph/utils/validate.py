from collections.abc import Mapping

from apixis.core.graph.base import _END
from apixis.core.graph.node import BaseNode


def validate_graph_definition(nodes: Mapping[str, BaseNode]) -> None:
    """Validate graph-local node names and instances without mutating them.

    Mapping keys define graph-local names independently of BaseNode.name.
    The internal terminal target cannot be used as an executable node name.
    """
    if not isinstance(nodes, Mapping):
        raise TypeError("nodes must be a mapping.")
    for name, node in nodes.items():
        if not isinstance(name, str):
            raise TypeError("Node names must be strings.")
        if not name:
            raise ValueError("Node names must not be empty.")
        if name == _END:
            raise ValueError(f"Node name {name!r} is reserved.")
        if not isinstance(node, BaseNode):
            raise TypeError(f"Node {name!r} must be a BaseNode instance.")
