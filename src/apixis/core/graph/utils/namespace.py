"""Namespace ownership and dispatch names for compiled graphs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from apixis.core.graph.base import GRAPH_DISPATCH, GLOBALNS, GRAPH_INTERRUPTED, _namespace_graphs, namespace_set

if TYPE_CHECKING:
    from apixis.core.graph.node_graph import NodeGraph


def validate_namespace(namespace: str) -> None:
    """Validate a graph registration name without changing ownership."""
    if any(character in namespace for character in "*?[]"):
        raise ValueError(
            f"Graph namespace `{namespace}` must not contain glob characters (*?[])."
        )


def get_graph_namespace(graph_id: str) -> str:
    """Resolve a live graph ID from the existing namespace registry."""
    for namespace, graph in _namespace_graphs.items():
        if graph.graph_id == graph_id:
            return namespace
    raise RuntimeError("The context's graph is no longer registered.")


def acquire_namespace(
    graph: NodeGraph,
    *,
    replace_existed: bool = False,
) -> NodeGraph:
    """Claim a namespace, forcefully retiring its previous graph if requested.

    Replacement completes synchronously. Listener registration remains the
    caller's responsibility; later failures do not restore the retired graph.
    """
    validate_namespace(graph.namespace)
    owner = _namespace_graphs.get(graph.namespace)
    if owner is not None and owner is not graph:
        if not replace_existed:
            raise ValueError(f"Graph namespace `{graph.namespace}` is already in use.")
        release_namespace(owner)
    _namespace_graphs[graph.namespace] = graph
    return graph


def release_namespace(
    graph: NodeGraph,
    *,
    decompose_immediately: bool = True,
) -> None:
    """Optionally retire the graph, then release only its own registration.

    Repeated cleanup and cleanup of a replaced graph are harmless. Decomposition
    calls back with decompose_immediately=False to avoid recursive teardown.
    """
    if decompose_immediately:
        graph.decompose(force=True)
    namespace = graph.namespace
    if _namespace_graphs.get(namespace) is graph:
        _namespace_graphs.pop(namespace)


def get_graph_dispatch_name(
    namespace_or_graph: str | NodeGraph | None = None,
    missing_ok: bool = True,
) -> str:
    """Return the event and handler name for graph dispatch.

    Use the returned name with ``subscribe`` and as the graph handler boundary
    in ``between_handlers``. No node name is needed.

    Args:
        namespace_or_graph: Graph namespace or graph instance. ``None`` and an empty string select
            ``<global>``. ``*`` produces a subscription pattern matching
            every graph, including the global graph. A wildcard pattern
            cannot identify a single handler for ``between_handlers``.
        missing_ok: If ``False``, require a concrete namespace to be owned by
            a compiled graph. The wildcard namespace ``*`` is always accepted.
    """
    if namespace_or_graph and not isinstance(namespace_or_graph, str):
        namespace = namespace_or_graph.namespace or GLOBALNS
    else:
        namespace = namespace_or_graph or GLOBALNS

    if not missing_ok and namespace != "*" and namespace not in namespace_set:
        raise KeyError(f"Namespace `{namespace}` not found in current namespace set.")

    return f"{GRAPH_DISPATCH}_{namespace}"


def get_graph_interrupted_name(
    namespace_or_graph: str | NodeGraph | None = None,
    missing_ok: bool = True,
) -> str:
    """Return the interruption event name and default user hook name.

    Use the returned name with ``subscribe`` and to look up or unregister the
    default handler registered by ``NodeGraph.add_interrupted_hook``. It can
    also be used as that handler's boundary in ``between_handlers``. A custom
    hook name must be used directly for handler lookup and boundaries.

    Args:
        namespace_or_graph: Graph namespace or graph instance. ``None`` and an empty string select
            ``<global>``. ``*`` produces a subscription pattern matching
            every graph, including the global graph. A wildcard pattern
            cannot identify a single handler for ``between_handlers``.
        missing_ok: If ``False``, require a concrete namespace to be owned by
            a compiled graph. The wildcard namespace ``*`` is always accepted.
    """
    if namespace_or_graph and not isinstance(namespace_or_graph, str):
        namespace = namespace_or_graph.namespace or GLOBALNS
    else:
        namespace = namespace_or_graph or GLOBALNS

    if not missing_ok and namespace != "*" and namespace not in namespace_set:
        raise KeyError(f"Namespace `{namespace}` not found in current namespace set.")

    return f"{GRAPH_INTERRUPTED}_{namespace}"
