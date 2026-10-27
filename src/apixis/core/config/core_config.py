"""Core runtime settings backed by the shared configuration loader."""

from apixis.core.config.base import _get_config
from apixis.core.utils.validation import validate_int


# Console event dispatch output
SHOW_EVENT_DISPATCH = _get_config("LOG.show_event_dispatch", True)


# Pipeline
# Bound the local event queue.
EVENT_PIPE_MAX_LEN = validate_int(
    _get_config("PIPELINE.event_pipe_max_len", 65536),
    "PIPELINE.event_pipe_max_len", minimum=1,
)
# Bound exact-name routing chains independently of observed event history.
HANDLER_CHAIN_CACHE_SIZE = validate_int(
    _get_config("PIPELINE.handler_chain_cache_size", 8192),
    "PIPELINE.handler_chain_cache_size", minimum=1,
)
# Bound individual handler executions, independently of event queue capacity.
EVENT_LOOP_BACKPRESSURE = max(128, validate_int(
    _get_config("PIPELINE.event_loop_backpressure", 1024),
    "PIPELINE.event_loop_backpressure", minimum=1,
))

# Keep only the most recent checkpoints of each invocation.
GRAPH_MAX_SNAPSHOTS = validate_int(
    _get_config("GRAPH.max_snapshots", 16), "GRAPH.max_snapshots", minimum=1,
)
