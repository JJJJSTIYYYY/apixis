"""Core runtime settings backed by the shared configuration loader."""

from typing import Literal
import os

from apixis.core.config.base import _get_config
from apixis.core.utils.validation import validate_int


# Shared data root and Apixis runtime paths
BASE_DIR = _get_config("SERVER.base_dir", "./.apix/")
APIXIS_BASE_DIR = os.path.join(BASE_DIR, "apixis")


# Log
DEBUG_LEVEL: Literal["DEBUG", "INFO", "WARN", "ERROR"] = _get_config(
    "LOG.debug_level",
    "DEBUG",
).upper()

TRACE = _get_config("LOG.trace", True)
SHOW_EVENT_DISPATCH = _get_config("LOG.show_event_dispatch", True)
MAX_LOG_FILE_SIZE = validate_int(
    _get_config("LOG.max_log_file_size", 10 * 1024 * 1024),
    "LOG.max_log_file_size", minimum=1,
)
# Retain at most this many pending log records across all logger names.
LOG_BUFFER_SIZE = validate_int(
    _get_config("LOG.buffer_size", 1024), "LOG.buffer_size", minimum=1,
)


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
