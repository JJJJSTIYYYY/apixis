"""Core runtime settings backed by the shared configuration loader."""

from typing import Literal
import os

from apixis.core.config.base import _get_config


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
MAX_LOG_FILE_SIZE = _get_config("LOG.max_log_file_size", 10 * 1024 * 1024)
# Retain at most this many pending log records across all logger names.
LOG_BUFFER_SIZE = _get_config("LOG.buffer_size", 1024)
if (
    isinstance(LOG_BUFFER_SIZE, bool)
    or not isinstance(LOG_BUFFER_SIZE, int)
    or LOG_BUFFER_SIZE <= 0
):
    raise ValueError("LOG.buffer_size must be a positive integer.")


# Pipeline
# Bound the local event queue.
EVENT_PIPE_MAX_LEN = _get_config("PIPELINE.event_pipe_max_len", 65536)
# Bound exact-name routing chains independently of observed event history.
HANDLER_CHAIN_CACHE_SIZE = _get_config("PIPELINE.handler_chain_cache_size", 8192)
if (
    isinstance(HANDLER_CHAIN_CACHE_SIZE, bool)
    or not isinstance(HANDLER_CHAIN_CACHE_SIZE, int)
    or HANDLER_CHAIN_CACHE_SIZE <= 0
):
    raise ValueError("PIPELINE.handler_chain_cache_size must be a positive integer.")
# Bound individual handler executions, independently of event queue capacity.
EVENT_LOOP_BACKPRESSURE = _get_config("PIPELINE.event_loop_backpressure", 1024)
if EVENT_LOOP_BACKPRESSURE < 128:
    EVENT_LOOP_BACKPRESSURE = 128
if EVENT_PIPE_MAX_LEN <= 0:
    raise ValueError("PIPELINE.event_pipe_max_len must be positive.")
