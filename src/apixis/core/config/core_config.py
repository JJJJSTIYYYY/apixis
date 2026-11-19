"""Core runtime settings backed by the shared configuration loader."""

from apixis.core.config.base import _get_config
from apixis.core.utils.validation import validate_int


# Bound the local event queue.
EVENT_PIPE_MAX_LEN = validate_int(
    _get_config("EVENT.event_pipe_max_len", 65536),
    "APIXIS.EVENT.event_pipe_max_len", minimum=1,
)
# Bound exact-name routing chains independently of observed event history.
HANDLER_CHAIN_CACHE_SIZE = validate_int(
    _get_config("EVENT.handler_chain_cache_size", 8192),
    "APIXIS.EVENT.handler_chain_cache_size", minimum=1,
)
# Optionally bound foreground dispatches, independently of event queue capacity.
EVENT_LOOP_BACKPRESSURE: int | None = _get_config("EVENT.event_loop_backpressure")
if EVENT_LOOP_BACKPRESSURE is not None:
    EVENT_LOOP_BACKPRESSURE = validate_int(
        EVENT_LOOP_BACKPRESSURE,
        "APIXIS.EVENT.event_loop_backpressure", minimum=1,
    )

SHOW_EVENT_DISPATCH = _get_config("LOG.show_event_dispatch", False)
