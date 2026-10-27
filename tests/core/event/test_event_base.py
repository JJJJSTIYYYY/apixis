"""Tests for public event values and handler defaults."""

from apixis.core.event.base import ApixEventHandler


async def _handler(event):
    return None


def test_handler_entry_defaults_to_infinite_wait():
    """A directly constructed handler also defaults to no timeout."""
    entry = ApixEventHandler(_handler)

    assert entry.time_out is None
