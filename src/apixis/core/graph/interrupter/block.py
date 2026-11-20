from asyncio import CancelledError, Future
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Block:
    run_id: str
    block_id: str
    namespace: str
    with_data: Any

    _accepted: bool = field(default=False, init=False)
    _future: Future[Any] = field(repr=False)
    graph_id: str | None = None

    def __post_init__(self) -> None:
        """Reject manually constructed blocks without an awaitable future."""
        if not isinstance(self._future, Future):
            raise TypeError("Block._future must be an asyncio.Future.")

    def __await__(self):
        return self._future.__await__()

    @property
    def done(self) -> bool:
        """Return whether this interruption has already been completed."""
        return self._future.done()

    @property
    def accepted(self) -> bool:
        """Return whether this interruption has already been accepted."""
        return self._accepted

    def accept(self) -> None:
        """Claim deferred handling without resolving or resuming this interruption."""
        self._accepted = True

    @property
    def cancelled(self) -> bool:
        """Return whether this interruption was cancelled externally."""
        return self._future.cancelled()

    def resolve(self, result: Any) -> None:
        """Resolve the block with `result`, unblocking the interrupted execution."""
        if self._future.done():
            return

        self._future.set_result(result)

    def fail(self, error: Exception | CancelledError) -> None:
        """Raise an interruption failure or runtime cancellation in the waiting node."""
        if not self._future.done():
            self._future.set_exception(error)

    def cancel(self) -> None:
        """Cancel the block and abort its current graph invocation."""
        self._future.cancel()
