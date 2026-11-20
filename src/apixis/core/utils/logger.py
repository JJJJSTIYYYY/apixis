"""Forward runtime logs to application-provided callables."""

from collections.abc import Callable
from typing import ClassVar


__all__ = ["Logger"]


class Logger:
    """Configure process-wide logging callbacks independently for each level.

    All callbacks default to None, so logging is silent until configured.
    Each set_{level}(logger) replaces that level's callback; pass None to
    disable it again. The shared runtime uses these callbacks.

    Log methods forward the message, positional arguments and keyword arguments
    unchanged. The application logger owns formatting, filtering, output and
    exception details. Callback return values are ignored and callback errors
    propagate to the caller. Use synchronous callables; callbacks are not awaited.

    Example:
        import logging

        application_logger = logging.getLogger("application")
        Logger.set_info(application_logger.info)
        Logger.set_error(application_logger.error)
        Logger.info("Job %s completed", "example")
        Logger.set_info(None)
    """
    _debug: ClassVar[Callable[..., object] | None] = None
    _info: ClassVar[Callable[..., object] | None] = None
    _success: ClassVar[Callable[..., object] | None] = None
    _warning: ClassVar[Callable[..., object] | None] = None
    _error: ClassVar[Callable[..., object] | None] = None
    _exception: ClassVar[Callable[..., object] | None] = None

    @classmethod
    def set_debug(cls, logger: Callable[..., object] | None) -> None:
        """Replace the debug callback; None disables this level."""
        cls._debug = logger

    @classmethod
    def debug(cls, message: object, *args: object, **kwargs: object) -> None:
        """Forward arguments unchanged to the debug callback, if configured."""
        logger = cls._debug
        if logger is not None:
            logger(message, *args, **kwargs)

    @classmethod
    def set_info(cls, logger: Callable[..., object] | None) -> None:
        """Replace the info callback; None disables this level."""
        cls._info = logger

    @classmethod
    def info(cls, message: object, *args: object, **kwargs: object) -> None:
        """Forward arguments unchanged to the info callback, if configured."""
        logger = cls._info
        if logger is not None:
            logger(message, *args, **kwargs)

    @classmethod
    def set_success(cls, logger: Callable[..., object] | None) -> None:
        """Replace the success callback; None disables this level."""
        cls._success = logger

    @classmethod
    def success(cls, message: object, *args: object, **kwargs: object) -> None:
        """Forward arguments unchanged to the success callback, if configured."""
        logger = cls._success
        if logger is not None:
            logger(message, *args, **kwargs)

    @classmethod
    def set_warning(cls, logger: Callable[..., object] | None) -> None:
        """Replace the warning callback; None disables this level."""
        cls._warning = logger

    @classmethod
    def warning(cls, message: object, *args: object, **kwargs: object) -> None:
        """Forward arguments unchanged to the warning callback, if configured."""
        logger = cls._warning
        if logger is not None:
            logger(message, *args, **kwargs)

    @classmethod
    def set_error(cls, logger: Callable[..., object] | None) -> None:
        """Replace the error callback; None disables this level."""
        cls._error = logger

    @classmethod
    def error(cls, message: object, *args: object, **kwargs: object) -> None:
        """Forward arguments unchanged to the error callback, if configured."""
        logger = cls._error
        if logger is not None:
            logger(message, *args, **kwargs)

    @classmethod
    def set_exception(cls, logger: Callable[..., object] | None) -> None:
        """Replace the exception callback; None disables this level."""
        cls._exception = logger

    @classmethod
    def exception(cls, message: object, *args: object, **kwargs: object) -> None:
        """Forward arguments unchanged to the exception callback, if configured."""
        logger = cls._exception
        if logger is not None:
            logger(message, *args, **kwargs)
