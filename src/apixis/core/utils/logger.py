"""Level-filtered ANSI-colored console output through module-level functions."""

from datetime import datetime
import sys
import traceback

from apixis.core.config import core_config


__all__ = ["debug", "info", "success", "warning", "error", "exception"]

_LEVELS = {"DEBUG": 10, "INFO": 20, "SUCCESS": 25, "WARNING": 30, "ERROR": 40, "NONE": 50}


def _write(level: str, color: str, message: object, *args: object) -> bool:
    if _LEVELS[level] < _LEVELS[core_config.LOG_LEVEL]:
        return False
    text = str(message) % args if args else str(message)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stream = sys.stderr if level == "ERROR" else sys.stdout
    print(f"\033[{color}m{timestamp} [APIXIS] [{level}] {text}\033[0m", file=stream)
    return True


def debug(message: object, *args: object) -> None:
    """Print a cyan DEBUG message to stdout; args use percent formatting."""
    _write("DEBUG", "96", message, *args)


def info(message: object, *args: object) -> None:
    """Print a gray INFO message to stdout; args use percent formatting."""
    _write("INFO", "37", message, *args)


def success(message: object, *args: object) -> None:
    """Print a green SUCCESS message to stdout; args use percent formatting."""
    _write("SUCCESS", "92", message, *args)


def warning(message: object, *args: object) -> None:
    """Print a yellow WARNING message to stdout; args use percent formatting."""
    _write("WARNING", "93", message, *args)


def error(message: object, *args: object) -> None:
    """Print a red ERROR message to stderr; args use percent formatting."""
    _write("ERROR", "91", message, *args)


def exception(message: object, *args: object) -> None:
    """Print an error and the current exception's traceback, if any, to stderr."""
    if _write("ERROR", "91", message, *args) and sys.exception() is not None:
        traceback.print_exc()
