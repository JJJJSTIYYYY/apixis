"""Stateless ANSI-colored console output through module-level functions."""

from datetime import datetime
import sys
import traceback


__all__ = ["debug", "info", "success", "warning", "error", "exception"]


def _write(level: str, color: str, message: object, *args: object) -> None:
    text = str(message) % args if args else str(message)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stream = sys.stderr if level == "ERROR" else sys.stdout
    print(f"\033[{color}m{timestamp} [APIX] [{level}] {text}\033[0m", file=stream)


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
    error(message, *args)
    if sys.exception() is not None:
        traceback.print_exc()
