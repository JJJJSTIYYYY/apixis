"""Shared validation for runtime capacities, budgets and numeric options."""

import math


def validate_int(value: object, name: str, *, minimum: int | None = None) -> int:
    """Require a real integer (excluding bool) and an optional lower bound."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int.")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}.")
    return value


def validate_number(
    value: object,
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    """Require a finite int/float (excluding bool) within optional bounds."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number.")
    try:
        number = float(value)
    except OverflowError:
        raise ValueError(f"{name} must be finite.") from None
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite.")
    if minimum is not None and number < minimum:
        raise ValueError(f"{name} must be at least {minimum}.")
    if maximum is not None and number > maximum:
        raise ValueError(f"{name} must be at most {maximum}.")
    return number


def validate_timeout(value: object, name: str = "timeout") -> float | None:
    """Normalize execution timeouts; None or non-positive means unlimited."""
    if value is None:
        return None
    number = validate_number(value, name)
    return number if number > 0 else None
