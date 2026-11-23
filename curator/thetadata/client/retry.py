"""Bounded exponential backoff with full jitter for transient ThetaData errors."""

from __future__ import annotations

import random
from collections.abc import Callable


def backoff_delay(
    attempt: int,
    base: float,
    cap: float,
    uniform: Callable[[float, float], float] = random.uniform,
) -> float:
    """Return a retry delay in seconds.

    The ceiling doubles on each attempt and never exceeds ``cap``. Full jitter
    draws uniformly from zero through that ceiling so retries do not line up.

    Args:
        attempt: Zero-based retry index. The first retry is 0.
        base: Delay ceiling for the first retry, in seconds.
        cap: Maximum ceiling, in seconds.
        uniform: Random source. Tests pass a stub. Production uses
            ``random.uniform``.

    Returns:
        A delay in ``[0, min(cap, base * 2**attempt)]``.
    """
    if attempt < 0:
        raise ValueError("retry attempt must be >= 0")
    if base < 0 or cap < 0:
        raise ValueError("backoff base and cap must be >= 0")
    ceiling = min(cap, base * (2**attempt))
    if ceiling == 0:
        return 0.0
    return uniform(0.0, ceiling)
