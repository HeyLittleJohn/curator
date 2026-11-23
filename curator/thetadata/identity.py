"""Contract identity and the intraday interval rule.

Strikes are exact thousandths of a dollar. Floats are rejected so a binary
fraction cannot be stored.
"""

from datetime import date
from decimal import Decimal

from curator.thetadata.config import (
    INTERVAL_1M_SECONDS,
    INTERVAL_15M_SECONDS,
    ONE_MINUTE_MAX_CALENDAR_DTE,
)

STRIKE_QUANTUM = Decimal("0.001")
RIGHT_CALL = "call"
RIGHT_PUT = "put"
_RIGHTS = {
    "c": RIGHT_CALL,
    "call": RIGHT_CALL,
    "p": RIGHT_PUT,
    "put": RIGHT_PUT,
}


def normalize_symbol(value: str) -> str:
    """Normalize an underlying symbol for the contracts unique key.

    Args:
        value: Vendor symbol, any case.

    Returns:
        The stripped upper-case ticker.

    Raises:
        ValueError: If the symbol is empty or not alphanumeric.
    """
    symbol = value.strip().upper()
    if not symbol or not symbol.isalnum() or len(symbol) > 32:
        raise ValueError("symbol must be 1-32 alphanumeric characters")
    return symbol


def parse_strike(value: object) -> Decimal:
    """Parse a strike in dollars to an exact thousandths Decimal.

    Args:
        value: Strike as text, int, or Decimal. ``4500`` and ``4500.000`` are
            the same strike. Floats and other types are rejected.

    Returns:
        The strike quantized to ``0.001``.

    Raises:
        TypeError: If ``value`` is a float or bool.
        ValueError: If the strike is not finite, not positive, or not an exact
            multiple of one thousandth.
    """
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("strike must be str, int, or Decimal, not float")
    if not isinstance(value, (str, Decimal, int)):
        raise TypeError("strike must be str, int, or Decimal")
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError("strike must be a finite number of dollars")
    quantized = parsed.quantize(STRIKE_QUANTUM)
    if quantized != parsed:
        raise ValueError("strike must be an exact multiple of 0.001")
    if quantized <= 0:
        raise ValueError("strike must be positive")
    return quantized


def parse_right(value: str) -> str:
    """Parse a ThetaData option right.

    Args:
        value: ``call``, ``put``, ``C``, or ``P``, any case.

    Returns:
        ``call`` or ``put``, matching the v3 query vocabulary stored in
        ``theta.contracts.right``.

    Raises:
        ValueError: If ``value`` is not a call or a put.
    """
    key = value.strip().lower()
    try:
        return _RIGHTS[key]
    except KeyError as exc:
        raise ValueError("right must be call or put") from exc


def calendar_dte(trade_date: date, expiration: date) -> int:
    """Return calendar days from a session to expiration.

    Args:
        trade_date: The session being requested.
        expiration: The contract expiration.

    Returns:
        ``expiration - trade_date`` in calendar days. Negative after expiry.
    """
    return (expiration - trade_date).days


def interval_seconds_for_dte(
    calendar_dte_days: int,
    *,
    one_minute_max_dte: int = ONE_MINUTE_MAX_CALENDAR_DTE,
    near_interval_s: int = INTERVAL_1M_SECONDS,
    far_interval_s: int = INTERVAL_15M_SECONDS,
) -> int:
    """Pick the one bar size for a contract on one session.

    Args:
        calendar_dte_days: Calendar days from the session to expiration.
        one_minute_max_dte: Inclusive upper bound for the near interval.
            The default of 5 covers the last 6 calendar days, DTE 0 through 5.
        near_interval_s: Bar length in seconds when calendar DTE is inside the bound.
        far_interval_s: Bar length in seconds when calendar DTE is outside the bound.

    Returns:
        ``near_interval_s`` when calendar DTE is 0 through ``one_minute_max_dte``,
        otherwise ``far_interval_s``. The defaults are 60 and 900. Each
        contract-session has one interval, so ``(contract_id, ts)`` stays unique.

    Raises:
        ValueError: If ``one_minute_max_dte`` is negative or either interval is not positive.
    """
    if one_minute_max_dte < 0:
        raise ValueError("one_minute_max_dte must be >= 0")
    if near_interval_s <= 0 or far_interval_s <= 0:
        raise ValueError("intervals must be positive seconds")
    if 0 <= calendar_dte_days <= one_minute_max_dte:
        return near_interval_s
    return far_interval_s
