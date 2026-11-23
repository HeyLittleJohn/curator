"""NYSE sessions for the SPXW planner.

``pandas_market_calendars`` ships the holiday and early-close rules, so tests
do not download a calendar. Half days stay in the session list. Full holidays
and weekends do not.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from typing import Any

import pandas_market_calendars as mcal

_FULL_CLOSE_HOUR = 16


@dataclass(frozen=True)
class TradingSession:
    """One NYSE session.

    Args:
        trade_date: Session date in America/New_York.
        half_day: True when the session closes before 16:00 Eastern.
    """

    trade_date: date
    half_day: bool


@lru_cache(maxsize=1)
def _nyse_calendar() -> Any:
    """Return the cached NYSE calendar.

    Returns:
        The pandas-market-calendars NYSE calendar.
    """
    return mcal.get_calendar("NYSE")


def _as_date(value: object) -> date:
    """Return a date from a schedule index label.

    Args:
        value: A ``date``, ``datetime``, or pandas timestamp.

    Returns:
        The calendar date.

    Raises:
        TypeError: The label is not a date.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise TypeError("NYSE schedule index is not a date")


def trading_sessions(start: date, end: date) -> tuple[TradingSession, ...]:
    """List NYSE sessions in an inclusive range.

    Args:
        start: First calendar date to consider.
        end: Last calendar date to consider.

    Returns:
        Sessions in order. Holidays and weekends are absent. Early closes are
        marked ``half_day``.

    Raises:
        ValueError: ``end`` is before ``start``.
    """
    if end < start:
        raise ValueError("end is before start")
    schedule = _nyse_calendar().schedule(start_date=start.isoformat(), end_date=end.isoformat())
    sessions: list[TradingSession] = []
    for label, row in schedule.iterrows():
        close_local = row["market_close"].tz_convert("America/New_York")
        sessions.append(TradingSession(trade_date=_as_date(label), half_day=int(close_local.hour) < _FULL_CLOSE_HOUR))
    return tuple(sessions)
