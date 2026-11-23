"""Picklable SPXW plan jobs.

A job is one dataset, one NYSE session, and one listed expiration. Intraday
datasets carry a single bar length. Daily open interest and EOD leave the
interval empty because those requests cover the full strike chain.

Workers claim these records from ``theta.progress``. The dataclass is a
top-level type so ``aiomultiprocess`` can pickle it into a worker process.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

SYMBOL_SPXW = "SPXW"

DATASET_QUOTES = "quotes"
DATASET_OHLC = "ohlc"
DATASET_GREEKS_FIRST_ORDER = "greeks_first_order"
DATASET_OPEN_INTEREST = "open_interest"
DATASET_EOD = "eod"

DATASETS_INTRADAY: tuple[str, ...] = (
    DATASET_QUOTES,
    DATASET_OHLC,
    DATASET_GREEKS_FIRST_ORDER,
)
DATASETS_DAILY: tuple[str, ...] = (
    DATASET_OPEN_INTEREST,
    DATASET_EOD,
)
DATASETS: tuple[str, ...] = DATASETS_INTRADAY + DATASETS_DAILY
DATASET_NAMES = frozenset(DATASETS)


@dataclass(frozen=True)
class PlanJob:
    """One download unit for the ThetaData worker pool.

    Args:
        dataset: ``quotes``, ``ohlc``, ``greeks_first_order``, ``open_interest``, or ``eod``.
        trade_date: NYSE session, including half days.
        expiration: Listed expiration with calendar DTE of at least 0.
        interval_s: Bar length in seconds, or ``None`` for a daily dataset.
        symbol: Underlying. This planner builds ``SPXW`` jobs.
        calendar_dte: Calendar days from ``trade_date`` to ``expiration``.
    """

    dataset: str
    trade_date: date
    expiration: date
    interval_s: int | None
    symbol: str = SYMBOL_SPXW
    calendar_dte: int = 0

    def key(self) -> tuple[str, date, date]:
        """Return the progress-table identity.

        Returns:
            ``(dataset, trade_date, expiration)``.
        """
        return (self.dataset, self.trade_date, self.expiration)
