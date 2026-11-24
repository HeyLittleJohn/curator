"""Stream one plan job into fact batches.

Intraday jobs with a finite strike range list the chain, then stream history
at the job's interval. The open bar is held only until its timestamp changes,
which bounds memory to one bar of the chain plus the insert batch. Daily open
interest and EOD ignore the strike window and stream the full chain. An
unlimited strike range does the same.

A 472 from the terminal is an empty job, not a failure. The request meter
counts each list or history call once. Client retries are not extra counts.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from curator.thetadata.client.http import ThetaDataClient
from curator.thetadata.client.rows import EodRow, GreeksRow, OhlcRow, OpenInterestRow, QuoteRow
from curator.thetadata.exceptions import ThetaDataNoDataError
from curator.thetadata.identity import normalize_symbol, parse_strike
from curator.thetadata.jobs import DATASETS_DAILY, DATASETS_INTRADAY, PlanJob
from curator.thetadata.mapping import LoadError, PendingFact, map_fact, theta_interval
from curator.thetadata.window import PricedRow, open_premium, select_strike_window

RowT = QuoteRow | OhlcRow | GreeksRow | OpenInterestRow | EodRow
FactIdentity = tuple[str, date, Decimal, str, date]


@dataclass
class RequestMeter:
    """Logical terminal requests started for one piece of work.

    Args:
        count: List and history calls. A retry inside the client is the same call.
    """

    count: int = 0


async def stream_fact_batches(
    client: ThetaDataClient,
    job: PlanJob,
    strike_range: int | None,
    batch_size: int,
    meter: RequestMeter,
) -> AsyncIterator[list[PendingFact]]:
    """Stream a job as insert batches after the strike window.

    Args:
        client: Open Options Standard client. Calls take the shared request slot.
        job: One dataset, session, and expiration.
        strike_range: Strikes on each side of ATM. ``None`` keeps the full chain.
            Ignored for open interest and EOD.
        batch_size: Maximum facts yielded together.
        meter: Incremented once per list or history request.

    Yields:
        Fact batches. Nothing is yielded when the terminal has no rows or the
        window drops every row.

    Raises:
        LoadError: ``batch_size`` is below 1, or a row cannot be mapped.
        ThetaDataClientError: The terminal fails for a reason other than no data.
    """
    if batch_size < 1:
        raise LoadError("batch_size must be >= 1")
    if job.dataset in DATASETS_INTRADAY and strike_range is not None:
        listed = await _listed_strikes(client, job, meter)
        if listed:
            async for batch in _windowed(client, job, listed, strike_range, batch_size, meter):
                yield batch
            return
    async for batch in _unfiltered(client, job, batch_size, meter):
        yield batch


async def _listed_strikes(client: ThetaDataClient, job: PlanJob, meter: RequestMeter) -> list[Decimal]:
    """Read the listed chain for one expiration.

    Args:
        client: Open client.
        job: Job whose expiration is listed.
        meter: Request counter.

    Returns:
        Unique strikes. Empty when the terminal has no chain for that expiration.
    """
    meter.count += 1
    found: list[Decimal] = []
    try:
        async for row in client.list_strikes(job.symbol, job.expiration):
            found.append(parse_strike(row.strike))
    except ThetaDataNoDataError:
        return []
    return found


def _history(client: ThetaDataClient, job: PlanJob) -> AsyncIterator[RowT]:
    """Open the history stream for one job.

    Args:
        client: Open client.
        job: Dataset, expiration, and bar length.

    Returns:
        The client's async iterator. The caller counts the request.

    Raises:
        LoadError: An intraday job has no bar length.
    """
    if job.dataset == "quotes":
        return client.iter_quotes(
            job.symbol,
            job.expiration,
            session_date=job.trade_date,
            right="both",
            interval=theta_interval(_require_interval(job)),
        )
    if job.dataset == "ohlc":
        return client.iter_ohlc(
            job.symbol,
            job.expiration,
            session_date=job.trade_date,
            right="both",
            interval=theta_interval(_require_interval(job)),
        )
    if job.dataset == "greeks_first_order":
        return client.iter_greeks(
            job.symbol,
            job.expiration,
            session_date=job.trade_date,
            right="both",
            interval=theta_interval(_require_interval(job)),
        )
    if job.dataset == "open_interest":
        return client.iter_open_interest(
            job.symbol,
            job.expiration,
            session_date=job.trade_date,
            right="both",
        )
    if job.dataset == "eod":
        return client.iter_eod(
            job.symbol,
            job.expiration,
            start_date=job.trade_date,
            end_date=job.trade_date,
            right="both",
        )
    raise LoadError("unknown dataset")


def _require_interval(job: PlanJob) -> int:
    """Return the job bar length.

    Args:
        job: Intraday job.

    Returns:
        A positive bar length in seconds.

    Raises:
        LoadError: The job has no bar length.
    """
    if job.interval_s is None:
        raise LoadError("intraday job is missing its bar length")
    return job.interval_s


async def _rows(client: ThetaDataClient, job: PlanJob, meter: RequestMeter) -> AsyncIterator[RowT]:
    """Yield history rows, treating no-data as an empty stream.

    Args:
        client: Open client.
        job: Job to request.
        meter: Request counter.

    Yields:
        Parsed rows. A 472 yields nothing.
    """
    meter.count += 1
    try:
        async for row in _history(client, job):
            yield row
    except ThetaDataNoDataError:
        return


def _in_scope(job: PlanJob, row: RowT) -> bool:
    """Return whether a row belongs to this job's contract family.

    Args:
        job: Job being loaded.
        row: One history row.

    Returns:
        True when the symbol and expiration match the job.
    """
    try:
        symbol = normalize_symbol(row.symbol)
    except ValueError:
        return False
    return symbol == job.symbol and row.expiration == job.expiration


def _as_priced(row: RowT) -> PricedRow:
    """Return an intraday row.

    Args:
        row: History row.

    Returns:
        The same row when it is a quote, bar, or greek.

    Raises:
        LoadError: A daily row arrived on an intraday job.
    """
    if isinstance(row, (QuoteRow, OhlcRow, GreeksRow)):
        return row
    raise LoadError("intraday job returned a daily row")


def _strike_of(row: RowT) -> Decimal:
    """Return the row strike at scale 3.

    Args:
        row: History row.

    Returns:
        The quantized strike.

    Raises:
        LoadError: The strike is not an exact positive thousandth.
    """
    try:
        return parse_strike(row.strike)
    except (TypeError, ValueError) as exc:
        raise LoadError("row strike is not usable") from exc


def _identity(job: PlanJob, fact: PendingFact) -> FactIdentity:
    """Return the fact identity used to drop duplicates inside one job.

    Args:
        job: Job being loaded.
        fact: Mapped fact.

    Returns:
        Contract key plus the conflict timestamp or session date.

    Raises:
        LoadError: The fact has no session timestamp or date.
    """
    marker = fact.values["date"] if job.dataset in DATASETS_DAILY else fact.values["ts"]
    if not isinstance(marker, date):
        raise LoadError("fact is missing its session time")
    return (fact.contract.symbol, fact.contract.expiration, fact.contract.strike, fact.contract.right, marker)


def _remember(
    batch: list[PendingFact],
    seen: set[FactIdentity],
    job: PlanJob,
    row: RowT,
) -> None:
    """Append a row unless this job already emitted that conflict key.

    Args:
        batch: Batch being filled.
        seen: Identities already emitted.
        job: Job being loaded.
        row: In-scope row.

    Returns:
        None.
    """
    fact = map_fact(job, row)
    identity = _identity(job, fact)
    if identity in seen:
        return
    seen.add(identity)
    batch.append(fact)


async def _unfiltered(
    client: ThetaDataClient,
    job: PlanJob,
    batch_size: int,
    meter: RequestMeter,
) -> AsyncIterator[list[PendingFact]]:
    """Stream every in-scope row.

    Args:
        client: Open client.
        job: Daily job, or an intraday job with an unlimited strike range.
        batch_size: Maximum facts per batch.
        meter: Request counter.

    Yields:
        Fact batches.
    """
    batch: list[PendingFact] = []
    seen: set[FactIdentity] = set()
    async for row in _rows(client, job, meter):
        if not _in_scope(job, row):
            continue
        _remember(batch, seen, job, row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _solve(listed: list[Decimal], held: list[PricedRow], strike_range: int) -> frozenset[Decimal]:
    """Solve the strike window from the open bar.

    Args:
        listed: Chain from ``option/list/strikes``.
        held: Rows from the first timestamp.
        strike_range: Strikes on each side of ATM.

    Returns:
        Strikes to store.
    """
    anchor: list[tuple[Decimal, str, Decimal]] = []
    for row in held:
        premium = open_premium(row)
        if premium is None:
            continue
        anchor.append((_strike_of(row), row.right.value.lower(), premium))
    return select_strike_window(listed, anchor, strike_range).strikes


def _take_open(
    batch: list[PendingFact],
    seen: set[FactIdentity],
    job: PlanJob,
    held: list[PricedRow],
    window: frozenset[Decimal],
) -> None:
    """Keep open-bar rows whose strike is inside the window.

    Args:
        batch: Batch being filled.
        seen: Identities already emitted.
        job: Intraday job.
        held: Open-bar rows.
        window: Strikes to store.

    Returns:
        None.
    """
    for row in held:
        if _strike_of(row) in window:
            _remember(batch, seen, job, row)


async def _windowed(
    client: ThetaDataClient,
    job: PlanJob,
    listed: list[Decimal],
    strike_range: int,
    batch_size: int,
    meter: RequestMeter,
) -> AsyncIterator[list[PendingFact]]:
    """Stream an intraday job and drop strikes outside the window.

    Args:
        client: Open client.
        job: Quote, OHLC, or greeks job.
        listed: Non-empty chain.
        strike_range: Strikes on each side of ATM.
        batch_size: Maximum facts per batch.
        meter: Request counter.

    Yields:
        Fact batches containing only window strikes.
    """
    window: frozenset[Decimal] | None = None
    held: list[PricedRow] = []
    first: datetime | None = None
    batch: list[PendingFact] = []
    seen: set[FactIdentity] = set()
    async for row in _rows(client, job, meter):
        if not _in_scope(job, row):
            continue
        priced = _as_priced(row)
        if window is None and (first is None or priced.timestamp == first):
            first = priced.timestamp
            held.append(priced)
            continue
        if window is None:
            window = _solve(listed, held, strike_range)
            _take_open(batch, seen, job, held, window)
            held = []
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if window is not None and _strike_of(priced) in window:
            _remember(batch, seen, job, priced)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if held:
        window = _solve(listed, held, strike_range)
        _take_open(batch, seen, job, held, window)
    if batch:
        yield batch
