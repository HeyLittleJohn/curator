"""Weekday incremental SPXW load after the 17:15 ET readiness gate.

The run plans with the TD-4 queue and loads with the TD-5 backfill. It reuses
that backfill's aiomultiprocess pool, the shared 4-request cap, and the machine
lock, so it cannot overlap a manual backfill. Options Standard only: no index,
Cboe, or FRED data.

A trading day before 17:15 America/New_York exits without waiting. ``--wait``
sleeps until 17:15 and then runs. Weekends and NYSE holidays exit without
loading; the next trading day's run catches sessions still inside the lookback.
The lookback is a count of NYSE sessions ending today, clamped to 2022-05-16.
Sessions in that window that are already done or empty are not requested.
Sessions that were never planned, or that still have pending, running, or
failed rows, are planned and loaded. A second run of a finished window makes
no terminal requests and writes nothing.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from curator.thetadata.backfill import run_backfill
from curator.thetadata.calendar import trading_sessions
from curator.thetadata.config import BACKFILL_START
from curator.thetadata.jobs import PlanJob
from curator.thetadata.limits import backfill_lock
from curator.thetadata.mapping import PendingFact
from curator.thetadata.planner import parse_datasets
from curator.thetadata.progress import MemoryProgressStore, ProgressQueue, open_progress_store
from curator.thetadata.report import LoadReport, format_failure_reasons
from curator.thetadata.time import EASTERN
from curator.thetadata.window import DEFAULT_STRIKE_RANGE

EOD_READY = time(17, 15)
DEFAULT_LOOKBACK_SESSIONS = 10
READY = "ready"
LOADED = "loaded"
SKIP_EARLY = "before_ready"
SKIP_CLOSED = "not_trading_day"
SKIP_EARLY_MESSAGE = (
    "Nightly skipped: EOD data is not ready until 17:15 America/New_York. "
    "The default is to exit. Re-run with --wait to sleep until then."
)
SKIP_CLOSED_MESSAGE = "Nightly skipped: today is not an NYSE session."

Clock = Callable[[], datetime]
Sleeper = Callable[[float], Awaitable[None]]
# Running rows are orphaned: this process holds the machine lock, so no worker
# is still downloading them. The grace keeps the comparison strictly after now.
_ORPHAN_GRACE = timedelta(minutes=1)


class NightlyError(ValueError):
    """The nightly request cannot be run."""


@dataclass(frozen=True)
class Readiness:
    """Whether one instant may start the nightly load.

    Args:
        action: ``ready``, ``before_ready``, or ``not_trading_day``. A half day
            uses this same gate.
        session: Eastern calendar date of the instant.
        ready_at: 17:15 America/New_York on that date.
    """

    action: str
    session: date
    ready_at: datetime


@dataclass(frozen=True)
class NightlyReport:
    """Counts for one nightly run. No row values and no connection string.

    Args:
        action: ``loaded``, ``before_ready``, or ``not_trading_day``.
        sessions_due: Sessions in the lookback that were not already finished.
            Zero when the run is skipped or the window is already complete.
        requests: Logical terminal requests. A finished rerun is zero.
        new_rows: Fact rows written by this run. A finished rerun is zero.
        jobs_done: Jobs marked done.
        jobs_empty: Jobs marked empty.
        jobs_failed: Jobs marked failed. The CLI exits non-zero when this is set.
        errors: Scrubbed failure texts. The formatter does not print them.
    """

    action: str
    sessions_due: int
    requests: int
    new_rows: int
    jobs_done: int
    jobs_empty: int
    jobs_failed: int
    errors: tuple[str, ...] = ()


def market_ready_at(session: date) -> datetime:
    """Return 17:15 America/New_York on a calendar date.

    Args:
        session: Eastern calendar date. The date does not have to be a session.

    Returns:
        The readiness instant. DST is applied by ``America/New_York``.
    """
    return datetime(session.year, session.month, session.day, EOD_READY.hour, EOD_READY.minute, tzinfo=EASTERN)


def is_trading_day(session: date) -> bool:
    """Return whether an Eastern date is an NYSE session.

    Args:
        session: Calendar date in America/New_York.

    Returns:
        True for full and half days. False for weekends and holidays.
    """
    return bool(trading_sessions(session, session))


def evaluate_readiness(now: datetime) -> Readiness:
    """Decide whether ``now`` may start a nightly load.

    Args:
        now: Timezone-aware instant. Naive values are rejected.

    Returns:
        The gate result. Exactly 17:15 is ready. Half days use that same
        instant, not the early cash close. A weekend or holiday is closed
        even when the clock is after 17:15, and even when it is earlier.

    Raises:
        NightlyError: ``now`` has no timezone.
    """
    if now.tzinfo is None:
        raise NightlyError("clock must be timezone-aware")
    eastern = now.astimezone(EASTERN)
    session = eastern.date()
    ready_at = market_ready_at(session)
    if not is_trading_day(session):
        action = SKIP_CLOSED
    elif eastern < ready_at:
        action = SKIP_EARLY
    else:
        action = READY
    return Readiness(action=action, session=session, ready_at=ready_at)


def catchup_sessions(today: date, lookback: int) -> tuple[date, ...]:
    """Return the NYSE sessions a nightly run may plan.

    Args:
        today: Eastern calendar date. Included when it is a session on or
            after 2022-05-16.
        lookback: How many sessions to keep, including ``today``.

    Returns:
        Sessions in order. Empty when ``today`` is before the backfill floor.
        Shorter than ``lookback`` when the floor cuts the window off.

    Raises:
        NightlyError: ``lookback`` is below 1.
    """
    if lookback < 1:
        raise NightlyError("lookback must be >= 1")
    if today < BACKFILL_START:
        return ()
    span_days = lookback * 3 + 14
    while True:
        start = today - timedelta(days=span_days)
        if start < BACKFILL_START:
            start = BACKFILL_START
        sessions = tuple(item.trade_date for item in trading_sessions(start, today))
        if len(sessions) >= lookback or start == BACKFILL_START:
            if len(sessions) > lookback:
                return sessions[-lookback:]
            return sessions
        span_days *= 2


def due_sessions(store: ProgressQueue, sessions: Sequence[date], datasets: Sequence[str]) -> tuple[date, ...]:
    """Return sessions that still need a plan or a load.

    Args:
        store: Progress queue. Not opened here.
        sessions: Candidate NYSE sessions, in order.
        datasets: Datasets that must each be finished on a session.

    Returns:
        Sessions that have a pending, running, or failed row, or that have no
        row for at least one requested dataset.
    """
    if not sessions:
        return ()
    covered = store.covered_dates(start=sessions[0], end=sessions[-1], datasets=datasets)
    due: list[date] = []
    for day in sessions:
        census = store.census(start=day, end=day, datasets=datasets)
        unfinished = census.pending or census.running or census.failed
        missing = any(day not in covered[name] for name in datasets)
        if unfinished or missing:
            due.append(day)
    return tuple(due)


def format_nightly_report(report: NightlyReport) -> str:
    """Format a nightly run as counts only.

    Args:
        report: Run summary.

    Returns:
        One ``name=count`` line per figure, ending in a newline. No dates,
        strikes, premiums, or connection strings.
    """
    lines = [
        f"skipped={int(report.action != LOADED)}",
        f"sessions_due={report.sessions_due}",
        f"requests={report.requests}",
        f"new_rows={report.new_rows}",
        f"jobs_done={report.jobs_done}",
        f"jobs_empty={report.jobs_empty}",
        f"jobs_failed={report.jobs_failed}",
        f"failure_reasons={format_failure_reasons(report.errors)}",
    ]
    return "\n".join(lines) + "\n"


def _now() -> datetime:
    """Return the current instant.

    Returns:
        A timezone-aware UTC datetime.
    """
    return datetime.now(timezone.utc)


async def _sleep(seconds: float) -> None:
    """Sleep for a number of seconds.

    Args:
        seconds: Delay. Zero or negative returns immediately.

    Returns:
        None.
    """
    import asyncio

    if seconds > 0:
        await asyncio.sleep(seconds)


def _quiet(action: str) -> NightlyReport:
    """Return a run that made no requests.

    Args:
        action: ``loaded``, ``before_ready``, or ``not_trading_day``.

    Returns:
        A zeroed report.
    """
    return NightlyReport(
        action=action,
        sessions_due=0,
        requests=0,
        new_rows=0,
        jobs_done=0,
        jobs_empty=0,
        jobs_failed=0,
    )


def _from_load(loaded: LoadReport, sessions_due: int) -> NightlyReport:
    """Copy load counts onto a nightly report.

    Args:
        loaded: Backfill summary for the due span.
        sessions_due: Sessions that were not already finished.

    Returns:
        Counts for this nightly run.
    """
    return NightlyReport(
        action=LOADED,
        sessions_due=sessions_due,
        requests=loaded.requests,
        new_rows=loaded.new_rows,
        jobs_done=loaded.jobs_done,
        jobs_empty=loaded.jobs_empty,
        jobs_failed=loaded.jobs_failed,
        errors=loaded.errors,
    )


@contextmanager
def _borrow(database_url: str | None, queue: ProgressQueue | None) -> Generator[ProgressQueue, None, None]:
    """Yield the caller's queue, or open postgres for one transaction.

    Args:
        database_url: Postgres URL. Ignored when ``queue`` is set. Not logged.
        queue: Queue reused for the orphan reset and the load.

    Yields:
        A progress queue. A postgres queue commits when the block exits.
    """
    if queue is not None:
        yield queue
        return
    with open_progress_store(database_url) as store:
        yield store


def _release_orphans(store: ProgressQueue, sessions: Sequence[date], datasets: Sequence[str]) -> None:
    """Return every running row in the window to pending.

    The caller holds the machine lock, so a running row has no live worker.

    Args:
        store: Progress queue.
        sessions: Lookback sessions.
        datasets: Datasets to reset.

    Returns:
        None.
    """
    if not sessions:
        return
    store.reset_stale(
        datetime.now(timezone.utc) + _ORPHAN_GRACE,
        start=sessions[0],
        end=sessions[-1],
        datasets=datasets,
    )


async def _wait_until_ready(clock: Clock, sleeper: Sleeper, *, wait: bool) -> Readiness:
    """Apply the trading-day and 17:15 gates, sleeping at most once.

    Args:
        clock: Timezone-aware clock.
        sleeper: Used only when ``wait`` is true and the session is early.
        wait: Sleep until 17:15 America/New_York when the session is open.

    Returns:
        The gate after an optional sleep. A no-op sleeper that leaves the
        clock early comes back as ``before_ready`` instead of looping.
    """
    decision = evaluate_readiness(clock())
    if decision.action != SKIP_EARLY or not wait:
        return decision
    remaining = (decision.ready_at - clock().astimezone(EASTERN)).total_seconds()
    await sleeper(remaining)
    return evaluate_readiness(clock())


async def run_nightly(
    *,
    api_base_url: str,
    database_url: str | None,
    datasets: Sequence[str] | None = None,
    max_dte: int | None = None,
    strike_range: int | None = DEFAULT_STRIKE_RANGE,
    worker_count: int | None = None,
    lock_path: Path | None = None,
    batch_size: int = 1000,
    queue: ProgressQueue | None = None,
    use_pool: bool = True,
    writer: Callable[[PlanJob, Sequence[PendingFact]], int] | None = None,
    lookback: int = DEFAULT_LOOKBACK_SESSIONS,
    wait: bool = False,
    now: Clock | None = None,
    sleep: Sleeper | None = None,
) -> NightlyReport:
    """Plan and load today's SPXW session and unfinished sessions in the lookback.

    Args:
        api_base_url: Theta Terminal v3 base URL.
        database_url: Postgres URL. Required when ``use_pool`` is true. Not logged.
        datasets: Datasets to load. ``None`` loads every SPXW dataset.
        max_dte: Optional calendar DTE cap. ``None`` keeps every listed expiration.
        strike_range: Strikes on each side of ATM. ``None`` is the full chain.
        worker_count: Worker processes. ``None`` uses the machine default.
        lock_path: Machine lock file. ``None`` uses ``/tmp/curator-theta.lock``.
        batch_size: Insert batch size.
        queue: Progress queue. ``None`` opens postgres, or a fresh memory queue
            when there is no database URL.
        use_pool: When true, parsing and upserts run in the aiomultiprocess pool.
        writer: In-process sink used instead of postgres. Tests pass this with
            ``use_pool`` false.
        lookback: NYSE sessions to consider, including today. The 2022-05-16
            floor still applies. The default is 10.
        wait: Sleep until 17:15 America/New_York on a trading day. The default
            returns ``before_ready`` and makes no requests.
        now: Clock. ``None`` uses the current time.
        sleep: Sleeper used with ``wait``. ``None`` uses ``asyncio.sleep``.

    Returns:
        Counts. A skipped day and a finished window both report zero requests
        and zero new rows. ``jobs_failed`` is set when a claimed job fails.

    Raises:
        NightlyError: The lookback or the clock is unusable.
        LoadError: The load request cannot be run.
        PlanRangeError: The datasets or DTE cap are not usable.
        PlanFloorError: The window ends before 2022-05-16.
    """
    selected = parse_datasets(datasets)
    if lookback < 1:
        raise NightlyError("lookback must be >= 1")
    clock = now or _now
    sleeper = sleep or _sleep
    decision = await _wait_until_ready(clock, sleeper, wait=wait)
    if decision.action != READY:
        return _quiet(decision.action)
    sessions = catchup_sessions(decision.session, lookback)
    if not sessions:
        return _quiet(LOADED)
    if queue is None and database_url is None:
        queue = MemoryProgressStore()
    lock = lock_path or Path("/tmp/curator-theta.lock")
    async with backfill_lock(lock):
        with _borrow(database_url, queue) as store:
            _release_orphans(store, sessions, selected)
            due = due_sessions(store, sessions, selected)
        if not due:
            return _quiet(LOADED)
        loaded = await run_backfill(
            start=due[0],
            end=due[-1],
            api_base_url=api_base_url,
            database_url=database_url,
            datasets=selected,
            max_dte=max_dte,
            strike_range=strike_range,
            worker_count=worker_count,
            lock_path=lock,
            batch_size=batch_size,
            queue=queue,
            use_pool=use_pool,
            writer=writer,
        )
    return _from_load(loaded, len(due))
