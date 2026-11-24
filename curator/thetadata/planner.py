"""SPXW job planner.

Jobs are ``(dataset, trade_date, expiration)`` for every NYSE session in range
and the listed SPXW expirations ``select_spxw_expirations`` keeps. Dates are
never invented: a holiday-shifted monthly is kept only when that Thursday is
already in the listed set.

Intraday datasets use a 1-minute bar when calendar DTE is 0 through 5, and a
15-minute bar otherwise. The cutoff and both lengths are arguments. Open
interest and EOD are daily: each job still names one expiration, the interval
is empty, and the downloader covers the full strike chain.

Selection follows the VolSignals 3D windows. Calendar DTE 0 through 40 keeps
every listed expiration. Weekly Fridays that are not the third Friday are kept
through 50. Monthlies (third Friday, or the Thursday before it when that
Friday is an NYSE holiday), quarterlies (those monthlies in March, June,
September, and December), and end-of-month (the last listed weekday expiration
of the month) are kept through 183, plus the next one of those dates through
220. ``max_dte`` is an optional extra cap applied after that selection.

The live expiration list has returned 2305 dates from 2012-06-01 through
2089-06-30. That last date sits decades after any listed SPXW series and is
treated as a ThetaData placeholder. Expirations more than
``FAR_EXPIRATION_HORIZON_DAYS`` (10 years of 366 days) after the plan end are
counted and dropped before the VS3D windows run. The horizon is not
``max_dte``. Pass ``horizon_days=None`` to keep every future date in the
windows, including that placeholder when it also passes selection.

A multi-session plan syncs one session at a time so the progress queue never
materializes the whole range in memory.

The backfill floor is 2022-05-16. An earlier start is clamped up to the floor
when the end is on or after it. A range that ends before the floor is refused.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Protocol

from curator.thetadata.calendar import trading_sessions
from curator.thetadata.config import (
    BACKFILL_START,
    INTERVAL_1M_SECONDS,
    INTERVAL_15M_SECONDS,
    NEAR_EXPIRATION_DTE,
    ONE_MINUTE_MAX_CALENDAR_DTE,
    ROLL_IN_EXPIRATION_DTE,
    STANDARD_EXPIRATION_DTE,
    WEEKLY_EXPIRATION_DTE,
)
from curator.thetadata.identity import calendar_dte, interval_seconds_for_dte
from curator.thetadata.jobs import DATASET_NAMES, DATASETS, DATASETS_INTRADAY, SYMBOL_SPXW, PlanJob
from curator.thetadata.progress import (
    STALE_RUNNING_AFTER,
    MemoryProgressStore,
    ProgressQueue,
    SyncStats,
)

# 2089-06-30 is the observed tail of option/list/expirations?symbol=SPXW.
# Ten 366-day years after the plan end drops that placeholder and keeps LEAPs.
FAR_EXPIRATION_HORIZON_DAYS = 366 * 10


class PlanError(ValueError):
    """The plan request cannot be built."""


class PlanRangeError(PlanError):
    """The dates, datasets, or DTE cap are not usable."""


class PlanFloorError(PlanError):
    """The range ends before the 2022-05-16 backfill floor."""


@dataclass(frozen=True)
class PlanCounts:
    """Counts for one plan. Dates and rows are not included.

    Args:
        sessions: NYSE sessions in the clamped range.
        half_days: Sessions that close before 16:00 Eastern.
        listed: Unique expiration dates in the input.
        placeholders: Expirations past the far-future horizon.
        expirations: Listed expirations that produced at least one job.
        clamped: True when the start moved up to the backfill floor.
        jobs: Jobs in the plan, including ones already finished.
        pending: Jobs left queued as pending.
        skipped_done: Finished rows left untouched.
        skipped_empty: Empty rows left untouched.
        skipped_running: Running rows that were not stale.
        requeued_failed: Failed rows returned to pending.
        reset_running: Stale running rows returned to pending.
        writes: Progress inserts and updates. A finished rerun is zero.
        by_dataset: Job counts keyed by dataset name.
        by_interval: Job counts keyed by ``daily`` or the bar length in seconds.
    """

    sessions: int
    half_days: int
    listed: int
    placeholders: int
    expirations: int
    clamped: bool
    jobs: int
    pending: int
    skipped_done: int
    skipped_empty: int
    skipped_running: int
    requeued_failed: int
    reset_running: int
    writes: int
    by_dataset: Mapping[str, int]
    by_interval: Mapping[str, int]


@dataclass(frozen=True)
class PlanResult:
    """A plan and the pending jobs a pool can pickle.

    ``jobs`` is the pending queue for this plan. Multiple worker processes
    should ``claim`` from the progress store instead of sharing this list,
    so two processes cannot take the same row.

    Args:
        counts: Count-only summary.
        jobs: Pending jobs when ``materialize`` is true, otherwise empty.
        start: Session start after the floor clamp.
        end: Requested end date.
    """

    counts: PlanCounts
    jobs: tuple[PlanJob, ...]
    start: date
    end: date


def resolve_range(start: date, end: date) -> tuple[date, date, bool]:
    """Apply the 2022-05-16 floor.

    Args:
        start: Requested first date.
        end: Requested last date.

    Returns:
        The effective start, the end, and whether the start was clamped.

    Raises:
        PlanRangeError: ``end`` is before ``start``.
        PlanFloorError: ``end`` is before the backfill floor.
    """
    if end < start:
        raise PlanRangeError("end is before start")
    if end < BACKFILL_START:
        raise PlanFloorError("end is before the 2022-05-16 backfill floor")
    if start < BACKFILL_START:
        return BACKFILL_START, end, True
    return start, end, False


def parse_datasets(values: Sequence[str] | None) -> tuple[str, ...]:
    """Resolve the dataset list.

    Args:
        values: Repeated ``--dataset`` values. Commas split one value.
            ``None`` or an empty sequence means every SPXW dataset.

    Returns:
        Dataset names in plan order.

    Raises:
        PlanRangeError: A name is not a known dataset.
    """
    if not values:
        return DATASETS
    names: list[str] = []
    for value in values:
        for part in value.split(","):
            name = part.strip()
            if not name:
                continue
            if name not in DATASET_NAMES:
                raise PlanRangeError(f"unknown dataset: {name}")
            if name not in names:
                names.append(name)
    if not names:
        raise PlanRangeError("unknown dataset")
    return tuple(names)


def partition_expirations(
    expirations: Iterable[date],
    *,
    plan_end: date,
    horizon_days: int | None,
) -> tuple[tuple[date, ...], tuple[date, ...]]:
    """Split listed expirations into kept dates and far-future placeholders.

    Args:
        expirations: Dates from ``option/list/expirations``. Duplicates collapse.
        plan_end: Last session in the plan.
        horizon_days: Drop expirations more than this many days after
            ``plan_end``. ``None`` keeps every date.

    Returns:
        Kept dates and dropped dates, each sorted.

    Raises:
        PlanRangeError: ``horizon_days`` is negative.
    """
    if horizon_days is not None and horizon_days < 0:
        raise PlanRangeError("horizon_days must be >= 0")
    unique = tuple(sorted(set(expirations)))
    if horizon_days is None:
        return unique, ()
    limit = plan_end + timedelta(days=horizon_days)
    kept = tuple(day for day in unique if day <= limit)
    dropped = tuple(day for day in unique if day > limit)
    return kept, dropped


def select_spxw_expirations(
    session: date,
    listed: Sequence[date],
    *,
    near_dte: int = NEAR_EXPIRATION_DTE,
    weekly_dte: int = WEEKLY_EXPIRATION_DTE,
    standard_dte: int = STANDARD_EXPIRATION_DTE,
    roll_in_dte: int = ROLL_IN_EXPIRATION_DTE,
) -> tuple[date, ...]:
    """Choose the listed SPXW expirations to load for one session.

    Only dates in ``listed`` are returned. Nothing is invented: a monthly that
    moves to Thursday because the third Friday is an NYSE holiday is kept only
    when that Thursday is already listed. Calendar DTE below 0 is dropped.

    Args:
        session: NYSE session the DTE is measured from.
        listed: Expiration dates from ``option/list/expirations``. Duplicates
            collapse. Order does not matter.
        near_dte: Inclusive calendar DTE through which every listed expiration
            is kept. Default 40.
        weekly_dte: Inclusive calendar DTE through which Friday expirations
            that are not the third Friday are kept. Default 50.
        standard_dte: Inclusive calendar DTE through which monthlies,
            quarterlies, and end-of-month expirations are kept. Default 183.
        roll_in_dte: Inclusive calendar DTE through which the next monthly,
            quarterly, or end-of-month expiration past ``standard_dte`` is
            kept. Default 220. One date, the soonest.

    Returns:
        Selected expiration dates, ascending.

    Raises:
        PlanRangeError: A threshold is negative.
    """
    _validate_expiration_window(near_dte, weekly_dte, standard_dte, roll_in_dte)
    unique = tuple(sorted(set(listed)))
    if not unique:
        return ()
    standards = _standard_expirations(set(unique))
    chosen: set[date] = set()
    roll_in: date | None = None
    for expiration in unique:
        dte = calendar_dte(session, expiration)
        if dte < 0:
            continue
        if _keep_listed(expiration, dte, standards, near_dte, weekly_dte, standard_dte):
            chosen.add(expiration)
            continue
        if expiration in standards and dte <= roll_in_dte and roll_in is None:
            roll_in = expiration
    if roll_in is not None:
        chosen.add(roll_in)
    return tuple(sorted(chosen))


def iter_jobs(
    sessions: Sequence[date],
    expirations: Sequence[date],
    *,
    datasets: Sequence[str],
    max_dte: int | None,
    one_minute_max_dte: int,
    near_interval_s: int,
    far_interval_s: int,
    near_dte: int = NEAR_EXPIRATION_DTE,
    weekly_dte: int = WEEKLY_EXPIRATION_DTE,
    standard_dte: int = STANDARD_EXPIRATION_DTE,
    roll_in_dte: int = ROLL_IN_EXPIRATION_DTE,
) -> Iterator[PlanJob]:
    """Yield one job per dataset, session, and selected expiration.

    Args:
        sessions: NYSE session dates, in order.
        expirations: Listed expiration dates. Order does not matter. The
            placeholder horizon is applied by the caller before this.
        datasets: Datasets to emit.
        max_dte: Optional inclusive calendar DTE cap applied after selection.
            ``None`` does not add a cap.
        one_minute_max_dte: Inclusive DTE bound for the near interval.
        near_interval_s: Bar length inside that bound.
        far_interval_s: Bar length outside that bound.
        near_dte: See ``select_spxw_expirations``.
        weekly_dte: See ``select_spxw_expirations``.
        standard_dte: See ``select_spxw_expirations``.
        roll_in_dte: See ``select_spxw_expirations``.

    Yields:
        Jobs ordered by session, expiration, then dataset.

    Raises:
        PlanRangeError: An expiration threshold is negative.
        ValueError: An interval argument is invalid.
    """
    for session in sessions:
        selected = select_spxw_expirations(
            session,
            expirations,
            near_dte=near_dte,
            weekly_dte=weekly_dte,
            standard_dte=standard_dte,
            roll_in_dte=roll_in_dte,
        )
        for expiration in selected:
            dte = calendar_dte(session, expiration)
            if max_dte is not None and dte > max_dte:
                continue
            for dataset in datasets:
                interval = _interval_for(
                    dataset,
                    dte,
                    one_minute_max_dte=one_minute_max_dte,
                    near_interval_s=near_interval_s,
                    far_interval_s=far_interval_s,
                )
                yield PlanJob(
                    dataset=dataset,
                    trade_date=session,
                    expiration=expiration,
                    interval_s=interval,
                    symbol=SYMBOL_SPXW,
                    calendar_dte=dte,
                )


def run_plan(
    expirations: Iterable[date],
    start: date,
    end: date,
    *,
    datasets: Sequence[str] | None = None,
    max_dte: int | None = None,
    queue: ProgressQueue | None = None,
    one_minute_max_dte: int = ONE_MINUTE_MAX_CALENDAR_DTE,
    near_interval_s: int = INTERVAL_1M_SECONDS,
    far_interval_s: int = INTERVAL_15M_SECONDS,
    horizon_days: int | None = FAR_EXPIRATION_HORIZON_DAYS,
    horizon_end: date | None = None,
    near_dte: int = NEAR_EXPIRATION_DTE,
    weekly_dte: int = WEEKLY_EXPIRATION_DTE,
    standard_dte: int = STANDARD_EXPIRATION_DTE,
    roll_in_dte: int = ROLL_IN_EXPIRATION_DTE,
    stale_after: timedelta = STALE_RUNNING_AFTER,
    materialize: bool = True,
) -> PlanResult:
    """Plan jobs and queue the ones that are not already finished.

    A second call after every job is ``done`` or ``empty`` writes nothing and
    returns no jobs. It does not call ThetaData. The caller supplies the
    expiration list.

    Args:
        expirations: Synthetic or listed expiration dates.
        start: Requested first date. Dates before 2022-05-16 are clamped.
        end: Requested last date.
        datasets: Datasets to plan. ``None`` plans all five.
        max_dte: Optional calendar DTE cap applied after VS3D selection.
            ``None`` does not add a cap.
        queue: Progress queue. ``None`` uses a new in-memory queue.
        one_minute_max_dte: Inclusive DTE bound for the near interval.
        near_interval_s: Near bar length in seconds.
        far_interval_s: Far bar length in seconds.
        horizon_days: Far-future placeholder horizon. ``None`` disables it.
        horizon_end: Date the placeholder horizon is measured from. ``None``
            uses this plan's end. A one-session chunk of a longer backfill
            passes the backfill end so the kept expirations match one plan of
            the whole range.
        near_dte: Inclusive DTE that keeps every listed expiration.
        weekly_dte: Inclusive DTE for non-third-Friday weeklies.
        standard_dte: Inclusive DTE for monthlies, quarterlies, and end-of-month.
        roll_in_dte: Inclusive DTE for the next standard expiration past
            ``standard_dte``.
        stale_after: Running rows older than this are reset to pending first.
        materialize: When true, ``jobs`` lists the pending queue. The queue
            itself is still synced one session at a time.

    Returns:
        Counts and, when requested, the pending jobs.

    Raises:
        PlanRangeError: A date, dataset, cap, interval, horizon anchor, or
            expiration window is invalid.
        PlanFloorError: The range ends before 2022-05-16.
    """
    effective_start, effective_end, clamped = resolve_range(start, end)
    selected = parse_datasets(datasets)
    _validate_limits(max_dte, one_minute_max_dte, near_interval_s, far_interval_s)
    _validate_expiration_window(near_dte, weekly_dte, standard_dte, roll_in_dte)
    anchor = _horizon_anchor(effective_end, horizon_end)
    sessions = trading_sessions(effective_start, effective_end)
    listed = tuple(sorted(set(expirations)))
    kept, dropped = partition_expirations(listed, plan_end=anchor, horizon_days=horizon_days)
    store = queue if queue is not None else MemoryProgressStore()
    reset_running = store.reset_stale(
        datetime.now(timezone.utc) - stale_after,
        start=effective_start,
        end=effective_end,
        datasets=selected,
    )
    keys: set[tuple[str, date, date]] = set()
    tally = _Tally(selected, near_interval_s, far_interval_s)
    parts: list[SyncStats] = []
    for session in sessions:
        parts.append(
            store.sync(
                _counted_session(
                    session.trade_date,
                    kept,
                    datasets=selected,
                    max_dte=max_dte,
                    one_minute_max_dte=one_minute_max_dte,
                    near_interval_s=near_interval_s,
                    far_interval_s=far_interval_s,
                    near_dte=near_dte,
                    weekly_dte=weekly_dte,
                    standard_dte=standard_dte,
                    roll_in_dte=roll_in_dte,
                    tally=tally,
                    keys=keys if materialize else None,
                )
            )
        )
    stats = _merge_stats(parts)
    pending: tuple[PlanJob, ...] = ()
    if materialize and sessions:
        pending = tuple(
            job
            for job in store.pending_jobs(
                start=effective_start,
                end=effective_end,
                datasets=selected,
            )
            if job.key() in keys
        )
    counts = tally.finish(
        sessions=len(sessions),
        half_days=sum(1 for session in sessions if session.half_day),
        listed=len(listed),
        placeholders=len(dropped),
        clamped=clamped,
        stats=stats,
        reset_running=reset_running,
    )
    return PlanResult(counts=counts, jobs=pending, start=effective_start, end=effective_end)


def format_plan_counts(result: PlanResult) -> str:
    """Format a plan as counts only.

    Args:
        result: Plan to print.

    Returns:
        One ``name=count`` line per figure, ending in a newline. No dates,
        expirations, or row contents.
    """
    counts = result.counts
    lines = [
        f"sessions={counts.sessions}",
        f"half_days={counts.half_days}",
        f"listed={counts.listed}",
        f"placeholders={counts.placeholders}",
        f"expirations={counts.expirations}",
        f"clamped={int(counts.clamped)}",
        f"jobs={counts.jobs}",
        f"pending={counts.pending}",
        f"skipped_done={counts.skipped_done}",
        f"skipped_empty={counts.skipped_empty}",
        f"skipped_running={counts.skipped_running}",
        f"requeued_failed={counts.requeued_failed}",
        f"reset_running={counts.reset_running}",
        f"writes={counts.writes}",
    ]
    for name in DATASETS:
        lines.append(f"{name}={counts.by_dataset.get(name, 0)}")
    for label in _interval_labels(counts.by_interval):
        lines.append(f"interval_{label}={counts.by_interval[label]}")
    return "\n".join(lines) + "\n"


class _ExpirationRow(Protocol):
    """Row that carries an expiration date."""

    expiration: date


class _ExpirationSource(Protocol):
    """Client method the planner uses for ``option/list/expirations``."""

    def list_expirations(self, symbol: str) -> AsyncIterator[_ExpirationRow]:
        """Stream expirations for one symbol.

        Args:
            symbol: Underlying symbol.

        Returns:
            An async iterator of expiration rows.
        """
        ...


async def load_spxw_expirations(client: _ExpirationSource) -> list[date]:
    """Read SPXW expirations from an already open client.

    Args:
        client: Object with ``list_expirations(symbol)`` returning expiration rows.
            Tests pass a fake. The CLI passes ``ThetaDataClient``.

    Returns:
        Expiration dates in stream order.
    """
    rows: list[date] = []
    async for row in client.list_expirations(SYMBOL_SPXW):
        rows.append(row.expiration)
    return rows


def _fetch_spxw_expirations(api_base_url: str) -> list[date]:
    """Fetch SPXW expirations from Theta Terminal.

    Args:
        api_base_url: v3 base URL. Not written to the plan output.

    Returns:
        Expiration dates from ``option/list/expirations?symbol=SPXW``.
    """

    async def _fetch() -> list[date]:
        """Open a client and stream the expiration list.

        Returns:
            Expiration dates.
        """
        from curator.thetadata.client import ThetaDataClient
        from curator.thetadata.config import Settings

        settings = Settings(api_base_url=api_base_url)
        async with ThetaDataClient(settings) as client:
            return await load_spxw_expirations(client)

    return asyncio.run(_fetch())


# A variable, not a function object, so tests can replace the loader.
fetch_spxw_expirations: Callable[[str], list[date]] = _fetch_spxw_expirations


def _counted_session(
    session: date,
    expirations: Sequence[date],
    *,
    datasets: Sequence[str],
    max_dte: int | None,
    one_minute_max_dte: int,
    near_interval_s: int,
    far_interval_s: int,
    near_dte: int,
    weekly_dte: int,
    standard_dte: int,
    roll_in_dte: int,
    tally: _Tally,
    keys: set[tuple[str, date, date]] | None,
) -> Iterator[PlanJob]:
    """Yield one session's jobs and count them.

    Args:
        session: NYSE session.
        expirations: Expirations inside the placeholder horizon.
        datasets: Datasets to emit.
        max_dte: Optional extra DTE cap.
        one_minute_max_dte: Inclusive DTE bound for the near interval.
        near_interval_s: Near bar length.
        far_interval_s: Far bar length.
        near_dte: VS3D near window.
        weekly_dte: VS3D weekly window.
        standard_dte: VS3D standard window.
        roll_in_dte: VS3D roll-in window.
        tally: Running counts for the whole plan.
        keys: When set, each job key is recorded so a materialized result can
            drop rows this plan did not produce.

    Yields:
        The session's jobs.
    """
    produced = iter_jobs(
        (session,),
        expirations,
        datasets=datasets,
        max_dte=max_dte,
        one_minute_max_dte=one_minute_max_dte,
        near_interval_s=near_interval_s,
        far_interval_s=far_interval_s,
        near_dte=near_dte,
        weekly_dte=weekly_dte,
        standard_dte=standard_dte,
        roll_in_dte=roll_in_dte,
    )
    for job in produced:
        if keys is not None:
            keys.add(job.key())
        tally.add(job)
        yield job


def _merge_stats(parts: Sequence[SyncStats]) -> SyncStats:
    """Add per-session sync results.

    Args:
        parts: One result per session. Empty when the range has no sessions.

    Returns:
        The combined counts. ``pending`` follows from the summed fields.
    """
    return SyncStats(
        inserted=sum(part.inserted for part in parts),
        requeued_failed=sum(part.requeued_failed for part in parts),
        touched=sum(part.touched for part in parts),
        skipped_done=sum(part.skipped_done for part in parts),
        skipped_empty=sum(part.skipped_empty for part in parts),
        skipped_pending=sum(part.skipped_pending for part in parts),
        skipped_running=sum(part.skipped_running for part in parts),
        writes=sum(part.writes for part in parts),
    )


def _horizon_anchor(plan_end: date, horizon_end: date | None) -> date:
    """Return the date the far-expiration horizon is measured from.

    Args:
        plan_end: Last session in this plan call.
        horizon_end: Caller override. ``None`` uses ``plan_end``.

    Returns:
        The anchor date.

    Raises:
        PlanRangeError: ``horizon_end`` is before ``plan_end``.
    """
    if horizon_end is None:
        return plan_end
    if horizon_end < plan_end:
        raise PlanRangeError("horizon_end is before the plan end")
    return horizon_end


def _validate_expiration_window(near_dte: int, weekly_dte: int, standard_dte: int, roll_in_dte: int) -> None:
    """Reject a negative VS3D threshold.

    Args:
        near_dte: Near window.
        weekly_dte: Weekly window.
        standard_dte: Standard window.
        roll_in_dte: Roll-in window.

    Raises:
        PlanRangeError: A threshold is negative.
    """
    if near_dte < 0 or weekly_dte < 0 or standard_dte < 0 or roll_in_dte < 0:
        raise PlanRangeError("expiration windows must be >= 0")


def _keep_listed(
    expiration: date,
    dte: int,
    standards: set[date],
    near_dte: int,
    weekly_dte: int,
    standard_dte: int,
) -> bool:
    """Return whether selection keeps this expiration before the roll-in.

    Args:
        expiration: Listed expiration.
        dte: Calendar DTE from the session. Never negative.
        standards: Monthlies, quarterlies, and end-of-month dates in the list.
        near_dte: Inclusive DTE that keeps every listed expiration.
        weekly_dte: Inclusive DTE for a Friday that is not the third Friday.
        standard_dte: Inclusive DTE for a date in ``standards``.

    Returns:
        True when the expiration is inside one of those windows.
    """
    if dte <= near_dte:
        return True
    if _is_weekly_friday(expiration) and dte <= weekly_dte:
        return True
    return expiration in standards and dte <= standard_dte


def _is_weekly_friday(expiration: date) -> bool:
    """Return whether a date is a Friday and not the third Friday of its month.

    Args:
        expiration: Listed expiration.

    Returns:
        True for a weekly Friday. The third Friday is a monthly, including
        when that Friday is an NYSE holiday.
    """
    return expiration.weekday() == 4 and expiration != _third_friday(expiration.year, expiration.month)


def _standard_expirations(listed: set[date]) -> set[date]:
    """Return monthlies, quarterlies, and end-of-month dates that are listed.

    Quarterlies are the monthlies of March, June, September, and December, so
    they are included by collecting monthlies. End-of-month is the last listed
    weekday expiration of each month.

    Args:
        listed: Unique expiration dates.

    Returns:
        The standard dates that appear in ``listed``.
    """
    found: set[date] = set()
    for year, month in {(day.year, day.month) for day in listed}:
        monthly = _listed_monthly(year, month, listed)
        if monthly is not None:
            found.add(monthly)
    found.update(_eom_expirations(listed))
    return found


def _listed_monthly(year: int, month: int, listed: set[date]) -> date | None:
    """Return the monthly expiration when it is in the listed set.

    The monthly is the third Friday. When that Friday is not an NYSE session,
    it is the Thursday before, and only if that Thursday is listed.

    Args:
        year: Calendar year.
        month: Calendar month, 1 through 12.
        listed: Unique expiration dates.

    Returns:
        The listed monthly, or ``None`` when the candidate is not listed.
    """
    friday = _third_friday(year, month)
    if trading_sessions(friday, friday):
        candidate = friday
    else:
        candidate = friday - timedelta(days=1)
        if not trading_sessions(candidate, candidate):
            return None
    if candidate in listed:
        return candidate
    return None


def _third_friday(year: int, month: int) -> date:
    """Return the third Friday of a month.

    Args:
        year: Calendar year.
        month: Calendar month, 1 through 12.

    Returns:
        The third Friday, whether or not the exchange is open.
    """
    first = date(year, month, 1)
    offset = (4 - first.weekday()) % 7
    return first + timedelta(days=offset + 14)


def _eom_expirations(listed: set[date]) -> set[date]:
    """Return the last listed weekday expiration of each month.

    Args:
        listed: Unique expiration dates.

    Returns:
        One date per month that has a listed Monday-through-Friday expiration.
    """
    by_month: dict[tuple[int, int], date] = {}
    for day in listed:
        if day.weekday() >= 5:
            continue
        key = (day.year, day.month)
        current = by_month.get(key)
        if current is None or day > current:
            by_month[key] = day
    return set(by_month.values())


def _interval_for(
    dataset: str,
    dte: int,
    *,
    one_minute_max_dte: int,
    near_interval_s: int,
    far_interval_s: int,
) -> int | None:
    """Return the bar length for one dataset and DTE.

    Args:
        dataset: Dataset name.
        dte: Calendar DTE.
        one_minute_max_dte: Inclusive bound for the near interval.
        near_interval_s: Near bar length.
        far_interval_s: Far bar length.

    Returns:
        ``None`` for daily datasets, otherwise the intraday bar length.
    """
    if dataset not in DATASETS_INTRADAY:
        return None
    return interval_seconds_for_dte(
        dte,
        one_minute_max_dte=one_minute_max_dte,
        near_interval_s=near_interval_s,
        far_interval_s=far_interval_s,
    )


def _validate_limits(
    max_dte: int | None,
    one_minute_max_dte: int,
    near_interval_s: int,
    far_interval_s: int,
) -> None:
    """Reject a negative cap or a non-positive interval before any queue write.

    Args:
        max_dte: Optional DTE cap.
        one_minute_max_dte: Near-interval bound.
        near_interval_s: Near bar length.
        far_interval_s: Far bar length.

    Raises:
        PlanRangeError: A value is out of range.
    """
    if max_dte is not None and max_dte < 0:
        raise PlanRangeError("max_dte must be >= 0")
    try:
        interval_seconds_for_dte(
            0,
            one_minute_max_dte=one_minute_max_dte,
            near_interval_s=near_interval_s,
            far_interval_s=far_interval_s,
        )
    except ValueError as exc:
        raise PlanRangeError(str(exc)) from None


def _interval_labels(counts: Mapping[str, int]) -> list[str]:
    """Return interval labels with ``daily`` first, then numeric lengths.

    Args:
        counts: Interval counts.

    Returns:
        Labels in print order.
    """

    def sort_key(label: str) -> tuple[int, int]:
        """Sort ``daily`` ahead of numeric bar lengths.

        Args:
            label: Interval label.

        Returns:
            A sort key.
        """
        if label == "daily":
            return (0, 0)
        return (1, int(label))

    return sorted(counts, key=sort_key)


class _Tally:
    """Running job counts for one plan."""

    def __init__(self, datasets: Sequence[str], near_interval_s: int, far_interval_s: int) -> None:
        """Start every requested dataset and the default interval buckets at zero.

        Args:
            datasets: Datasets being planned.
            near_interval_s: Near bar length to show even when its count is zero.
            far_interval_s: Far bar length to show even when its count is zero.
        """
        self._datasets = {name: 0 for name in datasets}
        self._intervals = {
            "daily": 0,
            str(near_interval_s): 0,
            str(far_interval_s): 0,
        }
        self._expirations: set[date] = set()
        self.jobs = 0

    def add(self, job: PlanJob) -> None:
        """Count one job.

        Args:
            job: Job about to be synced.
        """
        self.jobs += 1
        self._datasets[job.dataset] = self._datasets.get(job.dataset, 0) + 1
        label = "daily" if job.interval_s is None else str(job.interval_s)
        self._intervals[label] = self._intervals.get(label, 0) + 1
        self._expirations.add(job.expiration)

    def finish(
        self,
        *,
        sessions: int,
        half_days: int,
        listed: int,
        placeholders: int,
        clamped: bool,
        stats: SyncStats,
        reset_running: int,
    ) -> PlanCounts:
        """Combine job totals with queue results.

        Args:
            sessions: NYSE session count.
            half_days: Half-day count.
            listed: Unique input expirations.
            placeholders: Dropped far-future expirations.
            clamped: Whether the start was moved to the floor.
            stats: Queue sync result.
            reset_running: Stale running rows reset before sync.

        Returns:
            The count summary.
        """
        return PlanCounts(
            sessions=sessions,
            half_days=half_days,
            listed=listed,
            placeholders=placeholders,
            expirations=len(self._expirations),
            clamped=clamped,
            jobs=self.jobs,
            pending=stats.pending,
            skipped_done=stats.skipped_done,
            skipped_empty=stats.skipped_empty,
            skipped_running=stats.skipped_running,
            requeued_failed=stats.requeued_failed,
            reset_running=reset_running,
            writes=stats.writes + reset_running,
            by_dataset=dict(self._datasets),
            by_interval=dict(self._intervals),
        )
