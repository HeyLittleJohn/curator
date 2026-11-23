"""SPXW job planner.

Jobs are ``(dataset, trade_date, expiration)`` for every NYSE session in range
and every expiration from ``option/list/expirations?symbol=SPXW`` with calendar
DTE of at least 0. Expirations are never inferred from a weekday rule.

Intraday datasets use a 1-minute bar when calendar DTE is 0 through 5, and a
15-minute bar otherwise. The cutoff and both lengths are arguments. Open
interest and EOD are daily: each job still names one expiration, the interval
is empty, and the downloader covers the full strike chain.

The default includes every listed expiration inside the horizon below, so a
far-dated position still appears in GEX. ``max_dte`` is an optional cap.

The live expiration list has returned 2305 dates from 2012-06-01 through
2089-06-30. That last date sits decades after any listed SPXW series and is
treated as a ThetaData placeholder. Expirations more than
``FAR_EXPIRATION_HORIZON_DAYS`` (10 years of 366 days) after the plan end are
counted and dropped. The horizon is not ``max_dte``. Pass ``horizon_days=None``
to keep every future date, including that placeholder.

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
    ONE_MINUTE_MAX_CALENDAR_DTE,
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


def iter_jobs(
    sessions: Sequence[date],
    expirations: Sequence[date],
    *,
    datasets: Sequence[str],
    max_dte: int | None,
    one_minute_max_dte: int,
    near_interval_s: int,
    far_interval_s: int,
) -> Iterator[PlanJob]:
    """Yield one job per dataset, session, and expiration with DTE >= 0.

    Args:
        sessions: NYSE session dates, in order.
        expirations: Kept expiration dates. Order does not matter.
        datasets: Datasets to emit.
        max_dte: Optional inclusive calendar DTE cap. ``None`` does not cap.
        one_minute_max_dte: Inclusive DTE bound for the near interval.
        near_interval_s: Bar length inside that bound.
        far_interval_s: Bar length outside that bound.

    Yields:
        Jobs ordered by session, expiration, then dataset.

    Raises:
        ValueError: An interval argument is invalid.
    """
    ordered = tuple(sorted(set(expirations)))
    for session in sessions:
        for expiration in ordered:
            dte = calendar_dte(session, expiration)
            if dte < 0:
                continue
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
        max_dte: Optional calendar DTE cap. ``None`` keeps every kept expiration.
        queue: Progress queue. ``None`` uses a new in-memory queue.
        one_minute_max_dte: Inclusive DTE bound for the near interval.
        near_interval_s: Near bar length in seconds.
        far_interval_s: Far bar length in seconds.
        horizon_days: Far-future placeholder horizon. ``None`` disables it.
        stale_after: Running rows older than this are reset to pending first.
        materialize: When true, ``jobs`` lists the pending queue.

    Returns:
        Counts and, when requested, the pending jobs.

    Raises:
        PlanRangeError: A date, dataset, cap, or interval is invalid.
        PlanFloorError: The range ends before 2022-05-16.
    """
    effective_start, effective_end, clamped = resolve_range(start, end)
    selected = parse_datasets(datasets)
    _validate_limits(max_dte, one_minute_max_dte, near_interval_s, far_interval_s)
    sessions = trading_sessions(effective_start, effective_end)
    listed = tuple(sorted(set(expirations)))
    kept, dropped = partition_expirations(listed, plan_end=effective_end, horizon_days=horizon_days)
    store = queue if queue is not None else MemoryProgressStore()
    reset_running = store.reset_stale(
        datetime.now(timezone.utc) - stale_after,
        start=effective_start,
        end=effective_end,
        datasets=selected,
    )
    produced = iter_jobs(
        tuple(session.trade_date for session in sessions),
        kept,
        datasets=selected,
        max_dte=max_dte,
        one_minute_max_dte=one_minute_max_dte,
        near_interval_s=near_interval_s,
        far_interval_s=far_interval_s,
    )
    keys: set[tuple[str, date, date]] = set()
    tally = _Tally(selected, near_interval_s, far_interval_s)

    def counted() -> Iterator[PlanJob]:
        """Count jobs as they are queued.

        Yields:
            The same jobs ``iter_jobs`` produced.
        """
        for job in produced:
            keys.add(job.key())
            tally.add(job)
            yield job

    stats = store.sync(counted())
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
