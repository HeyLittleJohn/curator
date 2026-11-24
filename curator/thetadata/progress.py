"""Resumable ``theta.progress`` queue.

Statuses are ``pending``, ``running``, ``done``, ``failed``, and ``empty``.
``done`` and ``empty`` are finished: a later plan does not rewrite them and
does not put them back on the queue. ``failed`` is requeued. ``running`` rows
older than the stale window return to ``pending`` on resume.

Claiming is ``UPDATE ... WHERE status = 'pending' ... FOR UPDATE SKIP LOCKED
RETURNING``, so two worker processes cannot take the same row. The in-memory
store uses one lock around the same transition for tests and for a plan that
has no database URL. CI does not open Postgres.
"""

from __future__ import annotations

import threading
from collections.abc import Generator, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Protocol

from sqlalchemy import Column, Date, Integer, SmallInteger, String, table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.dml import Insert

from curator.thetadata.config import THETA_SCHEMA
from curator.thetadata.identity import calendar_dte
from curator.thetadata.jobs import DATASET_NAMES, SYMBOL_SPXW, PlanJob
from curator.thetadata.partitions import quote_ident

PENDING = "pending"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
EMPTY = "empty"
STATUSES = frozenset({PENDING, RUNNING, DONE, FAILED, EMPTY})
FINISHED = frozenset({DONE, EMPTY})

# A worker that died mid-request leaves ``running``. Resume returns those rows
# to ``pending`` once they are at least this old. A live worker stays running.
STALE_RUNNING_AFTER = timedelta(hours=1)

_SCHEMA = quote_ident(THETA_SCHEMA)
_INSERT_BATCH = 1000
_PROGRESS = table(
    "progress",
    Column("dataset", String),
    Column("date", Date),
    Column("expiration", Date),
    Column("status", String),
    Column("row_count", Integer),
    Column("interval_s", SmallInteger),
    schema=THETA_SCHEMA,
)


class ProgressError(ValueError):
    """A progress row or status is not usable."""


@dataclass(frozen=True)
class ProgressSnapshot:
    """One stored progress row, without the database URL.

    Args:
        status: Current status.
        interval_s: Stored bar length, or ``None`` for a daily dataset.
        row_count: Rows last recorded for the job.
        updated_at: Last status change, timezone-aware.
    """

    status: str
    interval_s: int | None
    row_count: int
    updated_at: datetime


@dataclass(frozen=True)
class RangeCensus:
    """How many progress rows in one session range sit in each status.

    Args:
        pending: Rows waiting to be claimed.
        running: Rows a worker currently holds.
        done: Rows that stored at least one fact.
        failed: Rows the last attempt could not finish.
        empty: Rows the terminal had no rows for, or whose strike window was empty.
    """

    pending: int = 0
    running: int = 0
    done: int = 0
    failed: int = 0
    empty: int = 0


@dataclass(frozen=True)
class SyncStats:
    """What one ``sync`` call did.

    Args:
        inserted: New pending rows.
        requeued_failed: Failed rows returned to pending.
        touched: Pending rows whose interval changed.
        skipped_done: Finished rows left alone.
        skipped_empty: Empty rows left alone.
        skipped_pending: Pending rows that already matched.
        skipped_running: Fresh running rows left alone.
        writes: Inserts plus updates. A finished rerun is zero.
    """

    inserted: int = 0
    requeued_failed: int = 0
    touched: int = 0
    skipped_done: int = 0
    skipped_empty: int = 0
    skipped_pending: int = 0
    skipped_running: int = 0
    writes: int = 0

    @property
    def pending(self) -> int:
        """Return how many of the synced jobs are pending afterward.

        Returns:
            Inserted, requeued, touched, and already-pending jobs.
        """
        return self.inserted + self.requeued_failed + self.touched + self.skipped_pending


class ProgressQueue(Protocol):
    """The planner's view of the progress table."""

    def reset_stale(
        self,
        stale_before: datetime,
        *,
        start: date,
        end: date,
        datasets: Sequence[str],
    ) -> int:
        """Return stale running rows in range to pending.

        Args:
            stale_before: Rows last updated before this instant are stale.
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to reset.

        Returns:
            The number of rows changed.
        """
        ...

    def sync(self, jobs: Iterable[PlanJob]) -> SyncStats:
        """Insert missing jobs and requeue failed ones.

        Args:
            jobs: Plan output. Finished keys are not written.

        Returns:
            Counts for this call.
        """
        ...

    def claim(
        self,
        limit: int = 1,
        *,
        start: date | None = None,
        end: date | None = None,
        datasets: Sequence[str] | None = None,
    ) -> list[PlanJob]:
        """Move up to ``limit`` pending jobs to running.

        Args:
            limit: Maximum jobs to take.
            start: First session date, inclusive. ``None`` does not filter.
            end: Last session date, inclusive. ``None`` does not filter.
            datasets: Datasets to claim. ``None`` claims every dataset.

        Returns:
            The claimed jobs. Empty when nothing pending matches.
        """
        ...

    def census(self, *, start: date, end: date, datasets: Sequence[str]) -> RangeCensus:
        """Count rows in a session range by status.

        Args:
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to count.

        Returns:
            One count per status. Missing statuses are zero.
        """
        ...

    def mark(self, job: PlanJob, status: str, row_count: int) -> None:
        """Set the status and row count of an existing job.

        Args:
            job: Job identity.
            status: One of the progress statuses.
            row_count: Non-negative row count.
        """
        ...

    def pending_jobs(self, *, start: date, end: date, datasets: Sequence[str]) -> list[PlanJob]:
        """Return pending jobs in a session range.

        Args:
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to include.

        Returns:
            Pending jobs ordered by session, expiration, then dataset.
        """
        ...

    def set_updated_at(self, job: PlanJob, updated_at: datetime) -> None:
        """Set the row clock. Resume uses it to decide a worker died.

        Args:
            job: Existing job.
            updated_at: Timezone-aware timestamp.
        """
        ...


def classify_job(existing: ProgressSnapshot | None, job: PlanJob) -> str:
    """Decide what a plan should do with one existing row.

    Args:
        existing: Stored row, or ``None`` when the key is new.
        job: Job the planner wants in the queue.

    Returns:
        ``insert``, ``requeue``, ``touch``, ``skip_done``, ``skip_empty``,
        ``skip_pending``, or ``skip_running``.

    Raises:
        ProgressError: The stored status is not one of the five statuses.
    """
    if existing is None:
        return "insert"
    if existing.status == FAILED:
        return "requeue"
    if existing.status == PENDING and existing.interval_s != job.interval_s:
        return "touch"
    if existing.status == DONE:
        return "skip_done"
    if existing.status == EMPTY:
        return "skip_empty"
    if existing.status == PENDING:
        return "skip_pending"
    if existing.status == RUNNING:
        return "skip_running"
    raise ProgressError("unknown progress status")


def _require_aware(value: datetime) -> datetime:
    """Reject a naive timestamp.

    Args:
        value: Timestamp to check.

    Returns:
        The same timestamp.

    Raises:
        ProgressError: ``value`` has no timezone.
    """
    if value.tzinfo is None:
        raise ProgressError("progress timestamps must be timezone-aware")
    return value


def _require_status(status: str, row_count: int) -> None:
    """Reject an unknown status or a negative row count.

    Args:
        status: Proposed status.
        row_count: Proposed row count.

    Raises:
        ProgressError: The status is unknown or ``row_count`` is negative.
    """
    if status not in STATUSES:
        raise ProgressError("unknown progress status")
    if row_count < 0:
        raise ProgressError("row_count must be >= 0")


def _check_datasets(datasets: Sequence[str]) -> tuple[str, ...]:
    """Return dataset names that are safe to interpolate into SQL.

    Args:
        datasets: Requested datasets.

    Returns:
        The names, in order, without duplicates.

    Raises:
        ProgressError: A name is not one of the SPXW datasets, or the list is empty.
    """
    if not datasets:
        raise ProgressError("at least one dataset is required")
    names: list[str] = []
    for name in datasets:
        if name not in DATASET_NAMES:
            raise ProgressError("unknown dataset")
        if name not in names:
            names.append(name)
    return tuple(names)


def _dataset_in(datasets: Sequence[str]) -> str:
    """Return a SQL ``dataset IN (...)`` fragment.

    Args:
        datasets: Already validated dataset names.

    Returns:
        A fragment such as ``dataset IN ('quotes', 'eod')``.
    """
    quoted = ", ".join(f"'{name}'" for name in datasets)
    return f"dataset IN ({quoted})"


def _job_from_row(
    dataset: str,
    trade_date: date,
    expiration: date,
    interval_s: int | None,
) -> PlanJob:
    """Build a job from progress columns.

    Args:
        dataset: Dataset name.
        trade_date: Session date.
        expiration: Expiration date.
        interval_s: Stored bar length.

    Returns:
        A picklable job. The symbol is SPXW.
    """
    return PlanJob(
        dataset=dataset,
        trade_date=trade_date,
        expiration=expiration,
        interval_s=interval_s,
        symbol=SYMBOL_SPXW,
        calendar_dte=calendar_dte(trade_date, expiration),
    )


def claim_sql(
    *,
    start: date | None = None,
    end: date | None = None,
    datasets: Sequence[str] | None = None,
) -> str:
    """Return the atomic pending-to-running statement.

    Args:
        start: When set, the statement binds ``:start``.
        end: When set, the statement binds ``:end``.
        datasets: When set, already-validated dataset names are inlined.

    Returns:
        SQL that locks pending rows with ``FOR UPDATE SKIP LOCKED`` and marks
        the locked rows ``running``. ``:limit`` is always bound.
    """
    filters = [f"status = '{PENDING}'"]
    if start is not None:
        filters.append('"date" >= :start')
    if end is not None:
        filters.append('"date" <= :end')
    if datasets is not None:
        filters.append(_dataset_in(datasets))
    where = " AND ".join(filters)
    return f"""
WITH picked AS (
    SELECT dataset, "date", expiration
    FROM {_SCHEMA}.progress
    WHERE {where}
    ORDER BY "date", expiration, dataset
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
)
UPDATE {_SCHEMA}.progress AS progress
SET status = '{RUNNING}',
    updated_at = clock_timestamp()
FROM picked
WHERE progress.dataset = picked.dataset
  AND progress."date" = picked."date"
  AND progress.expiration = picked.expiration
RETURNING progress.dataset,
          progress."date" AS trade_date,
          progress.expiration,
          progress.interval_s,
          progress.row_count
"""


@dataclass
class _MemoryRow:
    """Mutable progress row held by ``MemoryProgressStore``."""

    dataset: str
    trade_date: date
    expiration: date
    status: str
    interval_s: int | None
    row_count: int
    updated_at: datetime

    def snapshot(self) -> ProgressSnapshot:
        """Return the immutable view used by ``classify_job``.

        Returns:
            Status, interval, row count, and timestamp.
        """
        return ProgressSnapshot(
            status=self.status,
            interval_s=self.interval_s,
            row_count=self.row_count,
            updated_at=self.updated_at,
        )

    def to_job(self) -> PlanJob:
        """Return the picklable job for this row.

        Returns:
            A ``PlanJob`` with calendar DTE filled in.
        """
        return _job_from_row(self.dataset, self.trade_date, self.expiration, self.interval_s)


def _memory_pending(
    row: _MemoryRow,
    *,
    start: date | None,
    end: date | None,
    datasets: frozenset[str] | None,
) -> bool:
    """Return whether a memory row is pending and inside the claim filter.

    Args:
        row: Stored progress row.
        start: First session date, inclusive. ``None`` does not filter.
        end: Last session date, inclusive. ``None`` does not filter.
        datasets: Dataset names. ``None`` allows every dataset.

    Returns:
        True when the row can be claimed.
    """
    if row.status != PENDING:
        return False
    if start is not None and row.trade_date < start:
        return False
    if end is not None and row.trade_date > end:
        return False
    if datasets is not None and row.dataset not in datasets:
        return False
    return True


def _census_from(counts: dict[str, int]) -> RangeCensus:
    """Build a census from a status-to-count map.

    Args:
        counts: Counts keyed by status name. Missing keys are zero.

    Returns:
        The census.
    """
    return RangeCensus(
        pending=counts.get(PENDING, 0),
        running=counts.get(RUNNING, 0),
        done=counts.get(DONE, 0),
        failed=counts.get(FAILED, 0),
        empty=counts.get(EMPTY, 0),
    )


class MemoryProgressStore:
    """Process-local progress table.

    One lock covers reset, sync, claim, and mark, so threads in a test cannot
    claim the same job. This store does not open a connection.
    """

    def __init__(self) -> None:
        """Start with an empty table."""
        self._rows: dict[tuple[str, date, date], _MemoryRow] = {}
        self._lock = threading.Lock()
        self.writes = 0

    def reset_stale(
        self,
        stale_before: datetime,
        *,
        start: date,
        end: date,
        datasets: Sequence[str],
    ) -> int:
        """Return stale running rows in range to pending.

        Args:
            stale_before: Rows last updated before this instant are stale.
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to reset.

        Returns:
            The number of rows changed.
        """
        _require_aware(stale_before)
        allowed = set(_check_datasets(datasets))
        changed = 0
        now = datetime.now(timezone.utc)
        with self._lock:
            for row in self._rows.values():
                if row.dataset not in allowed or row.trade_date < start or row.trade_date > end:
                    continue
                if row.status == RUNNING and row.updated_at < stale_before:
                    row.status = PENDING
                    row.updated_at = now
                    changed += 1
            self.writes += changed
        return changed

    def sync(self, jobs: Iterable[PlanJob]) -> SyncStats:
        """Insert missing jobs and requeue failed ones.

        Args:
            jobs: Plan output. Finished keys are not written.

        Returns:
            Counts for this call.
        """
        stats = _empty_stats()
        now = datetime.now(timezone.utc)
        with self._lock:
            for job in jobs:
                action = classify_job(self._snapshot(job), job)
                self._apply(job, action, now, stats)
            self.writes += stats["writes"]
        return _stats_from(stats)

    def claim(
        self,
        limit: int = 1,
        *,
        start: date | None = None,
        end: date | None = None,
        datasets: Sequence[str] | None = None,
    ) -> list[PlanJob]:
        """Move up to ``limit`` pending jobs to running.

        Args:
            limit: Maximum jobs to take.
            start: First session date, inclusive. ``None`` does not filter.
            end: Last session date, inclusive. ``None`` does not filter.
            datasets: Datasets to claim. ``None`` claims every dataset.

        Returns:
            The claimed jobs, oldest session first.

        Raises:
            ProgressError: ``limit`` is below 1, or a dataset name is unknown.
        """
        if limit < 1:
            raise ProgressError("claim limit must be >= 1")
        allowed = frozenset(_check_datasets(datasets)) if datasets is not None else None
        now = datetime.now(timezone.utc)
        with self._lock:
            pending = [
                row for row in self._rows.values() if _memory_pending(row, start=start, end=end, datasets=allowed)
            ]
            pending.sort(key=lambda row: (row.trade_date, row.expiration, row.dataset))
            taken = pending[:limit]
            for row in taken:
                row.status = RUNNING
                row.updated_at = now
            return [row.to_job() for row in taken]

    def census(self, *, start: date, end: date, datasets: Sequence[str]) -> RangeCensus:
        """Count rows in a session range by status.

        Args:
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to count.

        Returns:
            One count per status. Missing statuses are zero.

        Raises:
            ProgressError: A dataset name is unknown.
        """
        allowed = frozenset(_check_datasets(datasets))
        counts = {status: 0 for status in STATUSES}
        with self._lock:
            for row in self._rows.values():
                if row.dataset not in allowed or row.trade_date < start or row.trade_date > end:
                    continue
                counts[row.status] = counts.get(row.status, 0) + 1
        return _census_from(counts)

    def mark(self, job: PlanJob, status: str, row_count: int) -> None:
        """Set the status and row count of an existing job.

        Args:
            job: Job identity.
            status: One of the progress statuses.
            row_count: Non-negative row count.

        Raises:
            KeyError: The job has not been synced.
            ProgressError: The status or row count is invalid.
        """
        _require_status(status, row_count)
        now = datetime.now(timezone.utc)
        with self._lock:
            row = self._rows.get(job.key())
            if row is None:
                raise KeyError("progress row does not exist")
            row.status = status
            row.row_count = row_count
            row.interval_s = job.interval_s
            row.updated_at = now
            self.writes += 1

    def pending_jobs(self, *, start: date, end: date, datasets: Sequence[str]) -> list[PlanJob]:
        """Return pending jobs in a session range.

        Args:
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to include.

        Returns:
            Pending jobs ordered by session, expiration, then dataset.
        """
        allowed = set(_check_datasets(datasets))
        with self._lock:
            rows = [
                row
                for row in self._rows.values()
                if row.status == PENDING and row.dataset in allowed and start <= row.trade_date <= end
            ]
            rows.sort(key=lambda row: (row.trade_date, row.expiration, row.dataset))
            return [row.to_job() for row in rows]

    def set_updated_at(self, job: PlanJob, updated_at: datetime) -> None:
        """Set the row clock.

        Args:
            job: Existing job.
            updated_at: Timezone-aware timestamp.

        Raises:
            KeyError: The job has not been synced.
            ProgressError: ``updated_at`` is naive.
        """
        _require_aware(updated_at)
        with self._lock:
            row = self._rows.get(job.key())
            if row is None:
                raise KeyError("progress row does not exist")
            row.updated_at = updated_at

    def _snapshot(self, job: PlanJob) -> ProgressSnapshot | None:
        """Return the stored row for ``job``.

        Args:
            job: Job identity.

        Returns:
            The snapshot, or ``None`` when the key is new. Caller holds the lock.
        """
        row = self._rows.get(job.key())
        if row is None:
            return None
        return row.snapshot()

    def _apply(self, job: PlanJob, action: str, now: datetime, stats: dict[str, int]) -> None:
        """Apply one classified action.

        Args:
            job: Job to write or skip.
            action: Result of ``classify_job``.
            now: Timestamp for a write.
            stats: Mutable counters. Caller holds the lock.
        """
        if action == "insert":
            self._rows[job.key()] = _MemoryRow(
                dataset=job.dataset,
                trade_date=job.trade_date,
                expiration=job.expiration,
                status=PENDING,
                interval_s=job.interval_s,
                row_count=0,
                updated_at=now,
            )
            stats["inserted"] += 1
            stats["writes"] += 1
            return
        if action == "requeue":
            row = self._rows[job.key()]
            row.status = PENDING
            row.interval_s = job.interval_s
            row.row_count = 0
            row.updated_at = now
            stats["requeued_failed"] += 1
            stats["writes"] += 1
            return
        if action == "touch":
            row = self._rows[job.key()]
            row.interval_s = job.interval_s
            row.updated_at = now
            stats["touched"] += 1
            stats["writes"] += 1
            return
        stats[_skip_counter(action)] += 1


_SKIP_COUNTERS = {
    "skip_done": "skipped_done",
    "skip_empty": "skipped_empty",
    "skip_pending": "skipped_pending",
    "skip_running": "skipped_running",
}


def _skip_counter(action: str) -> str:
    """Map a skip action to a ``SyncStats`` field.

    Args:
        action: ``skip_done``, ``skip_empty``, ``skip_pending``, or ``skip_running``.

    Returns:
        The matching counter name.

    Raises:
        ProgressError: ``action`` is not a skip.
    """
    try:
        return _SKIP_COUNTERS[action]
    except KeyError as exc:
        raise ProgressError("unknown progress action") from exc


def _empty_stats() -> dict[str, int]:
    """Return zeroed sync counters.

    Returns:
        A mutable counter mapping.
    """
    return {
        "inserted": 0,
        "requeued_failed": 0,
        "touched": 0,
        "skipped_done": 0,
        "skipped_empty": 0,
        "skipped_pending": 0,
        "skipped_running": 0,
        "writes": 0,
    }


def _stats_from(stats: dict[str, int]) -> SyncStats:
    """Freeze mutable counters.

    Args:
        stats: Counter mapping from ``_empty_stats``.

    Returns:
        A ``SyncStats`` value.
    """
    return SyncStats(
        inserted=stats["inserted"],
        requeued_failed=stats["requeued_failed"],
        touched=stats["touched"],
        skipped_done=stats["skipped_done"],
        skipped_empty=stats["skipped_empty"],
        skipped_pending=stats["skipped_pending"],
        skipped_running=stats["skipped_running"],
        writes=stats["writes"],
    )


class PostgresProgressStore:
    """``theta.progress`` on one SQLAlchemy connection.

    The caller owns the transaction. ``claim`` locks rows until that
    transaction commits, and other sessions skip those rows.
    """

    def __init__(self, connection: Connection) -> None:
        """Store a connection. The URL is not kept on this object.

        Args:
            connection: Open connection with parameters hidden by the engine.
        """
        self._connection = connection

    def reset_stale(
        self,
        stale_before: datetime,
        *,
        start: date,
        end: date,
        datasets: Sequence[str],
    ) -> int:
        """Return stale running rows in range to pending.

        Args:
            stale_before: Rows last updated before this instant are stale.
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to reset.

        Returns:
            The number of rows changed.
        """
        _require_aware(stale_before)
        names = _check_datasets(datasets)
        statement = text(
            f"""
            UPDATE {_SCHEMA}.progress
            SET status = '{PENDING}', updated_at = clock_timestamp()
            WHERE status = '{RUNNING}'
              AND updated_at < :stale_before
              AND "date" BETWEEN :start AND :end
              AND {_dataset_in(names)}
            RETURNING dataset
            """
        )
        rows = self._connection.execute(
            statement,
            {"stale_before": stale_before, "start": start, "end": end},
        ).fetchall()
        return len(rows)

    def sync(self, jobs: Iterable[PlanJob]) -> SyncStats:
        """Insert missing jobs and requeue failed ones.

        Args:
            jobs: Plan output. Finished keys are not written.

        Returns:
            Counts for this call. A finished rerun issues no INSERT or UPDATE.
        """
        job_list = list(jobs)
        if not job_list:
            return SyncStats()
        existing = self._load_existing(job_list)
        buckets: dict[str, list[PlanJob]] = {
            "insert": [],
            "requeue": [],
            "touch": [],
        }
        stats = _empty_stats()
        for job in job_list:
            action = classify_job(existing.get(job.key()), job)
            if action in buckets:
                buckets[action].append(job)
            else:
                stats[_skip_counter(action)] += 1
        stats["inserted"] = self._insert_pending(buckets["insert"])
        stats["requeued_failed"] = self._requeue(buckets["requeue"])
        stats["touched"] = self._touch(buckets["touch"])
        stats["writes"] = stats["inserted"] + stats["requeued_failed"] + stats["touched"]
        return _stats_from(stats)

    def claim(
        self,
        limit: int = 1,
        *,
        start: date | None = None,
        end: date | None = None,
        datasets: Sequence[str] | None = None,
    ) -> list[PlanJob]:
        """Move up to ``limit`` pending jobs to running.

        Args:
            limit: Maximum jobs to take.
            start: First session date, inclusive. ``None`` does not filter.
            end: Last session date, inclusive. ``None`` does not filter.
            datasets: Datasets to claim. ``None`` claims every dataset.

        Returns:
            The claimed jobs.

        Raises:
            ProgressError: ``limit`` is below 1, or a dataset name is unknown.
        """
        if limit < 1:
            raise ProgressError("claim limit must be >= 1")
        names = _check_datasets(datasets) if datasets is not None else None
        params: dict[str, object] = {"limit": limit}
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end
        rows = self._connection.execute(text(claim_sql(start=start, end=end, datasets=names)), params).mappings().all()
        return [_job_from_row(row["dataset"], row["trade_date"], row["expiration"], row["interval_s"]) for row in rows]

    def census(self, *, start: date, end: date, datasets: Sequence[str]) -> RangeCensus:
        """Count rows in a session range by status.

        Args:
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to count.

        Returns:
            One count per status. Missing statuses are zero.

        Raises:
            ProgressError: A dataset name is unknown.
        """
        names = _check_datasets(datasets)
        rows = self._connection.execute(
            text(
                f"""
                SELECT status, count(*) AS n
                FROM {_SCHEMA}.progress
                WHERE "date" BETWEEN :start AND :end
                  AND {_dataset_in(names)}
                GROUP BY status
                """
            ),
            {"start": start, "end": end},
        ).mappings()
        counts = {status: 0 for status in STATUSES}
        for row in rows:
            counts[str(row["status"])] = int(row["n"])
        return _census_from(counts)

    def mark(self, job: PlanJob, status: str, row_count: int) -> None:
        """Set the status and row count of an existing job.

        Args:
            job: Job identity.
            status: One of the progress statuses.
            row_count: Non-negative row count.

        Raises:
            KeyError: The job has not been synced.
            ProgressError: The status or row count is invalid.
        """
        _require_status(status, row_count)
        result = self._connection.execute(
            text(
                f"""
                UPDATE {_SCHEMA}.progress
                SET status = :status,
                    row_count = :row_count,
                    interval_s = :interval_s,
                    updated_at = clock_timestamp()
                WHERE dataset = :dataset
                  AND "date" = :trade_date
                  AND expiration = :expiration
                RETURNING dataset
                """
            ),
            {
                "status": status,
                "row_count": row_count,
                "interval_s": job.interval_s,
                "dataset": job.dataset,
                "trade_date": job.trade_date,
                "expiration": job.expiration,
            },
        ).fetchall()
        if not result:
            raise KeyError("progress row does not exist")

    def pending_jobs(self, *, start: date, end: date, datasets: Sequence[str]) -> list[PlanJob]:
        """Return pending jobs in a session range.

        Args:
            start: First session date, inclusive.
            end: Last session date, inclusive.
            datasets: Datasets to include.

        Returns:
            Pending jobs ordered by session, expiration, then dataset.
        """
        names = _check_datasets(datasets)
        rows = (
            self._connection.execute(
                text(
                    f"""
                    SELECT dataset, "date" AS trade_date, expiration, interval_s
                    FROM {_SCHEMA}.progress
                    WHERE status = '{PENDING}'
                      AND "date" BETWEEN :start AND :end
                      AND {_dataset_in(names)}
                    ORDER BY "date", expiration, dataset
                    """
                ),
                {"start": start, "end": end},
            )
            .mappings()
            .all()
        )
        return [_job_from_row(row["dataset"], row["trade_date"], row["expiration"], row["interval_s"]) for row in rows]

    def set_updated_at(self, job: PlanJob, updated_at: datetime) -> None:
        """Set the row clock.

        Args:
            job: Existing job.
            updated_at: Timezone-aware timestamp.

        Raises:
            KeyError: The job has not been synced.
            ProgressError: ``updated_at`` is naive.
        """
        _require_aware(updated_at)
        result = self._connection.execute(
            text(
                f"""
                UPDATE {_SCHEMA}.progress
                SET updated_at = :updated_at
                WHERE dataset = :dataset
                  AND "date" = :trade_date
                  AND expiration = :expiration
                RETURNING dataset
                """
            ),
            {
                "updated_at": updated_at,
                "dataset": job.dataset,
                "trade_date": job.trade_date,
                "expiration": job.expiration,
            },
        ).fetchall()
        if not result:
            raise KeyError("progress row does not exist")

    def _load_existing(self, jobs: Sequence[PlanJob]) -> dict[tuple[str, date, date], ProgressSnapshot]:
        """Read stored rows for the jobs about to be synced.

        Args:
            jobs: Plan output. The date span and datasets come from these jobs.

        Returns:
            Snapshots keyed by ``PlanJob.key``.
        """
        start = min(job.trade_date for job in jobs)
        end = max(job.trade_date for job in jobs)
        datasets = _check_datasets([job.dataset for job in jobs])
        rows = (
            self._connection.execute(
                text(
                    f"""
                    SELECT dataset, "date" AS trade_date, expiration, status, interval_s, row_count, updated_at
                    FROM {_SCHEMA}.progress
                    WHERE "date" BETWEEN :start AND :end
                      AND {_dataset_in(datasets)}
                    """
                ),
                {"start": start, "end": end},
            )
            .mappings()
            .all()
        )
        found: dict[tuple[str, date, date], ProgressSnapshot] = {}
        for row in rows:
            key = (row["dataset"], row["trade_date"], row["expiration"])
            found[key] = ProgressSnapshot(
                status=row["status"],
                interval_s=row["interval_s"],
                row_count=row["row_count"],
                updated_at=row["updated_at"],
            )
        return found

    def _insert_pending(self, jobs: Sequence[PlanJob]) -> int:
        """Insert jobs that are not already stored.

        Args:
            jobs: Keys classified as ``insert``.

        Returns:
            Rows actually inserted. A concurrent insert loses via ``DO NOTHING``.
        """
        if not jobs:
            return 0
        statement = pending_insert_statement()
        inserted = 0
        for batch in _batches(jobs):
            rows = self._connection.execute(
                statement,
                [
                    {
                        "dataset": job.dataset,
                        "date": job.trade_date,
                        "expiration": job.expiration,
                        "status": PENDING,
                        "row_count": 0,
                        "interval_s": job.interval_s,
                    }
                    for job in batch
                ],
            ).fetchall()
            inserted += len(rows)
        return inserted

    def _requeue(self, jobs: Sequence[PlanJob]) -> int:
        """Return failed rows to pending.

        Args:
            jobs: Keys classified as ``requeue``.

        Returns:
            Rows updated. A row that is no longer failed is left alone.
        """
        return self._update_jobs(
            jobs,
            f"""
            UPDATE {_SCHEMA}.progress
            SET status = '{PENDING}',
                interval_s = :interval_s,
                row_count = 0,
                updated_at = clock_timestamp()
            WHERE dataset = :dataset
              AND "date" = :trade_date
              AND expiration = :expiration
              AND status = '{FAILED}'
            RETURNING dataset
            """,
        )

    def _touch(self, jobs: Sequence[PlanJob]) -> int:
        """Update the interval on a still-pending row.

        Args:
            jobs: Keys classified as ``touch``.

        Returns:
            Rows updated.
        """
        return self._update_jobs(
            jobs,
            f"""
            UPDATE {_SCHEMA}.progress
            SET interval_s = :interval_s,
                updated_at = clock_timestamp()
            WHERE dataset = :dataset
              AND "date" = :trade_date
              AND expiration = :expiration
              AND status = '{PENDING}'
            RETURNING dataset
            """,
        )

    def _update_jobs(self, jobs: Sequence[PlanJob], statement_sql: str) -> int:
        """Run one parameterized update for each job.

        Args:
            jobs: Jobs to update.
            statement_sql: SQL with dataset, date, expiration, and interval binds.

        Returns:
            Rows updated.
        """
        if not jobs:
            return 0
        statement = text(statement_sql)
        updated = 0
        for job in jobs:
            rows = self._connection.execute(
                statement,
                {
                    "dataset": job.dataset,
                    "trade_date": job.trade_date,
                    "expiration": job.expiration,
                    "interval_s": job.interval_s,
                },
            ).fetchall()
            updated += len(rows)
        return updated


def pending_insert_statement() -> Insert:
    """Return the insert used to queue new pending jobs.

    Returns:
        A PostgreSQL ``INSERT ... ON CONFLICT DO NOTHING RETURNING`` statement.
        A concurrent planner that already inserted the key does not overwrite it.
    """
    return (
        pg_insert(_PROGRESS)
        .on_conflict_do_nothing(index_elements=["dataset", "date", "expiration"])
        .returning(_PROGRESS.c.dataset)
    )


def _batches(jobs: Sequence[PlanJob]) -> Iterator[Sequence[PlanJob]]:
    """Yield fixed-size job batches.

    Args:
        jobs: Jobs to split.

    Yields:
        Slices of at most ``_INSERT_BATCH`` jobs.
    """
    for offset in range(0, len(jobs), _INSERT_BATCH):
        yield jobs[offset : offset + _INSERT_BATCH]


@contextmanager
def open_progress_store(database_url: str | None) -> Generator[ProgressQueue, None, None]:
    """Open a progress queue and commit it on success.

    Args:
        database_url: Postgres URL, or ``None`` for the in-memory queue.
            The URL is not logged.

    Yields:
        A queue. A Postgres queue commits when the block exits cleanly.

    Raises:
        ProgressError: The URL is not a postgresql URL. The message has no URL.
    """
    if database_url is None or not database_url.strip():
        yield MemoryProgressStore()
        return
    from curator.thetadata.migrate import MigrationError, to_sync_database_url

    try:
        url = to_sync_database_url(database_url.strip())
    except MigrationError as exc:
        raise ProgressError(str(exc)) from None
    engine = _create_engine(url)
    try:
        with engine.begin() as connection:
            yield PostgresProgressStore(connection)
    finally:
        engine.dispose()


def _create_engine(url: str) -> Engine:
    """Open an engine that does not log SQL or parameters.

    Args:
        url: Sync SQLAlchemy URL. Not stored on the progress store.

    Returns:
        An engine with echo and parameter logging off.
    """
    from sqlalchemy import create_engine

    return create_engine(url, echo=False, hide_parameters=True, pool_pre_ping=True)
