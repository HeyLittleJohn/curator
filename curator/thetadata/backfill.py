"""Run planned SPXW jobs through the worker pool.

A finished range is a no-op: no expiration list, no history requests, no new
rows. Every NYSE session in the range must already have a row for each
requested dataset. A later session that was never planned is still loaded when
an earlier session is done. Otherwise the parent lists SPXW expirations, syncs
the TD-4 queue, and claims pending jobs with ``FOR UPDATE SKIP LOCKED``.
Workers stream and upsert. The parent holds the machine lock for the whole run
so a second backfill cannot open its own request semaphore.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable, Generator, Sequence
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path

from aiomultiprocess import Pool

from curator.thetadata.calendar import trading_sessions
from curator.thetadata.client.http import ThetaDataClient
from curator.thetadata.config import Settings
from curator.thetadata.jobs import PlanJob
from curator.thetadata.limits import backfill_lock
from curator.thetadata.mapping import LoadError, PendingFact
from curator.thetadata.planner import load_spxw_expirations, parse_datasets, run_plan
from curator.thetadata.progress import (
    DONE,
    EMPTY,
    FAILED,
    STALE_RUNNING_AFTER,
    MemoryProgressStore,
    ProgressQueue,
    open_progress_store,
)
from curator.thetadata.report import (
    LoadReport,
    empty_bytes,
    read_storage,
    session_span,
)
from curator.thetadata.upsert import open_engine, write_batch
from curator.thetadata.window import DEFAULT_STRIKE_RANGE
from curator.thetadata.workers.load import JobOutcome, load_one, load_plan_job, safe_error
from curator.thetadata.workers.pool import theta_pool_kwargs

ProcessBatch = Callable[[Sequence[PlanJob]], Awaitable[Sequence[JobOutcome]]]


def _validate(
    strike_range: int | None, batch_size: int, worker_count: int | None, use_pool: bool, writer: object
) -> None:
    """Reject a load request the runner cannot execute.

    Args:
        strike_range: Window width, or ``None`` for the full chain.
        batch_size: Insert batch size.
        worker_count: Pool size, or ``None`` for the default.
        use_pool: When true, jobs run in worker processes.
        writer: Optional in-process sink. Not used with the pool.

    Returns:
        None.

    Raises:
        LoadError: A width, batch, or worker count is unusable, or the pool
            was combined with an in-process writer.
    """
    if strike_range is not None and strike_range < 0:
        raise LoadError("strike_range must be >= 0 or unlimited")
    if batch_size < 1:
        raise LoadError("batch_size must be >= 1")
    if worker_count is not None and worker_count < 1:
        raise LoadError("workers must be >= 1")
    if use_pool and writer is not None:
        raise LoadError("a custom writer cannot run inside the process pool")


def _range_finished(store: ProgressQueue, start: date, end: date, datasets: Sequence[str]) -> bool:
    """Return whether every session in the range is already done or empty.

    A range is not finished when any requested dataset is missing on an NYSE
    session inside it. An earlier finished session does not hide a later
    session that was never planned.

    Args:
        store: Progress queue.
        start: First session date, inclusive.
        end: Last session date, inclusive.
        datasets: Datasets that must each have a finished row on every session.

    Returns:
        True when every session has a row for every dataset and none of those
        rows are pending, running, or failed.
    """
    census = store.census(start=start, end=end, datasets=datasets)
    if census.pending or census.running or census.failed:
        return False
    sessions = trading_sessions(start, end)
    if not sessions:
        return False
    covered = store.covered_dates(start=start, end=end, datasets=datasets)
    for session in sessions:
        for dataset in datasets:
            if session.trade_date not in covered[dataset]:
                return False
    return True


@contextmanager
def _open_queue(database_url: str | None, queue: ProgressQueue | None) -> Generator[ProgressQueue, None, None]:
    """Yield the caller's queue, or a short postgres transaction.

    Args:
        database_url: Postgres URL. Ignored when ``queue`` is set. Not logged.
        queue: Queue reused across the run. ``None`` opens postgres.

    Yields:
        A progress queue. A postgres queue commits when the block exits.
    """
    if queue is not None:
        yield queue
        return
    with open_progress_store(database_url) as store:
        yield store


def _client_settings(api_base_url: str) -> Settings:
    """Build client settings that do not carry a database URL.

    Args:
        api_base_url: Theta Terminal v3 base URL.

    Returns:
        Settings for the parent client.
    """
    return Settings(_env_file=None, api_base_url=api_base_url, database_url=None)


def _failed(job: PlanJob, message: str) -> JobOutcome:
    """Mark a claimed job failed when the batch itself crashes.

    Args:
        job: Claimed job.
        message: Scrubbed error text.

    Returns:
        A failed outcome with no rows.
    """
    return JobOutcome(
        dataset=job.dataset,
        trade_date=job.trade_date,
        expiration=job.expiration,
        interval_s=job.interval_s,
        status=FAILED,
        row_count=0,
        requests=0,
        error=message,
    )


async def _claim_loop(
    *,
    database_url: str | None,
    queue: ProgressQueue | None,
    start: date,
    end: date,
    datasets: Sequence[str],
    limit: int,
    process: ProcessBatch,
) -> list[JobOutcome]:
    """Claim pending jobs and mark each outcome.

    Args:
        database_url: Postgres URL when ``queue`` is not an in-memory store.
        queue: Progress queue for this run.
        start: First session date, inclusive.
        end: Last session date, inclusive.
        datasets: Datasets to claim.
        limit: Jobs to claim at once.
        process: Runs one claimed batch.

    Returns:
        Outcomes in claim order.
    """
    outcomes: list[JobOutcome] = []
    while True:
        with _open_queue(database_url, queue) as store:
            claimed = store.claim(limit, start=start, end=end, datasets=datasets)
        if not claimed:
            break
        try:
            finished = list(await process(claimed))
        except Exception as exc:
            finished = [_failed(job, safe_error(exc)) for job in claimed]
        if len(finished) != len(claimed):
            finished = [_failed(job, "worker returned no outcome") for job in claimed]
        with _open_queue(database_url, queue) as store:
            for outcome in finished:
                store.mark(outcome.as_job(), outcome.status, outcome.row_count)
        outcomes.extend(finished)
    return outcomes


async def _execute_pool(
    *,
    database_url: str,
    queue: ProgressQueue | None,
    api_base_url: str,
    start: date,
    end: date,
    datasets: Sequence[str],
    strike_range: int | None,
    batch_size: int,
    worker_count: int,
) -> list[JobOutcome]:
    """Claim jobs into the aiomultiprocess pool.

    Args:
        database_url: Postgres URL passed to each worker. Not logged.
        queue: Progress queue.
        api_base_url: Terminal base URL.
        start: First session date.
        end: Last session date.
        datasets: Datasets to claim.
        strike_range: Strike window width.
        batch_size: Insert batch size.
        worker_count: Worker processes. Also the claim batch size.

    Returns:
        One outcome per claimed job.
    """
    async with Pool(**theta_pool_kwargs(worker_count)) as pool:

        async def process(batch: Sequence[PlanJob]) -> list[JobOutcome]:
            """Run a claimed batch on the pool.

            Args:
                batch: Jobs already marked running.

            Returns:
                Worker outcomes, in batch order.
            """
            payloads = [(job, api_base_url, database_url, strike_range, batch_size) for job in batch]
            return list(await pool.starmap(load_plan_job, payloads))

        return await _claim_loop(
            database_url=database_url,
            queue=queue,
            start=start,
            end=end,
            datasets=datasets,
            limit=worker_count,
            process=process,
        )


async def _execute_inline(
    client: ThetaDataClient,
    *,
    database_url: str | None,
    queue: ProgressQueue | None,
    start: date,
    end: date,
    datasets: Sequence[str],
    strike_range: int | None,
    batch_size: int,
    writer: Callable[[PlanJob, Sequence[PendingFact]], int] | None,
) -> list[JobOutcome]:
    """Claim jobs and load them on the parent's event loop.

    Args:
        client: Open parent client.
        database_url: Postgres URL, or ``None`` when ``writer`` stores the rows.
        queue: Progress queue.
        start: First session date.
        end: Last session date.
        datasets: Datasets to claim.
        strike_range: Strike window width.
        batch_size: Insert batch size.
        writer: In-process sink. ``None`` writes to postgres.

    Returns:
        One outcome per claimed job.

    Raises:
        LoadError: Postgres was requested and the URL is missing.
    """
    engine = None if writer is not None or database_url is None else open_engine(database_url)
    if writer is None and engine is None:
        raise LoadError("database URL is required")

    def persist(job: PlanJob, facts: Sequence[PendingFact]) -> int:
        """Write one batch to the test sink or to postgres.

        Args:
            job: Job the batch belongs to.
            facts: Rows to store.

        Returns:
            Rows written.
        """
        if writer is not None:
            return writer(job, facts)
        assert engine is not None
        return write_batch(engine, job, facts)

    async def process(batch: Sequence[PlanJob]) -> list[JobOutcome]:
        """Load a claimed batch on this event loop.

        Args:
            batch: Jobs already marked running.

        Returns:
            Outcomes in batch order.
        """
        return [await load_one(client, job, strike_range, batch_size, persist) for job in batch]

    try:
        return await _claim_loop(
            database_url=database_url,
            queue=queue,
            start=start,
            end=end,
            datasets=datasets,
            limit=1,
            process=process,
        )
    finally:
        if engine is not None:
            engine.dispose()


def _counts(outcomes: Sequence[JobOutcome]) -> tuple[int, int, int, int]:
    """Sum rows and status counts for this run.

    Args:
        outcomes: Jobs attempted. A finished rerun passes an empty sequence.

    Returns:
        New rows, done jobs, empty jobs, and failed jobs.
    """
    new_rows = sum(outcome.row_count for outcome in outcomes)
    done = sum(1 for outcome in outcomes if outcome.status == DONE)
    empty = sum(1 for outcome in outcomes if outcome.status == EMPTY)
    failed = sum(1 for outcome in outcomes if outcome.status == FAILED)
    return new_rows, done, empty, failed


async def run_backfill(
    *,
    start: date,
    end: date,
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
) -> LoadReport:
    """Plan a range and load the jobs that are not already finished.

    Args:
        start: First date. Dates before 2022-05-16 are clamped.
        end: Last date.
        api_base_url: Theta Terminal v3 base URL.
        database_url: Postgres URL. Required for the process pool. Not logged.
        datasets: Datasets to load. ``None`` loads every SPXW dataset.
        max_dte: Optional calendar DTE cap. ``None`` keeps every listed expiration.
        strike_range: Strikes on each side of ATM. ``None`` is the full chain.
            The default is 125. Open interest and EOD ignore it.
        worker_count: Worker processes. ``None`` uses the machine default.
        lock_path: Machine lock file. ``None`` uses ``/tmp/curator-theta.lock``.
        batch_size: Insert batch size.
        queue: Progress queue. ``None`` opens postgres, or a fresh memory queue
            when there is no database URL.
        use_pool: When true, parsing and upserts run in the aiomultiprocess pool.
        writer: In-process sink used instead of postgres. Tests pass this with
            ``use_pool`` false.

    Returns:
        Counts and byte sizes. A second call after every session in the range
        is done or empty reports zero requests and zero new rows.

    Raises:
        LoadError: The request cannot be run.
        PlanRangeError: The dates, datasets, or DTE cap are not usable.
        PlanFloorError: The range ends before 2022-05-16.
    """
    _validate(strike_range, batch_size, worker_count, use_pool, writer)
    if use_pool and not database_url:
        raise LoadError("database URL is required")
    effective_start, effective_end, sessions_loaded, sessions_full = session_span(start, end)
    selected = parse_datasets(datasets)
    if queue is None and database_url is None:
        queue = MemoryProgressStore()
    lock = lock_path or Path("/tmp/curator-theta.lock")
    async with backfill_lock(lock):
        outcomes, list_requests = await _load(
            queue=queue,
            database_url=database_url,
            api_base_url=api_base_url,
            start=start,
            end=end,
            effective_start=effective_start,
            effective_end=effective_end,
            datasets=selected,
            max_dte=max_dte,
            strike_range=strike_range,
            batch_size=batch_size,
            worker_count=worker_count,
            use_pool=use_pool,
            writer=writer,
        )
    if database_url is None:
        stored_rows: dict[tuple[str, str], int] = {}
        stored_bytes = empty_bytes()
    else:
        stored_rows, stored_bytes = read_storage(database_url, effective_start, effective_end)
    new_rows, done, empty, failed = _counts(outcomes)
    requests = list_requests + sum(outcome.requests for outcome in outcomes)
    errors = tuple(outcome.error for outcome in outcomes if outcome.error)
    return LoadReport(
        rows=stored_rows,
        relation_bytes=stored_bytes,
        sessions_loaded=sessions_loaded,
        sessions_full=sessions_full,
        requests=requests,
        new_rows=new_rows,
        jobs_done=done,
        jobs_empty=empty,
        jobs_failed=failed,
        errors=errors,
    )


async def _load(
    *,
    queue: ProgressQueue | None,
    database_url: str | None,
    api_base_url: str,
    start: date,
    end: date,
    effective_start: date,
    effective_end: date,
    datasets: tuple[str, ...],
    max_dte: int | None,
    strike_range: int | None,
    batch_size: int,
    worker_count: int | None,
    use_pool: bool,
    writer: Callable[[PlanJob, Sequence[PendingFact]], int] | None,
) -> tuple[list[JobOutcome], int]:
    """Fetch expirations only when the range still has work.

    Args:
        queue: Progress queue.
        database_url: Postgres URL, or ``None`` for an in-memory queue.
        api_base_url: Terminal base URL.
        start: Requested start, before the floor clamp is reapplied by the planner.
        end: Requested end.
        effective_start: Start after the floor clamp.
        effective_end: End date.
        datasets: Datasets to load.
        max_dte: Optional DTE cap.
        strike_range: Strike window width.
        batch_size: Insert batch size.
        worker_count: Pool size.
        use_pool: Use worker processes when true.
        writer: Optional in-process sink.

    Returns:
        Outcomes, and 1 when the expiration list was requested, otherwise 0.
    """
    stale_before = datetime.now(timezone.utc) - STALE_RUNNING_AFTER
    with _open_queue(database_url, queue) as store:
        store.reset_stale(stale_before, start=effective_start, end=effective_end, datasets=datasets)
        finished = _range_finished(store, effective_start, effective_end, datasets)
    if finished:
        return [], 0
    async with ThetaDataClient(_client_settings(api_base_url)) as client:
        expirations = await load_spxw_expirations(client)
        with _open_queue(database_url, queue) as store:
            run_plan(
                expirations,
                start,
                end,
                datasets=datasets,
                max_dte=max_dte,
                queue=store,
                materialize=False,
            )
        if not use_pool:
            outcomes = await _execute_inline(
                client,
                database_url=database_url,
                queue=queue,
                start=effective_start,
                end=effective_end,
                datasets=datasets,
                strike_range=strike_range,
                batch_size=batch_size,
                writer=writer,
            )
            return outcomes, 1
    workers = worker_count if worker_count is not None else os.cpu_count() or 1
    if database_url is None:
        raise LoadError("database URL is required")
    outcomes = await _execute_pool(
        database_url=database_url,
        queue=queue,
        api_base_url=api_base_url,
        start=effective_start,
        end=effective_end,
        datasets=datasets,
        strike_range=strike_range,
        batch_size=batch_size,
        worker_count=workers,
    )
    return outcomes, 1
