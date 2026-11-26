"""One plan job inside a worker process.

The parent holds the machine lock and passes the shared request semaphore
through the pool initializer. This module opens its own database engine, streams
the terminal response, and upserts batches. A no-data response is ``empty``.
Any other failure is ``failed`` and the message is scrubbed of URLs.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date

from curator.thetadata.client.failure import classify_failure, failure_endpoint, outcome_error
from curator.thetadata.client.http import ThetaDataClient
from curator.thetadata.config import Settings
from curator.thetadata.exceptions import ThetaDataNoDataError, sanitize_text
from curator.thetadata.identity import calendar_dte
from curator.thetadata.jobs import SYMBOL_SPXW, PlanJob
from curator.thetadata.mapping import PendingFact
from curator.thetadata.progress import EMPTY, FAILED
from curator.thetadata.secrets import scrub_secrets
from curator.thetadata.stream_job import RequestMeter, stream_fact_batches
from curator.thetadata.upsert import open_engine, write_batch

logger = logging.getLogger(__name__)

Persist = Callable[[PlanJob, Sequence[PendingFact]], int]


@dataclass(frozen=True)
class JobOutcome:
    """What one worker did with one job. Counts only.

    Args:
        dataset: Dataset name.
        trade_date: Session date.
        expiration: Expiration date.
        interval_s: Bar length copied from the job.
        status: ``done``, ``empty``, or ``failed``.
        row_count: Rows written. Empty and a failed job that wrote nothing are 0.
        requests: Logical terminal requests for this job.
        error: Scrubbed failure text, prefixed with the reason code. Not printed
            by the CLI except as a counted reason.
        reason: ``timeout``, ``http_<status>``, or an exception class name.
            ``None`` when the job did not fail.
    """

    dataset: str
    trade_date: date
    expiration: date
    interval_s: int | None
    status: str
    row_count: int
    requests: int
    error: str | None = None
    reason: str | None = None

    def as_job(self) -> PlanJob:
        """Rebuild the plan job this outcome belongs to.

        Returns:
            A job the progress table can mark.
        """
        return PlanJob(
            dataset=self.dataset,
            trade_date=self.trade_date,
            expiration=self.expiration,
            interval_s=self.interval_s,
            symbol=SYMBOL_SPXW,
            calendar_dte=calendar_dte(self.trade_date, self.expiration),
        )


def safe_error(exc: BaseException) -> str:
    """Return a one-line failure message without a URL or password.

    Args:
        exc: Exception caught while loading a job.

    Returns:
        Sanitized text.
    """
    return sanitize_text(scrub_secrets(f"{type(exc).__name__}: {exc}"))


def _outcome(
    job: PlanJob,
    status: str,
    row_count: int,
    requests: int,
    error: str | None,
    reason: str | None = None,
) -> JobOutcome:
    """Build an outcome from the job identity.

    Args:
        job: Job that was attempted.
        status: Progress status.
        row_count: Rows written.
        requests: Logical terminal requests.
        error: Scrubbed failure text, or ``None``.
        reason: Failure reason code, or ``None``.

    Returns:
        The outcome.
    """
    return JobOutcome(
        dataset=job.dataset,
        trade_date=job.trade_date,
        expiration=job.expiration,
        interval_s=job.interval_s,
        status=status,
        row_count=row_count,
        requests=requests,
        error=error,
        reason=reason,
    )


def _fail(job: PlanJob, row_count: int, requests: int, exc: BaseException) -> JobOutcome:
    """Log a failed job and return its outcome.

    ``theta.progress`` has no error column, so the reason is logged with the
    job identity and stored only on the outcome.

    Args:
        job: Job that failed.
        row_count: Rows written before the failure.
        requests: Logical terminal requests.
        exc: Exception from the attempt.

    Returns:
        A failed outcome whose ``reason`` is ``timeout``, ``http_<status>``,
        or an exception class name.
    """
    reason, message = classify_failure(exc)
    endpoint = failure_endpoint(exc, job.dataset)
    logger.error(
        "ThetaData job failed root=%s date=%s endpoint=%s reason=%s message=%s",
        job.symbol,
        job.trade_date.isoformat(),
        endpoint,
        reason,
        message,
    )
    return _outcome(job, FAILED, row_count, requests, outcome_error(reason, message), reason)


async def load_one(
    client: ThetaDataClient,
    job: PlanJob,
    strike_range: int | None,
    batch_size: int,
    persist: Persist,
) -> JobOutcome:
    """Download one job and persist its batches.

    Args:
        client: Open client. History calls use the shared request slot.
        job: Claimed plan job.
        strike_range: Strikes on each side of ATM, or ``None`` for the full chain.
        batch_size: Insert batch size.
        persist: Writes one batch and returns how many rows it stored.

    Returns:
        Done, empty, or failed. Empty covers a terminal with no rows and a
        window that kept none.
    """
    meter = RequestMeter()
    total = 0
    try:
        async for batch in stream_fact_batches(client, job, strike_range, batch_size, meter):
            total += persist(job, batch)
    except ThetaDataNoDataError:
        return _outcome(job, EMPTY, 0, meter.count, None)
    except Exception as exc:
        return _fail(job, total, meter.count, exc)
    status = EMPTY if total == 0 else "done"
    return _outcome(job, status, total, meter.count, None)


async def load_plan_job(
    job: PlanJob,
    api_base_url: str,
    database_url: str,
    strike_range: int | None,
    batch_size: int,
) -> JobOutcome:
    """Download and upsert one job in a worker process.

    Args:
        job: Claimed plan job. Pickled in by the pool.
        api_base_url: Theta Terminal v3 base URL.
        database_url: Postgres URL for this worker's own engine. Not logged.
        strike_range: Strikes on each side of ATM, or ``None`` for the full chain.
        batch_size: Insert batch size.

    Returns:
        The job outcome. The parent marks progress from it.
    """
    settings = Settings(
        _env_file=None,
        api_base_url=api_base_url,
        database_url=None,
    )
    try:
        engine = open_engine(database_url)
    except Exception as exc:
        return _fail(job, 0, 0, exc)

    def persist(one_job: PlanJob, facts: Sequence[PendingFact]) -> int:
        """Upsert one batch on this worker's engine.

        Args:
            one_job: Job the batch belongs to.
            facts: Rows to store.

        Returns:
            Rows written.
        """
        return write_batch(engine, one_job, facts)

    try:
        async with ThetaDataClient(settings) as client:
            return await load_one(client, job, strike_range, batch_size, persist)
    finally:
        engine.dispose()
