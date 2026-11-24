"""Session order for the SPXW backfill.

Synthetic expirations and an in-memory queue. Nothing here opens Postgres or
a socket. The claim statement is checked as text. A scratch-Postgres check of
the same order lives in ``test_theta_progress_postgres.py``.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator, Sequence
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata import backfill as backfill_mod
from curator.thetadata.jobs import PlanJob
from curator.thetadata.limits import reset_machine_lock, reset_request_slots
from curator.thetadata.mapping import PendingFact
from curator.thetadata.nightly import run_nightly
from curator.thetadata.planner import run_plan
from curator.thetadata.progress import (
    DONE,
    ORDER_NEWEST,
    ORDER_OLDEST,
    ProgressError,
    claim_sql,
)
from curator.thetadata.progress import MemoryProgressStore as ProgressStore
from curator.thetadata.report import LoadReport
from curator.thetadata.workers.load import JobOutcome

JAN2 = date(2024, 1, 2)
JAN3 = date(2024, 1, 3)
JAN4 = date(2024, 1, 4)
NEAR = date(2024, 1, 5)
FAR = date(2024, 1, 19)
QUOTES = ("quotes",)
Key = tuple[date, date, str]


@pytest.fixture(autouse=True)
def _reset_caps() -> Iterator[None]:
    """Give each test a fresh request semaphore and an unlocked machine lock."""
    reset_request_slots()
    reset_machine_lock()
    yield
    reset_machine_lock()
    reset_request_slots()


def _keys(jobs: Sequence[PlanJob]) -> list[Key]:
    """Return claim identities in list order.

    Args:
        jobs: Claimed or planned jobs.

    Returns:
        ``(trade_date, expiration, dataset)`` for each job.
    """
    return [(job.trade_date, job.expiration, job.dataset) for job in jobs]


def _queue_two_expirations() -> ProgressStore:
    """Queue quotes and EOD for three May 2022 sessions and two expirations.

    Returns:
        A store whose pending jobs cover 2022-05-16 through 2022-05-18.
    """
    store = ProgressStore()
    run_plan(
        [date(2022, 5, 20), date(2022, 6, 17)],
        date(2022, 5, 16),
        date(2022, 5, 18),
        datasets=["quotes", "eod"],
        queue=store,
        materialize=False,
    )
    return store


def _quiet_report() -> LoadReport:
    """Return a load report with no rows and no failures.

    Returns:
        A report the CLI can print.
    """
    return LoadReport(
        rows={},
        relation_bytes={},
        sessions_loaded=1,
        sessions_full=1,
        requests=0,
        new_rows=0,
        jobs_done=0,
        jobs_empty=0,
        jobs_failed=0,
    )


def _writer(job: PlanJob, facts: Sequence[PendingFact]) -> int:
    """Ignore persisted rows.

    Args:
        job: Job the batch belongs to.
        facts: Mapped rows. Unused because the loader is replaced.

    Returns:
        Zero.
    """
    del job, facts
    return 0


class _Client:
    """Stand-in terminal client. It does not open a socket."""

    def __init__(self, settings: object) -> None:
        """Accept the settings object and drop it.

        Args:
            settings: Client settings. Not stored.
        """
        del settings

    async def __aenter__(self) -> _Client:
        """Enter the context.

        Returns:
            This client.
        """
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Leave the context.

        Args:
            exc_type: Exception type, if any.
            exc: Exception, if any.
            tb: Traceback, if any.

        Returns:
            None.
        """
        del exc_type, exc, tb


def test_claim_is_newest_first_and_oldest_reverses_it() -> None:
    """The default claim takes the newest session, and oldest restores ascending order."""
    store = _queue_two_expirations()
    newest_day = date(2022, 5, 18)
    first = store.claim(2)
    assert [job.trade_date for job in first] == [newest_day, newest_day]
    rest = store.claim(20)
    claimed = first + rest
    keys = _keys(claimed)
    assert len(keys) == 12
    assert keys == sorted(keys, reverse=True)
    assert keys[0] == (newest_day, date(2022, 6, 17), "quotes")
    assert keys[-1][0] == date(2022, 5, 16)
    for job in claimed:
        store.mark(job, "pending", 0)
    oldest = store.claim(20, order=ORDER_OLDEST)
    old_keys = _keys(oldest)
    assert old_keys == sorted(old_keys)
    assert old_keys == list(reversed(keys))
    sql = " ".join(claim_sql().lower().split())
    assert 'order by "date" desc, expiration desc, dataset desc' in sql
    oldest_sql = " ".join(claim_sql(order=ORDER_OLDEST).lower().split())
    assert 'order by "date", expiration, dataset' in oldest_sql
    assert "desc" not in oldest_sql
    with pytest.raises(ProgressError) as caught:
        claim_sql(order="date; select")
    assert "select" not in str(caught.value).lower()
    assert "://" not in sql


def _install(
    monkeypatch: pytest.MonkeyPatch,
    seen: list[PlanJob],
    queue: ProgressStore,
    *,
    watch: tuple[date, date] | None,
) -> None:
    """Replace the terminal and the downloader with in-process fakes.

    Args:
        monkeypatch: Pytest patcher.
        seen: Jobs passed to the downloader, in call order.
        queue: Progress queue the backfill is draining.
        watch: When set, the first download asserts this inclusive session span
            has no progress rows yet.
    """

    async def _expirations(client: object) -> list[date]:
        """Return the two synthetic expirations.

        Args:
            client: Unused stand-in client.

        Returns:
            The near and far expirations.
        """
        del client
        return [NEAR, FAR]

    async def _finish(
        client: object,
        job: PlanJob,
        strike_range: int | None,
        batch_size: int,
        persist: object,
    ) -> JobOutcome:
        """Record one claimed job and mark it done without a request.

        Args:
            client: Unused stand-in client.
            job: Claimed job.
            strike_range: Unused window.
            batch_size: Unused batch size.
            persist: Unused writer.

        Returns:
            A done outcome with one row.
        """
        del client, strike_range, batch_size, persist
        if watch is not None and not seen:
            census = queue.census(start=watch[0], end=watch[1], datasets=QUOTES)
            assert (census.pending, census.running, census.done, census.failed, census.empty) == (0, 0, 0, 0, 0)
        seen.append(job)
        return JobOutcome(
            dataset=job.dataset,
            trade_date=job.trade_date,
            expiration=job.expiration,
            interval_s=job.interval_s,
            status=DONE,
            row_count=1,
            requests=1,
        )

    monkeypatch.setattr(backfill_mod, "ThetaDataClient", _Client)
    monkeypatch.setattr(backfill_mod, "load_spxw_expirations", _expirations)
    monkeypatch.setattr(backfill_mod, "load_one", _finish)


def _load(queue: ProgressStore, lock: Path, order: str) -> LoadReport:
    """Run one in-process backfill.

    Args:
        queue: Progress queue.
        lock: Machine lock path.
        order: ``newest`` or ``oldest``.

    Returns:
        The load report.
    """
    return asyncio.run(
        backfill_mod.run_backfill(
            start=JAN2,
            end=JAN4,
            api_base_url="http://127.0.0.1:9/v3",
            database_url=None,
            datasets=list(QUOTES),
            lock_path=lock,
            queue=queue,
            use_pool=False,
            writer=_writer,
            order=order,
        )
    )


def test_backfill_drains_the_newest_session_before_planning_an_older_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fresh run loads 2024-01-04 before it queues 2024-01-02."""
    seen: list[PlanJob] = []
    queue = ProgressStore()
    _install(monkeypatch, seen, queue, watch=(JAN2, JAN3))
    report = _load(queue, tmp_path / "newest.lock", ORDER_NEWEST)
    assert report.jobs_failed == 0
    assert _keys(seen) == [
        (JAN4, FAR, "quotes"),
        (JAN4, NEAR, "quotes"),
        (JAN3, FAR, "quotes"),
        (JAN3, NEAR, "quotes"),
        (JAN2, FAR, "quotes"),
        (JAN2, NEAR, "quotes"),
    ]


def test_order_oldest_walks_forward(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``--order oldest`` plans 2024-01-02 before it queues 2024-01-04."""
    seen: list[PlanJob] = []
    queue = ProgressStore()
    _install(monkeypatch, seen, queue, watch=(JAN3, JAN4))
    report = _load(queue, tmp_path / "oldest.lock", ORDER_OLDEST)
    assert report.jobs_failed == 0
    assert _keys(seen) == [
        (JAN2, NEAR, "quotes"),
        (JAN2, FAR, "quotes"),
        (JAN3, NEAR, "quotes"),
        (JAN3, FAR, "quotes"),
        (JAN4, NEAR, "quotes"),
        (JAN4, FAR, "quotes"),
    ]


def _seed_interrupted(queue: ProgressStore) -> None:
    """Queue three sessions and finish the later expiration on the newest one.

    Args:
        queue: Empty progress queue.

    Returns:
        None.
    """
    run_plan([NEAR, FAR], JAN2, JAN4, datasets=list(QUOTES), queue=queue, materialize=False)
    for job in queue.pending_jobs(start=JAN4, end=JAN4, datasets=QUOTES):
        if job.expiration == FAR:
            queue.mark(job, DONE, 1)


def test_resume_starts_at_the_newest_unfinished_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An interrupted run claims the remaining newest-session job before any older session."""
    seen: list[PlanJob] = []
    queue = ProgressStore()
    _seed_interrupted(queue)
    _install(monkeypatch, seen, queue, watch=None)
    report = _load(queue, tmp_path / "resume.lock", ORDER_NEWEST)
    assert report.jobs_failed == 0
    assert _keys(seen) == [
        (JAN4, NEAR, "quotes"),
        (JAN3, FAR, "quotes"),
        (JAN3, NEAR, "quotes"),
        (JAN2, FAR, "quotes"),
        (JAN2, NEAR, "quotes"),
    ]
    assert seen[0].trade_date == JAN4
    assert all(job.trade_date == JAN3 for job in seen[1:3])


def test_resume_oldest_starts_at_the_earliest_unfinished_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``oldest`` after the same interruption walks forward from 2024-01-02."""
    seen: list[PlanJob] = []
    queue = ProgressStore()
    _seed_interrupted(queue)
    _install(monkeypatch, seen, queue, watch=None)
    report = _load(queue, tmp_path / "resume-oldest.lock", ORDER_OLDEST)
    assert report.jobs_failed == 0
    assert _keys(seen) == [
        (JAN2, NEAR, "quotes"),
        (JAN2, FAR, "quotes"),
        (JAN3, NEAR, "quotes"),
        (JAN3, FAR, "quotes"),
        (JAN4, NEAR, "quotes"),
    ]


def test_backfill_cli_order_defaults_to_newest_and_oldest_is_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The backfill flag defaults to newest and accepts oldest."""
    captured: list[str] = []

    async def _fake(**kwargs: object) -> LoadReport:
        """Record the order and return an empty report.

        Args:
            kwargs: Arguments from the CLI.

        Returns:
            A report with no failures.
        """
        order = kwargs["order"]
        assert isinstance(order, str)
        captured.append(order)
        return _quiet_report()

    monkeypatch.setattr(backfill_mod, "run_backfill", _fake)
    env = {
        "PATH": os.environ.get("PATH", ""),
        "THETADATA_DATABASE_URL": "",
        "THETADATA_LOCK_PATH": str(tmp_path / "cli.lock"),
    }
    default = CliRunner().invoke(app, ["theta", "backfill", "--start", "2024-01-02", "--end", "2024-01-04"], env=env)
    assert default.exit_code == 0, default.output
    explicit = CliRunner().invoke(
        app,
        ["theta", "backfill", "--start", "2024-01-02", "--end", "2024-01-04", "--order", "oldest"],
        env=env,
    )
    assert explicit.exit_code == 0, explicit.output
    assert captured == [ORDER_NEWEST, ORDER_OLDEST]
    refused = CliRunner().invoke(
        app,
        ["theta", "backfill", "--start", "2024-01-02", "--end", "2024-01-04", "--order", "sideways"],
        env=env,
    )
    assert refused.exit_code == 1
    assert "newest" in (refused.stderr or "")
    assert "sideways" not in (refused.stderr or "")
    assert captured == [ORDER_NEWEST, ORDER_OLDEST]
    help_result = CliRunner().invoke(app, ["theta", "backfill", "--help"])
    assert "--order" in help_result.stdout
    assert "newest" in help_result.stdout


def test_nightly_keeps_the_oldest_walk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Nightly still asks the backfill to walk the lookback oldest first."""
    captured: list[str] = []

    async def _fake(**kwargs: object) -> LoadReport:
        """Record the order and return an empty report.

        Args:
            kwargs: Arguments from nightly.

        Returns:
            A report with no failures.
        """
        order = kwargs["order"]
        assert isinstance(order, str)
        captured.append(order)
        return _quiet_report()

    monkeypatch.setattr("curator.thetadata.nightly.run_backfill", _fake)

    async def _once() -> None:
        """Run one ready nightly session.

        Returns:
            None.
        """
        await run_nightly(
            api_base_url="http://127.0.0.1:9/v3",
            database_url=None,
            datasets=list(QUOTES),
            queue=ProgressStore(),
            use_pool=False,
            lock_path=tmp_path / "nightly.lock",
            lookback=1,
            now=lambda: datetime(2024, 1, 4, 22, 30, tzinfo=timezone.utc),
        )

    asyncio.run(_once())
    assert captured == [ORDER_OLDEST]
