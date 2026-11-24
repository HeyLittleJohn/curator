"""Planner tests on synthetic expirations.

These tests do not open Postgres or a socket. The NYSE calendar comes from
pandas-market-calendars, which ships its holiday rules.
"""

import pickle
import re
import threading
from collections.abc import AsyncIterator
from datetime import date, datetime, timedelta, timezone

from sqlalchemy.dialects import postgresql
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata.calendar import trading_sessions
from curator.thetadata.config import BACKFILL_START, INTERVAL_1M_SECONDS, INTERVAL_15M_SECONDS
from curator.thetadata.jobs import DATASETS, PlanJob
from curator.thetadata.migrate import render_migration_sql
from curator.thetadata.models.theta import THETA_SCHEMA, ThetaBase
from curator.thetadata.planner import (
    FAR_EXPIRATION_HORIZON_DAYS,
    PlanFloorError,
    PlanRangeError,
    format_plan_counts,
    load_spxw_expirations,
    run_plan,
)
from curator.thetadata.progress import (
    DONE,
    EMPTY,
    FAILED,
    claim_sql,
    pending_insert_statement,
)
from curator.thetadata.progress import MemoryProgressStore as ProgressStore

PLACEHOLDER = date(2089, 6, 30)
ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


class _Row:
    """One synthetic expiration row."""

    def __init__(self, expiration: date) -> None:
        """Store the date.

        Args:
            expiration: Expiration date.
        """
        self.expiration = expiration


class _Expirations:
    """Fake ``option/list/expirations`` client."""

    def __init__(self, days: list[date]) -> None:
        """Store the dates the stream will yield.

        Args:
            days: Expiration dates.
        """
        self.days = days
        self.symbols: list[str] = []

    def list_expirations(self, symbol: str) -> AsyncIterator[_Row]:
        """Stream the stored dates.

        Args:
            symbol: Requested underlying.

        Returns:
            An async iterator of rows.
        """
        self.symbols.append(symbol)

        async def rows() -> AsyncIterator[_Row]:
            """Yield one row per stored date.

            Yields:
                A row with an ``expiration`` attribute.
            """
            for day in self.days:
                yield _Row(day)

        return rows()


def test_floor_clamps_and_refuses() -> None:
    """Starts before 2022-05-16 clamp. A range that ends earlier is refused."""
    store = ProgressStore()
    try:
        run_plan([date(2022, 5, 13)], date(2020, 1, 1), date(2022, 5, 15), queue=store)
    except PlanFloorError:
        pass
    else:
        raise AssertionError("range before the floor was planned")
    assert store.writes == 0

    result = run_plan(
        [date(2022, 5, 13), date(2022, 5, 16), date(2022, 5, 17)],
        date(2022, 1, 3),
        date(2022, 5, 17),
        queue=store,
    )
    assert result.counts.clamped is True
    assert result.start == BACKFILL_START
    assert {job.trade_date for job in result.jobs} == {date(2022, 5, 16), date(2022, 5, 17)}
    assert date(2022, 5, 13) not in {job.expiration for job in result.jobs}
    assert all(job.trade_date >= BACKFILL_START for job in result.jobs)


def test_holidays_are_skipped_and_half_days_stay() -> None:
    """Memorial Day and Thanksgiving are out. The day after Thanksgiving stays."""
    memorial = trading_sessions(date(2022, 5, 27), date(2022, 6, 3))
    memorial_dates = [session.trade_date for session in memorial]
    assert date(2022, 5, 30) not in memorial_dates
    assert date(2022, 5, 28) not in memorial_dates
    assert date(2022, 5, 31) in memorial_dates

    thanksgiving = trading_sessions(date(2022, 11, 23), date(2022, 11, 25))
    assert [session.trade_date for session in thanksgiving] == [date(2022, 11, 23), date(2022, 11, 25)]
    assert thanksgiving[0].half_day is False
    assert thanksgiving[1].half_day is True

    christmas = trading_sessions(date(2022, 12, 23), date(2022, 12, 27))
    assert date(2022, 12, 26) not in [session.trade_date for session in christmas]
    assert [session.trade_date for session in christmas] == [date(2022, 12, 23), date(2022, 12, 27)]

    planned = run_plan([date(2022, 11, 25)], date(2022, 11, 23), date(2022, 11, 25), datasets=["quotes"])
    assert planned.counts.sessions == 2
    assert planned.counts.half_days == 1
    assert {job.trade_date for job in planned.jobs} == {date(2022, 11, 23), date(2022, 11, 25)}


def test_default_keeps_near_expirations_and_drops_the_placeholder() -> None:
    """No max_dte cap. 2089-06-30 is a placeholder. A 2030 expiration is past the roll-in."""
    near = date(2022, 5, 20)
    far = date(2030, 12, 20)
    early = date(2012, 6, 1)
    result = run_plan(
        [near, far, early, PLACEHOLDER, near],
        date(2022, 5, 16),
        date(2022, 5, 16),
        datasets=["eod"],
    )
    expirations = {job.expiration for job in result.jobs}
    assert expirations == {near}
    assert result.counts.placeholders == 1
    assert result.counts.listed == 4
    assert result.counts.expirations == 1
    assert all(job.interval_s is None for job in result.jobs)

    capped = run_plan(
        [near, far, PLACEHOLDER],
        date(2022, 5, 16),
        date(2022, 5, 16),
        datasets=["eod"],
        max_dte=5,
    )
    assert {job.expiration for job in capped.jobs} == {near}
    assert capped.counts.placeholders == 1


def test_horizon_boundary_is_inclusive() -> None:
    """An expiration exactly on the horizon is not a placeholder. The next day is."""
    from curator.thetadata.planner import partition_expirations

    end = date(2022, 5, 16)
    edge = end + timedelta(days=FAR_EXPIRATION_HORIZON_DAYS)
    kept, dropped = partition_expirations(
        [edge, edge + timedelta(days=1)],
        plan_end=end,
        horizon_days=FAR_EXPIRATION_HORIZON_DAYS,
    )
    assert kept == (edge,)
    assert dropped == (edge + timedelta(days=1),)
    result = run_plan([edge, edge + timedelta(days=1)], end, end, datasets=["eod"])
    assert result.jobs == ()
    assert result.counts.placeholders == 1


def test_interval_switches_on_the_last_six_calendar_days() -> None:
    """One expiration is 15 minutes early and 1 minute on calendar DTE 0 through 5."""
    expiration = date(2022, 6, 3)
    result = run_plan([expiration], date(2022, 5, 16), expiration, datasets=["quotes"])
    by_date = {job.trade_date: job for job in result.jobs}
    assert date(2022, 5, 30) not in by_date
    assert by_date[date(2022, 5, 27)].calendar_dte == 7
    assert by_date[date(2022, 5, 27)].interval_s == INTERVAL_15M_SECONDS
    last_six = {expiration - timedelta(days=offset) for offset in range(6)}
    for day, job in by_date.items():
        if day in last_six:
            assert job.calendar_dte <= 5
            assert job.interval_s == INTERVAL_1M_SECONDS
        else:
            assert job.calendar_dte >= 6
            assert job.interval_s == INTERVAL_15M_SECONDS
    assert by_date[date(2022, 6, 3)].interval_s == INTERVAL_1M_SECONDS
    assert by_date[date(2022, 5, 31)].interval_s == INTERVAL_1M_SECONDS


def test_cutoff_and_both_intervals_are_configurable() -> None:
    """The DTE cutoff and the two bar lengths follow the caller."""
    expiration = date(2022, 6, 3)
    result = run_plan(
        [expiration],
        date(2022, 5, 26),
        expiration,
        datasets=["ohlc", "open_interest"],
        one_minute_max_dte=7,
        near_interval_s=10,
        far_interval_s=20,
    )
    ohlc = {job.trade_date: job for job in result.jobs if job.dataset == "ohlc"}
    assert ohlc[date(2022, 5, 26)].calendar_dte == 8
    assert ohlc[date(2022, 5, 26)].interval_s == 20
    assert ohlc[date(2022, 5, 27)].calendar_dte == 7
    assert ohlc[date(2022, 5, 27)].interval_s == 10
    daily = [job for job in result.jobs if job.dataset == "open_interest"]
    assert daily
    assert all(job.interval_s is None for job in daily)
    assert {job.expiration for job in daily} == {expiration}


def test_tuesday_expirations_are_not_filtered() -> None:
    """Listed expirations are used as given. Tuesday is not dropped."""
    tuesday = date(2022, 5, 17)
    result = run_plan([tuesday], tuesday, tuesday)
    assert {job.expiration for job in result.jobs} == {tuesday}
    assert {job.dataset for job in result.jobs} == set(DATASETS)
    assert result.counts.jobs == len(DATASETS)


def test_resume_skips_done_and_empty_and_rerun_is_a_noop() -> None:
    """A finished day writes nothing and queues nothing. Failed rows are requeued."""
    expiration = date(2022, 5, 20)
    store = ProgressStore()
    first = run_plan([expiration], date(2022, 5, 16), date(2022, 5, 16), queue=store)
    assert first.counts.writes == first.counts.jobs
    assert first.counts.pending == first.counts.jobs
    done_job = next(job for job in first.jobs if job.dataset == "quotes")
    empty_job = next(job for job in first.jobs if job.dataset == "eod")
    failed_job = next(job for job in first.jobs if job.dataset == "ohlc")
    for job in first.jobs:
        if job == failed_job:
            store.mark(job, FAILED, 4)
        elif job == empty_job:
            store.mark(job, EMPTY, 0)
        else:
            store.mark(job, DONE, 3)
    before = store.writes
    second = run_plan([expiration], date(2022, 5, 16), date(2022, 5, 16), queue=store)
    assert second.jobs == (failed_job,)
    assert second.counts.writes == 1
    assert second.counts.skipped_done == first.counts.jobs - 2
    assert second.counts.skipped_empty == 1
    assert second.counts.requeued_failed == 1
    assert second.counts.pending == 1
    assert store.writes == before + 1
    assert done_job.key() not in {job.key() for job in second.jobs}

    store.mark(failed_job, DONE, 1)
    quiet = store.writes
    third = run_plan([expiration], date(2022, 5, 16), date(2022, 5, 16), queue=store)
    assert third.jobs == ()
    assert third.counts.writes == 0
    assert third.counts.pending == 0
    assert store.writes == quiet


def test_pending_rerun_does_not_rewrite_matching_rows() -> None:
    """Planning the same pending jobs twice does not write the second time."""
    store = ProgressStore()
    first = run_plan([date(2022, 5, 16)], date(2022, 5, 16), date(2022, 5, 16), queue=store)
    before = store.writes
    second = run_plan([date(2022, 5, 16)], date(2022, 5, 16), date(2022, 5, 16), queue=store)
    assert second.counts.writes == 0
    assert second.counts.pending == first.counts.jobs
    assert store.writes == before
    assert {job.key() for job in second.jobs} == {job.key() for job in first.jobs}


def test_stale_running_jobs_reset_and_fresh_ones_stay() -> None:
    """Resume returns an old running job to pending and leaves a live one."""
    expiration = date(2022, 5, 18)
    store = ProgressStore()
    first = run_plan([expiration], date(2022, 5, 16), date(2022, 5, 17), datasets=["quotes"], queue=store)
    assert len(first.jobs) == 2
    stale = first.jobs[0]
    fresh = first.jobs[1]
    claimed = store.claim(2)
    assert {job.key() for job in claimed} == {stale.key(), fresh.key()}
    store.set_updated_at(stale, datetime.now(timezone.utc) - timedelta(hours=2))
    resumed = run_plan([expiration], date(2022, 5, 16), date(2022, 5, 17), datasets=["quotes"], queue=store)
    assert resumed.counts.reset_running == 1
    assert resumed.counts.skipped_running == 1
    assert resumed.counts.writes == 1
    assert [job.key() for job in resumed.jobs] == [stale.key()]
    assert store.claim(1)[0].key() == stale.key()
    assert store.claim(1) == []


def test_claim_is_exclusive_across_threads() -> None:
    """Four claimers take each pending job once."""
    store = ProgressStore()
    planned = run_plan(
        [date(2022, 5, 16), date(2022, 5, 18), date(2022, 5, 20)],
        date(2022, 5, 16),
        date(2022, 5, 20),
        datasets=["quotes", "ohlc", "eod"],
        queue=store,
    )
    found: list[PlanJob] = []
    guard = threading.Lock()

    def take() -> None:
        """Claim until the queue is empty.

        Returns:
            None.
        """
        local: list[PlanJob] = []
        while True:
            batch = store.claim(1)
            if not batch:
                break
            local.extend(batch)
        with guard:
            found.extend(local)

    threads = [threading.Thread(target=take) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    keys = [job.key() for job in found]
    assert len(keys) == len(set(keys)) == planned.counts.jobs
    assert store.pending_jobs(start=date(2022, 5, 16), end=date(2022, 5, 20), datasets=["quotes"]) == []


def test_plan_job_pickles() -> None:
    """A job round-trips through pickle for the worker pool."""
    result = run_plan([date(2022, 5, 20)], date(2022, 5, 16), date(2022, 5, 16), datasets=["quotes"])
    job = result.jobs[0]
    restored = pickle.loads(pickle.dumps(job))
    assert restored == job
    assert restored.symbol == "SPXW"
    assert restored.interval_s == INTERVAL_1M_SECONDS


def test_claim_sql_uses_skip_locked_and_insert_keeps_conflicts() -> None:
    """The claim statement is atomic and the insert does not overwrite a key."""
    sql = " ".join(claim_sql().lower().split())
    assert "for update skip locked" in sql
    assert "returning" in sql
    assert "status = 'pending'" in sql
    assert "status = 'running'" in sql
    assert "://" not in sql
    compiled = str(pending_insert_statement().compile(dialect=postgresql.dialect()))
    normalized = " ".join(compiled.lower().split())
    assert "on conflict (dataset, date, expiration) do nothing" in normalized
    assert "theta.progress" in normalized
    assert "://" not in compiled


def test_progress_migration_adds_interval_and_claim_index() -> None:
    """The theta chain adds interval_s and a pending-row index. The main head stays."""
    sql = render_migration_sql("upgrade", "head").lower().replace('"', "")
    assert "ix_progress_claim" in sql
    assert "where status = 'pending'" in sql
    assert "ix_progress_running" in sql
    assert "ck_progress_status" in sql
    assert "interval_s" in sql
    assert "add column error" in sql
    progress = ThetaBase.metadata.tables[f"{THETA_SCHEMA}.progress"]
    assert progress.columns["error"].nullable is True
    assert progress.columns["interval_s"].nullable is True
    assert {index.name for index in progress.indexes} >= {"ix_progress_claim", "ix_progress_running"}
    assert "34551fa734e8" not in sql


def test_counts_hide_dates() -> None:
    """The printed plan is counts. Dates and URLs are not in it."""
    result = run_plan(
        [date(2022, 5, 16), PLACEHOLDER],
        date(2022, 5, 1),
        date(2022, 5, 16),
        datasets=["quotes"],
        materialize=False,
    )
    text = format_plan_counts(result)
    assert ISO_DATE.search(text) is None
    assert "://" not in text
    assert "placeholders=1" in text
    assert "clamped=1" in text
    assert result.jobs == ()
    assert result.counts.pending == result.counts.jobs


def test_load_expirations_uses_the_list_endpoint_symbol() -> None:
    """The loader asks the client for SPXW and returns those dates."""
    import asyncio

    client = _Expirations([date(2022, 5, 16), date(2022, 5, 20)])
    found = asyncio.run(load_spxw_expirations(client))
    assert found == [date(2022, 5, 16), date(2022, 5, 20)]
    assert client.symbols == ["SPXW"]


def test_plan_cli_prints_counts_and_refuses_without_a_request() -> None:
    """``theta plan`` prints counts. A pre-floor range does not fetch expirations."""
    calls = {"count": 0}

    def fetch(api_base_url: str) -> list[date]:
        """Return a synthetic list and count the call.

        Args:
            api_base_url: Unused base URL.

        Returns:
            Two near expirations and the far-future placeholder.
        """
        del api_base_url
        calls["count"] += 1
        return [date(2022, 5, 16), date(2022, 5, 20), PLACEHOLDER]

    from curator.thetadata import planner

    original = planner.fetch_spxw_expirations
    planner.fetch_spxw_expirations = fetch
    try:
        refused = CliRunner().invoke(
            app,
            ["theta", "plan", "--start", "2022-01-01", "--end", "2022-05-13"],
            env={"THETADATA_DATABASE_URL": ""},
        )
        assert refused.exit_code == 1
        assert calls["count"] == 0
        assert "://" not in refused.stdout
        assert "://" not in (refused.stderr or "")

        planned = CliRunner().invoke(
            app,
            ["theta", "plan", "--start", "2022-05-01", "--end", "2022-05-17", "--max-dte", "5", "--dataset", "quotes"],
            env={"THETADATA_DATABASE_URL": ""},
        )
    finally:
        planner.fetch_spxw_expirations = original
    assert planned.exit_code == 0, planned.output
    assert calls["count"] == 1
    assert "sessions=2" in planned.stdout
    assert "placeholders=1" in planned.stdout
    assert "clamped=1" in planned.stdout
    assert "quotes=3" in planned.stdout
    assert "writes=3" in planned.stdout
    assert ISO_DATE.search(planned.stdout) is None
    assert "://" not in planned.stdout
    assert "2089" not in planned.stdout
    help_result = CliRunner().invoke(app, ["theta", "--help"])
    assert "plan" in help_result.stdout


def test_bad_dataset_and_inverted_range_write_nothing() -> None:
    """Unknown datasets and inverted dates fail before the queue changes."""
    store = ProgressStore()
    try:
        run_plan([date(2022, 5, 16)], date(2022, 5, 16), date(2022, 5, 16), datasets=["trades"], queue=store)
    except PlanRangeError:
        pass
    else:
        raise AssertionError("unknown dataset was accepted")
    try:
        run_plan([date(2022, 5, 16)], date(2022, 5, 18), date(2022, 5, 16), queue=store)
    except PlanRangeError:
        pass
    else:
        raise AssertionError("inverted range was accepted")
    assert store.writes == 0
