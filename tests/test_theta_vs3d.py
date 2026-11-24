"""VS3D expiration windows, one-session planning, and failure logging.

Rows and calendars are synthetic. Nothing here opens Postgres, the network,
or ThetaData.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata.backfill import _worker_count
from curator.thetadata.calendar import trading_sessions
from curator.thetadata.config import DEFAULT_BACKFILL_WORKERS
from curator.thetadata.jobs import PlanJob
from curator.thetadata.limits import reset_machine_lock, reset_request_slots
from curator.thetadata.planner import run_plan, select_spxw_expirations
from curator.thetadata.progress import (
    FAILED,
    ORDER_OLDEST,
    MemoryProgressStore,
    SyncStats,
)
from curator.thetadata.report import LoadReport, empty_bytes
from curator.thetadata.time import EASTERN

SESSION_2026 = date(2026, 8, 20)
SESSION_2022 = date(2022, 5, 17)


def _weekdays(start: date, days: int) -> list[date]:
    """Return weekday dates in a calendar span.

    Args:
        start: First calendar date.
        days: Number of calendar days, including ``start``.

    Returns:
        Monday-through-Friday dates in that span.
    """
    return [start + timedelta(days=offset) for offset in range(days) if (start + timedelta(days=offset)).weekday() < 5]


def _fridays(start: date, count: int) -> list[date]:
    """Return consecutive Fridays.

    Args:
        start: First candidate. The search moves forward to a Friday.
        count: How many Fridays to return.

    Returns:
        Friday dates.
    """
    day = start
    while day.weekday() != 4:
        day += timedelta(days=1)
    return [day + timedelta(days=7 * offset) for offset in range(count)]


def _third_friday(year: int, month: int) -> date:
    """Return the third Friday of a month.

    Args:
        year: Calendar year.
        month: Calendar month.

    Returns:
        The third Friday.
    """
    first = date(year, month, 1)
    offset = (4 - first.weekday()) % 7
    return first + timedelta(days=offset + 14)


class _SpyStore(MemoryProgressStore):
    """Memory queue that records the sessions inside each ``sync`` call."""

    def __init__(self) -> None:
        """Start an empty span log.

        Returns:
            None.
        """
        super().__init__()
        self.spans: list[set[date]] = []

    def sync(self, jobs: Iterable[PlanJob]) -> SyncStats:
        """Record which sessions this call queues, then sync them.

        Args:
            jobs: One session of plan output.

        Returns:
            The memory-store sync counts.

        Raises:
            TypeError: The call contains more than one session. That is the
                full-range materialization that crashed multi-day backfill.
        """
        batch = list(jobs)
        dates = {job.trade_date for job in batch}
        if len(dates) > 1:
            raise TypeError("'int' object is not subscriptable")
        self.spans.append(dates)
        return super().sync(batch)


def test_vs3d_windows_keep_near_weeklies_standards_and_one_roll_in() -> None:
    """A dense 2026-08-20 calendar stays near 40, and a 2022 Friday list stays small."""
    listed_2026 = _weekdays(SESSION_2026, 260)
    leap = _third_friday(2028, 12)
    listed_2026.append(leap)
    selected_2026 = select_spxw_expirations(SESSION_2026, listed_2026)
    assert len(selected_2026) == 42
    assert leap not in selected_2026
    near_tuesday = date(2026, 8, 25)
    assert near_tuesday in selected_2026
    far_tuesday = date(2026, 11, 3)
    assert (far_tuesday - SESSION_2026).days > 50
    assert far_tuesday.weekday() == 1
    assert far_tuesday not in selected_2026
    assert max((day - SESSION_2026).days for day in selected_2026) <= 220
    assert max((day - SESSION_2026).days for day in selected_2026) > 183

    fridays = _fridays(date(2022, 5, 20), 128)
    dailies = _weekdays(SESSION_2022, 21)
    listed_2022 = sorted(set(fridays + dailies))
    positive = [day for day in listed_2022 if day >= SESSION_2022]
    assert len(positive) == 140
    selected_2022 = select_spxw_expirations(SESSION_2022, listed_2022)
    assert len(selected_2022) == 28
    assert all(day >= SESSION_2022 for day in selected_2022)


def test_holiday_shifted_monthly_eom_and_roll_in() -> None:
    """June 2026's monthly is Thursday, EOM is kept, and only one roll-in is kept."""
    session = date(2026, 5, 1)
    assert not trading_sessions(date(2026, 6, 19), date(2026, 6, 19))
    monthly = date(2026, 6, 18)
    holiday = date(2026, 6, 19)
    eom = date(2026, 6, 30)
    skipped_tuesday = date(2026, 7, 7)
    july_eom = date(2026, 7, 31)
    inside = _third_friday(2026, 10)
    roll = _third_friday(2026, 11)
    later = _third_friday(2026, 12)
    listed = [monthly, holiday, eom, skipped_tuesday, july_eom, inside, roll, later]
    selected = select_spxw_expirations(session, listed)
    assert monthly in selected
    assert holiday not in selected
    assert eom in selected
    assert july_eom in selected
    assert skipped_tuesday not in selected
    assert inside in selected
    assert roll in selected
    assert later not in selected
    assert 183 < (roll - session).days <= 220
    assert (later - session).days > 220


def test_max_dte_is_an_extra_cap() -> None:
    """``max_dte`` drops selected expirations past the cap and does not add any."""
    session = date(2026, 8, 20)
    listed = _weekdays(session, 80)
    capped = select_spxw_expirations(session, listed)
    kept = tuple(day for day in capped if (day - session).days <= 5)
    result = run_plan(listed, session, session, datasets=["quotes"], max_dte=5)
    assert {job.expiration for job in result.jobs} == set(kept)
    assert all((job.expiration - session).days <= 5 for job in result.jobs)


def test_multi_day_plan_syncs_one_session_at_a_time() -> None:
    """A multi-day plan never hands two sessions to one sync call.

    The previous backfill built the whole range first. ``PostgresProgressStore.sync``
    then did ``job_list = list(jobs)`` for millions of rows and the process
    died with ``TypeError: 'int' object is not subscriptable`` outside the
    per-job handler. This store raises that same error if a sync spans sessions.
    """
    store = _SpyStore()
    start = date(2024, 1, 2)
    end = date(2024, 1, 8)
    sessions = [item.trade_date for item in trading_sessions(start, end)]
    assert len(sessions) >= 3
    listed = _weekdays(start, 30)
    result = run_plan(listed, start, end, datasets=["quotes", "eod"], queue=store, materialize=False)
    assert result.counts.sessions == len(sessions)
    assert len(store.spans) == len(sessions)
    assert all(len(span) <= 1 for span in store.spans)
    assert result.counts.jobs > 0
    assert result.jobs == ()


def test_default_workers_are_the_account_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """``None`` means 4 even when the machine reports more CPUs."""
    monkeypatch.setattr("os.cpu_count", lambda: 32)
    assert DEFAULT_BACKFILL_WORKERS == 4
    assert _worker_count(None) == 4
    assert _worker_count(8) == 8


def test_claim_default_is_newest_session() -> None:
    """The first claim is the later session, then the later expiration."""
    store = MemoryProgressStore()
    early = date(2024, 1, 2)
    late = date(2024, 1, 3)
    run_plan([date(2024, 1, 5), date(2024, 1, 19)], early, late, datasets=["eod", "quotes"], queue=store)
    first = store.claim(1)
    assert first[0].trade_date == late
    assert first[0].expiration == date(2024, 1, 19)
    oldest = store.claim(1, order=ORDER_OLDEST)
    assert oldest[0].trade_date == early


def test_failed_job_records_sanitized_error_and_success_clears_it() -> None:
    """Failure text is stored without a DSN, and a later success clears it."""
    store = MemoryProgressStore()
    expiration = date(2024, 1, 5)
    result = run_plan([expiration], date(2024, 1, 2), date(2024, 1, 2), datasets=["quotes"], queue=store)
    job = result.jobs[0]
    store.mark(job, FAILED, 0, error="TypeError postgresql://user:secret@db/curator broke")
    stored = store.failure_text(job)
    assert stored is not None
    assert "secret" not in stored
    assert "postgresql://" not in stored
    assert "TypeError" in stored
    store.mark(job, "done", 3, error="should be cleared")
    assert store.failure_text(job) is None


def test_plan_logs_a_sanitized_traceback_and_debug_prints_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A planning TypeError is logged with its traceback and without the DSN."""
    import logging

    import curator.thetadata.exceptions as errors
    from curator.thetadata import planner

    log_path = tmp_path / "curator.log"
    logger = logging.getLogger("curator.thetadata")
    logger.handlers.clear()

    def _write_here(target: logging.Logger) -> None:
        """Attach the test log file once.

        Args:
            target: Theta logger.

        Returns:
            None.
        """
        attached = any(
            isinstance(handler, logging.FileHandler) and Path(handler.baseFilename) == log_path
            for handler in target.handlers
        )
        if attached:
            return
        handler = logging.FileHandler(log_path)
        handler.setFormatter(logging.Formatter("%(message)s"))
        target.addHandler(handler)
        target.setLevel(logging.ERROR)
        target.propagate = False

    monkeypatch.setattr(errors, "_LOG_ATTACHED", False)
    monkeypatch.setattr(errors, "_ensure_log_file", _write_here)

    def fetch(api_base_url: str) -> list[date]:
        """Raise the backfill failure, with a DSN in the message.

        Args:
            api_base_url: Unused base URL.

        Returns:
            Never.

        Raises:
            TypeError: Always.
        """
        del api_base_url
        raise TypeError("int object is not subscriptable postgresql://user:secret@db/curator")

    monkeypatch.setattr(planner, "fetch_spxw_expirations", fetch)
    previous_handlers = list(logger.handlers)
    previous_propagate = logger.propagate
    try:
        quiet = CliRunner().invoke(
            app,
            ["theta", "plan", "--start", "2024-01-02", "--end", "2024-01-03"],
            env={"THETADATA_DATABASE_URL": ""},
        )
        assert quiet.exit_code == 1
        assert "int object is not subscriptable" in (quiet.stderr or "")
        assert "Traceback" not in (quiet.stderr or "")
        assert "secret" not in (quiet.stderr or "")
        logged = log_path.read_text()
        assert "Traceback" in logged
        assert "int object is not subscriptable" in logged
        assert "secret" not in logged
        assert "postgresql://" not in logged

        monkeypatch.setattr(errors, "_LOG_ATTACHED", False)
        debug = CliRunner().invoke(
            app,
            ["theta", "plan", "--start", "2024-01-02", "--end", "2024-01-03", "--debug"],
            env={"THETADATA_DATABASE_URL": ""},
        )
        assert debug.exit_code == 1
        assert "Traceback" in (debug.stderr or "")
        assert "secret" not in (debug.stderr or "")
    finally:
        for handler in list(logger.handlers):
            if handler not in previous_handlers:
                handler.close()
                logger.removeHandler(handler)
        logger.propagate = previous_propagate


def test_nightly_keeps_the_oldest_walk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Nightly still asks the backfill to walk forward."""
    from curator.thetadata import nightly as nightly_mod

    seen: dict[str, object] = {}

    async def fake_backfill(**kwargs: object) -> LoadReport:
        """Record the walk order and return an empty load.

        Args:
            kwargs: Arguments ``run_nightly`` passed.

        Returns:
            A quiet load report.
        """
        seen["order"] = kwargs["order"]
        return LoadReport(
            rows={},
            relation_bytes=empty_bytes(),
            sessions_loaded=1,
            sessions_full=1,
            requests=0,
            new_rows=0,
            jobs_done=0,
            jobs_empty=0,
            jobs_failed=0,
        )

    monkeypatch.setattr(nightly_mod, "run_backfill", fake_backfill)
    reset_machine_lock()
    reset_request_slots()
    moment = datetime(2024, 1, 4, 18, 0, tzinfo=EASTERN)

    async def _run() -> None:
        """Run one ready nightly.

        Returns:
            None.
        """
        await nightly_mod.run_nightly(
            api_base_url="http://127.0.0.1:9/v3",
            database_url=None,
            use_pool=False,
            lock_path=tmp_path / "theta.lock",
            now=lambda: moment,
            lookback=1,
        )

    asyncio.run(_run())
    assert seen["order"] == ORDER_OLDEST
