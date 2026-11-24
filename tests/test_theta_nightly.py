"""Readiness, catch-up, and rerun tests for the nightly SPXW load.

Rows are synthetic. Nothing here opens Postgres or leaves the machine.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import aiohttp.web as web
import pytest
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata.calendar import trading_sessions
from curator.thetadata.jobs import PlanJob
from curator.thetadata.limits import reset_machine_lock, reset_request_slots
from curator.thetadata.mapping import PendingFact
from curator.thetadata.nightly import (
    LOADED,
    READY,
    SKIP_CLOSED,
    SKIP_CLOSED_MESSAGE,
    SKIP_EARLY,
    SKIP_EARLY_MESSAGE,
    NightlyReport,
    catchup_sessions,
    evaluate_readiness,
    format_nightly_report,
    run_nightly,
)
from curator.thetadata.planner import run_plan
from curator.thetadata.progress import DONE, FAILED, RUNNING, MemoryProgressStore
from curator.thetadata.time import EASTERN

JAN2 = date(2024, 1, 2)
JAN3 = date(2024, 1, 3)
JAN4 = date(2024, 1, 4)
EXP = date(2024, 1, 5)
DATASETS = ("quotes",)
Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    """Drop ANSI color codes from captured CLI text.

    Args:
        text: Stdout or stderr from ``CliRunner``.

    Returns:
        The same text without escape sequences. Typer's highlighter can split
        ``--wait`` when GitHub Actions forces a color terminal.
    """
    return _ANSI.sub("", text)


@pytest.fixture(autouse=True)
def _reset_caps() -> Iterator[None]:
    """Give each test a fresh request semaphore and an unlocked machine lock."""
    reset_request_slots()
    reset_machine_lock()
    yield
    reset_machine_lock()
    reset_request_slots()


class Clock:
    """Mutable clock whose sleep moves the instant forward.

    Args:
        moment: Starting timezone-aware instant.
    """

    def __init__(self, moment: datetime) -> None:
        """Store the instant.

        Args:
            moment: Starting timezone-aware instant.
        """
        self.moment = moment
        self.sleeps: list[float] = []

    def __call__(self) -> datetime:
        """Return the current instant.

        Returns:
            The stored datetime.
        """
        return self.moment

    async def sleep(self, seconds: float) -> None:
        """Advance the clock by ``seconds`` without waiting.

        Args:
            seconds: Delay the production sleeper would use.

        Returns:
            None.
        """
        self.sleeps.append(seconds)
        self.moment = self.moment + timedelta(seconds=seconds)


def _ready(day: date, hour: int = 18) -> datetime:
    """Return an Eastern instant on a calendar date.

    Args:
        day: Calendar date.
        hour: Hour in America/New_York.

    Returns:
        A timezone-aware datetime.
    """
    return datetime(day.year, day.month, day.day, hour, tzinfo=EASTERN)


def _chain(hits: list[str], *, fail: bool) -> Handler:
    """Return a one-expiration quote terminal.

    Args:
        hits: Appended with ``path?query`` for each GET.
        fail: When true, quote history returns HTTP 473.

    Returns:
        The aiohttp handler.
    """

    async def handler(request: web.Request) -> web.Response:
        hits.append(f"{request.path}?{request.query_string}")
        path = request.path
        if path.endswith("/option/list/expirations"):
            return web.Response(text=f"symbol,expiration\nSPXW,{EXP:%Y%m%d}\n", content_type="text/csv")
        if path.endswith("/option/list/strikes"):
            return web.Response(text="symbol,strike\nSPXW,5000\n", content_type="text/csv")
        if path.endswith("/option/history/quote"):
            if fail:
                return web.Response(status=473, text="INVALID_PARAMS")
            session = request.query.get("date", "20240102")
            stamp = f"{session[0:4]}-{session[4:6]}-{session[6:8]}T09:30:00.000"
            text = (
                "symbol,expiration,strike,right,timestamp,bid,ask,bid_size,ask_size\n"
                f"SPXW,{EXP:%Y%m%d},5000,CALL,{stamp},1.00,1.20,1,1\n"
                f"SPXW,{EXP:%Y%m%d},5000,PUT,{stamp},1.00,1.20,1,1\n"
            )
            return web.Response(text=text, content_type="text/csv")
        return web.Response(status=404, text="NO_IMPL")

    return handler


async def _serve(handler: Handler) -> tuple[web.AppRunner, str]:
    """Start a localhost terminal fake.

    Args:
        handler: Response callback.

    Returns:
        The runner and a base URL ending in ``/v3``.
    """
    app_ = web.Application()
    app_.router.add_get("/{tail:.*}", handler)
    runner = web.AppRunner(app_)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    return runner, f"http://127.0.0.1:{port}/v3"


def _history_dates(hits: Sequence[str]) -> set[str]:
    """Collect session dates from quote history requests.

    Args:
        hits: Recorded ``path?query`` lines.

    Returns:
        Eight-digit dates.
    """
    found: set[str] = set()
    for hit in hits:
        if "/option/history/quote" not in hit or "date=" not in hit:
            continue
        found.add(hit.split("date=", 1)[1][:8])
    return found


def _mark(store: MemoryProgressStore, day: date, status: str) -> None:
    """Set every pending quotes job on one session.

    Args:
        store: Queue that already holds the planned jobs.
        day: Session date.
        status: Progress status to store.

    Returns:
        None.
    """
    jobs = store.pending_jobs(start=day, end=day, datasets=DATASETS)
    assert jobs
    for job in jobs:
        store.mark(job, status, 1 if status == DONE else 0)


def _plan(store: MemoryProgressStore, day: date) -> None:
    """Queue quotes for one session and one synthetic expiration.

    Args:
        store: Progress queue.
        day: Session date.

    Returns:
        None.
    """
    run_plan([EXP], day, day, datasets=list(DATASETS), max_dte=5, queue=store, materialize=False)


async def _run(
    *,
    hits: list[str],
    captured: list[tuple[PlanJob, PendingFact]],
    queue: MemoryProgressStore,
    lock: Path,
    moment: datetime,
    lookback: int,
    wait: bool,
    fail: bool,
    clock: Clock | None = None,
) -> NightlyReport:
    """Run one nightly load against the fake terminal.

    Args:
        hits: Request log.
        captured: Writer output.
        queue: Progress queue reused across reruns.
        lock: Machine lock path.
        moment: Clock instant used when ``clock`` is omitted.
        lookback: Session window.
        wait: Whether to sleep until 17:15.
        fail: Whether quote history returns HTTP 473.
        clock: Optional clock that records sleeps.

    Returns:
        The nightly report.
    """
    runner, base_url = await _serve(_chain(hits, fail=fail))
    source = clock or Clock(moment)
    try:

        def writer(job: PlanJob, facts: Sequence[PendingFact]) -> int:
            """Record facts and return how many were accepted.

            Args:
                job: Job being written.
                facts: Mapped rows.

            Returns:
                The batch size.
            """
            captured.extend((job, fact) for fact in facts)
            return len(facts)

        return await run_nightly(
            api_base_url=base_url,
            database_url=None,
            datasets=list(DATASETS),
            max_dte=5,
            strike_range=1,
            lock_path=lock,
            queue=queue,
            use_pool=False,
            writer=writer,
            lookback=lookback,
            wait=wait,
            now=source,
            sleep=source.sleep,
        )
    finally:
        await runner.cleanup()


def test_readiness_is_dst_aware_and_skips_closed_days() -> None:
    """17:15 ET is the gate in January and July, and holidays and weekends stay closed."""
    assert evaluate_readiness(datetime(2024, 1, 2, 22, 14, tzinfo=timezone.utc)).action == SKIP_EARLY
    assert evaluate_readiness(datetime(2024, 1, 2, 22, 15, tzinfo=timezone.utc)).action == READY
    assert evaluate_readiness(datetime(2024, 7, 1, 21, 14, tzinfo=timezone.utc)).action == SKIP_EARLY
    assert evaluate_readiness(datetime(2024, 7, 1, 21, 15, tzinfo=timezone.utc)).action == READY
    assert evaluate_readiness(datetime(2024, 7, 1, 22, 14, tzinfo=timezone.utc)).action == READY
    assert evaluate_readiness(datetime(2024, 1, 2, 17, 14, 59, tzinfo=EASTERN)).action == SKIP_EARLY
    assert evaluate_readiness(datetime(2024, 1, 2, 17, 15, tzinfo=EASTERN)).action == READY
    half = trading_sessions(date(2024, 7, 3), date(2024, 7, 3))
    assert half and half[0].half_day
    assert evaluate_readiness(datetime(2024, 7, 3, 17, 14, tzinfo=EASTERN)).action == SKIP_EARLY
    assert evaluate_readiness(datetime(2024, 7, 3, 17, 15, tzinfo=EASTERN)).action == READY
    assert evaluate_readiness(datetime(2024, 1, 6, 22, 30, tzinfo=timezone.utc)).action == SKIP_CLOSED
    assert evaluate_readiness(datetime(2024, 1, 7, 18, 0, tzinfo=EASTERN)).action == SKIP_CLOSED
    assert evaluate_readiness(datetime(2024, 11, 28, 22, 30, tzinfo=timezone.utc)).action == SKIP_CLOSED
    assert evaluate_readiness(datetime(2024, 12, 25, 22, 30, tzinfo=timezone.utc)).action == SKIP_CLOSED
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate_readiness(datetime(2024, 1, 2, 17, 30))


def test_catchup_respects_the_floor_and_skips_holidays() -> None:
    """The lookback is NYSE sessions, clamped to 2022-05-16, and omits holidays."""
    assert catchup_sessions(date(2022, 5, 13), 10) == ()
    assert catchup_sessions(date(2022, 5, 16), 10) == (date(2022, 5, 16),)
    assert catchup_sessions(date(2022, 5, 17), 10) == (date(2022, 5, 16), date(2022, 5, 17))
    assert catchup_sessions(date(2024, 11, 29), 2) == (date(2024, 11, 27), date(2024, 11, 29))
    with pytest.raises(ValueError, match="lookback"):
        catchup_sessions(JAN4, 0)


def test_early_and_closed_runs_do_not_request(tmp_path: Path) -> None:
    """Before 17:15 the default exits, and a weekend does not wait or load."""
    queue = MemoryProgressStore()
    writes = queue.writes

    async def _run_gates() -> tuple[NightlyReport, NightlyReport, list[float]]:
        early_clock = Clock(_ready(JAN4, 17).replace(minute=14))
        early = await run_nightly(
            api_base_url="http://127.0.0.1:9/v3",
            database_url=None,
            datasets=list(DATASETS),
            queue=queue,
            use_pool=False,
            lock_path=tmp_path / "early.lock",
            lookback=3,
            wait=False,
            now=early_clock,
            sleep=early_clock.sleep,
        )
        weekend = Clock(_ready(date(2024, 1, 6), 18))
        closed = await run_nightly(
            api_base_url="http://127.0.0.1:9/v3",
            database_url=None,
            datasets=list(DATASETS),
            queue=queue,
            use_pool=False,
            lock_path=tmp_path / "closed.lock",
            lookback=3,
            wait=True,
            now=weekend,
            sleep=weekend.sleep,
        )
        return early, closed, early_clock.sleeps + weekend.sleeps

    early, closed, sleeps = asyncio.run(_run_gates())
    assert early.action == SKIP_EARLY
    assert closed.action == SKIP_CLOSED
    assert sleeps == []
    assert early.requests == 0 and closed.requests == 0
    assert early.new_rows == 0 and closed.new_rows == 0
    assert queue.writes == writes
    text = format_nightly_report(early)
    assert "skipped=1" in text
    assert "requests=0" in text
    assert "://" not in text


def test_wait_sleeps_until_1715_then_loads(tmp_path: Path) -> None:
    """``--wait`` sleeps the remaining time and then loads the session."""
    hits: list[str] = []
    clock = Clock(_ready(JAN4, 17).replace(minute=14))

    async def _once() -> NightlyReport:
        return await _run(
            hits=hits,
            captured=[],
            queue=MemoryProgressStore(),
            lock=tmp_path / "wait.lock",
            moment=clock.moment,
            lookback=1,
            wait=True,
            fail=False,
            clock=clock,
        )

    report = asyncio.run(_once())
    assert clock.sleeps == [60]
    assert report.action == LOADED
    assert report.jobs_failed == 0
    assert report.requests > 0
    assert _history_dates(hits) == {f"{JAN4:%Y%m%d}"}


def test_catchup_loads_failed_and_unplanned_days_and_rerun_is_a_noop(tmp_path: Path) -> None:
    """A done day between two gaps is not reloaded, and the next run writes nothing."""
    queue = MemoryProgressStore()
    _plan(queue, JAN3)
    _mark(queue, JAN3, DONE)
    hits: list[str] = []
    captured: list[tuple[PlanJob, PendingFact]] = []

    async def _twice() -> tuple[NightlyReport, NightlyReport, int]:
        first = await _run(
            hits=hits,
            captured=captured,
            queue=queue,
            lock=tmp_path / "catchup.lock",
            moment=_ready(JAN4),
            lookback=3,
            wait=False,
            fail=False,
        )
        writes = queue.writes
        recorded = len(hits)
        stored = len(captured)
        second = await _run(
            hits=hits,
            captured=captured,
            queue=queue,
            lock=tmp_path / "catchup.lock",
            moment=_ready(JAN4),
            lookback=3,
            wait=False,
            fail=False,
        )
        assert len(hits) == recorded
        assert len(captured) == stored
        assert queue.writes == writes
        return first, second, writes

    first, second, _writes = asyncio.run(_twice())
    assert first.action == LOADED
    assert first.sessions_due == 2
    assert first.jobs_failed == 0, first.errors
    assert first.requests == len(hits)
    assert first.new_rows == len(captured)
    assert first.new_rows > 0
    assert _history_dates(hits) == {f"{JAN2:%Y%m%d}", f"{JAN4:%Y%m%d}"}
    assert second.requests == 0
    assert second.new_rows == 0
    assert second.sessions_due == 0
    assert second.jobs_failed == 0
    text = format_nightly_report(second)
    assert "requests=0" in text
    assert "new_rows=0" in text
    assert "://" not in text
    assert "5000" not in text


def test_failed_job_is_requeued_and_a_terminal_error_exits_failed(tmp_path: Path) -> None:
    """A failed session is loaded again, and HTTP 473 marks the job failed."""
    queue = MemoryProgressStore()
    _plan(queue, JAN3)
    _mark(queue, JAN3, DONE)
    _plan(queue, JAN4)
    _mark(queue, JAN4, FAILED)
    hits: list[str] = []

    async def _recover() -> NightlyReport:
        return await _run(
            hits=hits,
            captured=[],
            queue=queue,
            lock=tmp_path / "failed.lock",
            moment=_ready(JAN4),
            lookback=2,
            wait=False,
            fail=False,
        )

    recovered = asyncio.run(_recover())
    assert recovered.jobs_failed == 0, recovered.errors
    assert recovered.sessions_due == 1
    assert _history_dates(hits) == {f"{JAN4:%Y%m%d}"}
    census = queue.census(start=JAN4, end=JAN4, datasets=list(DATASETS))
    assert census.failed == 0
    assert census.done >= 1

    broken_hits: list[str] = []

    async def _break() -> NightlyReport:
        return await _run(
            hits=broken_hits,
            captured=[],
            queue=MemoryProgressStore(),
            lock=tmp_path / "error.lock",
            moment=_ready(JAN4),
            lookback=1,
            wait=False,
            fail=True,
        )

    broken = asyncio.run(_break())
    assert broken.jobs_failed >= 1
    assert broken.requests > 0
    assert "postgresql" not in " ".join(broken.errors).lower()


def test_orphan_running_rows_are_loaded(tmp_path: Path) -> None:
    """A fresh running row is pending again because the nightly holds the lock."""
    queue = MemoryProgressStore()
    _plan(queue, JAN4)
    job = queue.pending_jobs(start=JAN4, end=JAN4, datasets=list(DATASETS))[0]
    queue.mark(job, RUNNING, 0)
    queue.set_updated_at(job, datetime.now(timezone.utc))

    async def _once() -> NightlyReport:
        return await _run(
            hits=[],
            captured=[],
            queue=queue,
            lock=tmp_path / "orphan.lock",
            moment=_ready(JAN4),
            lookback=1,
            wait=False,
            fail=False,
        )

    report = asyncio.run(_once())
    assert report.jobs_failed == 0, report.errors
    census = queue.census(start=JAN4, end=JAN4, datasets=list(DATASETS))
    assert census.running == 0
    assert census.done >= 1


def test_cli_skips_early_and_exits_nonzero_on_job_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Help documents the default, a skip exits 0, and job failures exit 1."""
    help_result = CliRunner().invoke(app, ["theta", "--help"])
    assert help_result.exit_code == 0
    assert "nightly" in _plain(help_result.stdout)
    nightly_result = CliRunner().invoke(app, ["theta", "nightly", "--help"])
    assert nightly_result.exit_code == 0
    nightly_help = _plain(nightly_result.stdout)
    assert "17:15" in nightly_help
    assert "--wait" in nightly_help
    assert "exit 0" in nightly_help

    async def skipped(**_kwargs: object) -> NightlyReport:
        """Return an early skip.

        Returns:
            A zeroed before-ready report.
        """
        return NightlyReport(SKIP_EARLY, 0, 0, 0, 0, 0, 0)

    async def failed(**_kwargs: object) -> NightlyReport:
        """Return a run whose jobs failed.

        Returns:
            A report with a non-zero failure count.
        """
        return NightlyReport(LOADED, 1, 2, 0, 0, 0, 3, ("failed",))

    monkeypatch.setattr("curator.thetadata.nightly.run_nightly", skipped)
    env = {"PATH": os.environ.get("PATH", ""), "THETADATA_LOCK_PATH": str(tmp_path / "theta.lock")}
    early = CliRunner().invoke(app, ["theta", "nightly"], env=env)
    assert early.exit_code == 0
    assert "skipped=1" in early.stdout
    assert "requests=0" in early.stdout
    assert SKIP_EARLY_MESSAGE in (early.stderr or "")
    assert "://" not in f"{early.stdout}\n{early.stderr or ''}"

    monkeypatch.setattr("curator.thetadata.nightly.run_nightly", failed)
    result = CliRunner().invoke(app, ["theta", "nightly"], env=env)
    assert result.exit_code == 1
    assert "jobs_failed=3" in result.stdout
    assert "://" not in result.stdout


def test_cli_closed_message_and_unit_files_keep_secrets_out() -> None:
    """The closed-day message is fixed, and the unit files carry no DSN."""
    assert "NYSE" in SKIP_CLOSED_MESSAGE
    root = Path(__file__).resolve().parents[1]
    service = (root / "deploy/systemd/user/curator-theta-nightly.service").read_text(encoding="utf-8")
    timer = (root / "deploy/systemd/user/curator-theta-nightly.timer").read_text(encoding="utf-8")
    runbook = (root / "docs/theta-nightly.md").read_text(encoding="utf-8")
    for text in (service, timer, runbook):
        lowered = text.lower()
        assert "postgresql" not in lowered
        assert "password" not in lowered
        assert "secret" not in lowered
    assert "EnvironmentFile=%h/curator/.env" in service
    assert "After=theta-terminal.service" in service
    assert "Wants=theta-terminal.service" in service
    assert "TimeoutStartSec=infinity" in service
    assert "://" not in service
    assert "OnCalendar=Mon..Fri *-*-* 17:30:00 America/New_York" in timer
    assert "Persistent=true" in timer
    assert "After=theta-terminal.service" in timer
    assert "Wants=theta-terminal.service" in timer
    assert "loginctl enable-linger" in runbook
    assert "systemctl --user enable --now theta-terminal.service" in runbook
    assert "systemctl --user enable --now curator-theta-nightly.timer" in runbook
    assert ".env" in (root / ".gitignore").read_text(encoding="utf-8")
