"""Scratch-Postgres proof of ``theta load-day`` against a fake terminal.

Opt in with ``THETADATA_TEST_DATABASE_URL`` and ``pytest -m postgres``.
CI deselects this marker. Rows are synthetic. The URL is never printed.
"""

from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path

import aiohttp.web as web
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata.migrate import run_migration, to_sync_database_url
from curator.thetadata.report import project_count, session_span
from curator.thetadata.secrets import scrub_secrets
from tests.test_theta_load import EMPTY, FAR, NEAR, SESSION, _chain_handler, _serve

pytestmark = pytest.mark.postgres

_QUOTES_PER_TIER = 12
_DAILY_ROWS = 44


def _require_database() -> str:
    """Return the scratch database URL or skip.

    Returns:
        The URL. Callers must not print it.

    Raises:
        pytest.skip.Exception: When the variable is unset.
    """
    url = os.environ.get("THETADATA_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("THETADATA_TEST_DATABASE_URL is not set")
    return url


def _engine() -> Engine:
    """Open an engine without logging the URL.

    Returns:
        An engine with parameters hidden.

    Raises:
        pytest.fail.Exception: If connecting fails. The message is scrubbed.
    """
    url = to_sync_database_url(_require_database())
    try:
        engine = create_engine(url, pool_pre_ping=True, echo=False, hide_parameters=True)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except Exception as exc:
        pytest.fail(scrub_secrets(str(exc)))


def _reset(engine: Engine) -> None:
    """Drop a previous theta schema on the scratch database.

    Args:
        engine: Open engine.

    Returns:
        None.
    """
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS theta CASCADE"))


class _Server:
    """Fake terminal running on its own event loop."""

    def __init__(self, hits: list[str]) -> None:
        """Remember the request log.

        Args:
            hits: Appended with each request by the handler.
        """
        self._hits = hits
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._runner: web.AppRunner | None = None
        self.base_url = ""

    def start(self) -> None:
        """Bind a localhost terminal and wait until it accepts connections.

        Returns:
            None.
        """
        loop = asyncio.new_event_loop()
        ready = threading.Event()
        error: list[BaseException] = []

        def _run() -> None:
            """Serve until ``stop`` ends the loop.

            Returns:
                None.
            """
            asyncio.set_event_loop(loop)

            async def _up() -> None:
                """Start the site.

                Returns:
                    None.
                """
                runner, base_url = await _serve(_chain_handler(self._hits))
                self._runner = runner
                self.base_url = base_url

            try:
                loop.run_until_complete(_up())
            except BaseException as exc:
                error.append(exc)
                ready.set()
                return
            ready.set()
            loop.run_forever()

        self._loop = loop
        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        ready.wait(timeout=10)
        if error:
            raise error[0]
        if not self.base_url:
            raise RuntimeError("fake terminal did not start")

    def stop(self) -> None:
        """Close the site and join its thread.

        Returns:
            None.
        """
        loop = self._loop
        thread = self._thread
        if loop is None or thread is None:
            return

        async def _down() -> None:
            """Stop the site.

            Returns:
                None.
            """
            if self._runner is not None:
                await self._runner.cleanup()

        if loop.is_running():
            asyncio.run_coroutine_threadsafe(_down(), loop).result(timeout=10)
            loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)


def _figures(text: str) -> dict[str, int]:
    """Parse ``name=integer`` lines.

    Args:
        text: Command output.

    Returns:
        Counts keyed by name.
    """
    found: dict[str, int] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        name, raw = line.split("=", 1)
        if raw.isdecimal():
            found[name] = int(raw)
    return found


def _assert_quiet(text: str) -> None:
    """Reject output that carries a URL, a password, or a price.

    Args:
        text: Combined stdout and stderr.

    Returns:
        None.
    """
    lowered = text.lower()
    assert "postgresql" not in lowered
    assert "postgres://" not in lowered
    assert "password" not in lowered
    assert "4700" not in text
    assert "." not in text


def _load_day(base_url: str, database_url: str, lock: Path) -> tuple[int, str]:
    """Run ``theta load-day`` without putting the database URL on the command line.

    Args:
        base_url: Fake terminal base URL.
        database_url: Scratch database URL. Passed only through the environment.
        lock: Machine lock path.

    Returns:
        Exit code and combined stdout and stderr.
    """
    env = os.environ.copy()
    env["THETADATA_DATABASE_URL"] = database_url
    env["THETADATA_LOCK_PATH"] = str(lock)
    result = CliRunner().invoke(
        app,
        [
            "theta",
            "load-day",
            "--date",
            "2024-01-02",
            "--strike-range",
            "1",
            "--workers",
            "2",
            "--api-url",
            base_url,
        ],
        env=env,
    )
    combined = f"{result.stdout}\n{result.stderr or ''}"
    return result.exit_code, combined


def _scalar(engine: Engine, statement: str) -> int:
    """Run a count query.

    Args:
        engine: Open engine.
        statement: SQL that returns one integer.

    Returns:
        The integer.
    """
    with engine.connect() as connection:
        value = connection.execute(text(statement)).scalar_one()
    return int(value)


def test_load_day_upserts_reports_and_reruns_clean(tmp_path: Path) -> None:
    """A fake session loads both tiers, reports bytes, and a rerun does no HTTP."""
    database_url = _require_database()
    engine = _engine()
    hits: list[str] = []
    server = _Server(hits)
    try:
        _reset(engine)
        run_migration("upgrade", "head", database_url)
        server.start()
        code, output = _load_day(server.base_url, database_url, tmp_path / "theta.lock")
        figures = _figures(output)
        assert code == 0, scrub_secrets(output)
        _assert_quiet(output)
        _start, _end, loaded, full = session_span(SESSION, SESSION)
        assert loaded == 1
        assert figures["sessions_loaded"] == loaded
        assert figures["sessions_from_floor"] == full
        assert figures["rows_quotes_1m"] == _QUOTES_PER_TIER
        assert figures["rows_quotes_15m"] == _QUOTES_PER_TIER
        assert figures["rows_ohlc_1m"] == _QUOTES_PER_TIER
        assert figures["rows_ohlc_15m"] == _QUOTES_PER_TIER
        assert figures["rows_greeks_first_order_1m"] == _QUOTES_PER_TIER
        assert figures["rows_greeks_first_order_15m"] == _QUOTES_PER_TIER
        assert figures["rows_open_interest_daily"] == _DAILY_ROWS
        assert figures["rows_eod_daily"] == _DAILY_ROWS
        assert figures["bytes_quotes"] > 0
        assert figures["bytes_contracts"] > 0
        assert figures["bytes_ohlc"] > 0
        assert figures["bytes_greeks_first_order"] > 0
        assert figures["bytes_open_interest"] > 0
        assert figures["bytes_eod"] > 0
        measured_quotes = figures["rows_quotes_1m"] + figures["rows_quotes_15m"]
        assert figures["projected_rows_quotes"] == project_count(measured_quotes, loaded, full)
        assert figures["projected_bytes_quotes"] == project_count(figures["bytes_quotes"], loaded, full)
        projected_total = sum(
            figures[f"projected_bytes_{name}"]
            for name in (
                "contracts",
                "quotes",
                "ohlc",
                "greeks_first_order",
                "open_interest",
                "eod",
            )
        )
        assert figures["projected_bytes_total"] == projected_total
        assert figures["requests"] == len(hits)
        assert figures["requests"] > 0
        assert figures["jobs_done"] == 10
        assert figures["jobs_empty"] == 5
        assert figures["jobs_failed"] == 0
        assert any(f"expiration={NEAR:%Y%m%d}" in hit and "interval=1m" in hit for hit in hits)
        assert any(f"expiration={FAR:%Y%m%d}" in hit and "interval=15m" in hit for hit in hits)
        assert any(f"expiration={EMPTY:%Y%m%d}" in hit for hit in hits)
        assert (
            _scalar(
                engine,
                "SELECT count(DISTINCT c.strike) FROM theta.quotes q JOIN theta.contracts c ON c.id = q.contract_id",
            )
            == 3
        )
        joined = "theta.quotes q JOIN theta.contracts c ON c.id = q.contract_id"
        assert _scalar(engine, f"SELECT count(*) FROM {joined} WHERE c.strike = 4700") == 0
        assert (
            _scalar(
                engine,
                "SELECT count(DISTINCT c.strike) FROM theta.eod e JOIN theta.contracts c ON c.id = e.contract_id",
            )
            == 11
        )
        assert _scalar(engine, "SELECT count(*) FROM theta.quotes WHERE interval_s = 60") == _QUOTES_PER_TIER
        assert _scalar(engine, "SELECT count(*) FROM theta.quotes WHERE interval_s = 900") == _QUOTES_PER_TIER
        assert (
            _scalar(
                engine,
                "SELECT count(*) FROM theta.quotes WHERE ts = TIMESTAMPTZ '2024-01-02 09:30:00-05'",
            )
            == _QUOTES_PER_TIER
        )
        assert _scalar(engine, "SELECT count(*) FROM theta.greeks_first_order WHERE underlying_price = 1") == (
            _QUOTES_PER_TIER * 2
        )
        assert _scalar(engine, "SELECT count(*) FROM theta.progress WHERE status = 'done'") == 10
        assert _scalar(engine, "SELECT count(*) FROM theta.progress WHERE status = 'empty'") == 5
        before = len(hits)
        again_code, again = _load_day(server.base_url, database_url, tmp_path / "theta.lock")
        _assert_quiet(again)
        again_figures = _figures(again)
        assert again_code == 0, again_figures
        assert len(hits) == before
        assert again_figures["requests"] == 0
        assert again_figures["new_rows"] == 0
        assert again_figures["rows_quotes_1m"] == _QUOTES_PER_TIER
        assert again_figures["rows_eod_daily"] == _DAILY_ROWS
        assert _scalar(engine, "SELECT count(*) FROM theta.quotes") == _QUOTES_PER_TIER * 2
    finally:
        server.stop()
        _reset(engine)
        engine.dispose()
