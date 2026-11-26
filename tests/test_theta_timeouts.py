"""Timeouts, retry backoff, and failure reasons for the ThetaData client.

The fakes never open a socket and never sleep.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from datetime import date
from typing import cast

import aiohttp
import pytest
from pydantic import ValidationError

from curator.thetadata.client.failure import FAILURE_REASON, classify_failure
from curator.thetadata.client.http import ThetaDataClient
from curator.thetadata.config import Settings
from curator.thetadata.exceptions import ThetaDataRequestError, ThetaDataTerminalUnreachableError
from curator.thetadata.jobs import PlanJob
from curator.thetadata.limits import reset_request_slots
from curator.thetadata.mapping import PendingFact
from curator.thetadata.nightly import LOADED, NightlyReport, format_nightly_report
from curator.thetadata.progress import FAILED
from curator.thetadata.report import LoadReport, empty_bytes, format_failure_reasons, format_load_report
from curator.thetadata.workers.load import JobOutcome, load_one

_TIMEOUT_ENV = (
    "THETADATA_API_TIMEOUT",
    "THETADATA_API_READ_TIMEOUT",
    "THETADATA_API_CONNECT_TIMEOUT",
    "THETADATA_API_MAX_RETRIES",
    "THETADATA_API_BACKOFF_BASE",
    "THETADATA_API_BACKOFF_CAP",
)


@pytest.fixture(autouse=True)
def _fresh_request_slots() -> None:
    """Drop the process-wide request semaphore before each test."""
    reset_request_slots()


def _clear_timeout_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove timeout settings inherited from the process environment.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    for name in _TIMEOUT_ENV:
        monkeypatch.delenv(name, raising=False)


class _Body:
    """Response body that returns one stored payload."""

    def __init__(self, payload: bytes) -> None:
        """Store the bytes ``read`` will return.

        Args:
            payload: Response body.
        """
        self._payload = payload

    async def read(self, n: int = -1) -> bytes:
        """Return the stored body.

        Args:
            n: Maximum number of bytes. Negative reads the whole body.

        Returns:
            The payload, truncated to ``n`` when ``n`` is non-negative.
        """
        if n >= 0:
            return self._payload[:n]
        return self._payload


class _Response:
    """Minimal aiohttp response for ``async with session.get()``."""

    def __init__(self, status: int, body: bytes = b"") -> None:
        """Store the status and body.

        Args:
            status: HTTP status code.
            body: Response body.
        """
        self.status = status
        self.headers: dict[str, str] = {}
        self.content = _Body(body)

    async def __aenter__(self) -> _Response:
        """Enter the response context.

        Returns:
            This response.
        """
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Leave the response context."""
        return None


class _Session:
    """Session whose ``get`` raises or returns the next scripted result."""

    def __init__(self, result: BaseException | _Response) -> None:
        """Store the result of every ``get``.

        Args:
            result: Exception to raise, or a response to return.
        """
        self._result = result
        self.calls = 0

    def get(self, url: str, params: object = None) -> _Response:
        """Fail or return the scripted response.

        Args:
            url: Request URL. Ignored.
            params: Query parameters. Ignored.

        Returns:
            The scripted response.

        Raises:
            BaseException: The scripted failure, including a read timeout.
        """
        del url, params
        self.calls += 1
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result

    async def close(self) -> None:
        """Close the fake session."""
        return None


class _Sleep:
    """Record retry delays without waiting."""

    def __init__(self) -> None:
        """Start with an empty delay list."""
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        """Record one delay.

        Args:
            seconds: Delay the client would have slept.
        """
        self.delays.append(seconds)


def _job() -> PlanJob:
    """Build one SPXW OHLC job.

    Returns:
        A 15-minute job for 2023-02-07.
    """
    return PlanJob(
        dataset="ohlc",
        trade_date=date(2023, 2, 7),
        expiration=date(2023, 2, 10),
        interval_s=900,
        symbol="SPXW",
        calendar_dte=3,
    )


def _persist(job: PlanJob, facts: Sequence[PendingFact]) -> int:
    """Ignore a batch. These tests fail before any row is stored.

    Args:
        job: Job the batch belongs to.
        facts: Rows that would be stored.

    Returns:
        Zero.
    """
    del job, facts
    return 0


def _client(
    session: _Session,
    sleep: _Sleep,
    *,
    api_max_retries: int,
    api_backoff_base: float = 0.5,
    api_backoff_cap: float = 8.0,
) -> ThetaDataClient:
    """Build a client that uses the fake session and the recorded sleep.

    Args:
        session: Scripted session installed in place of aiohttp.
        sleep: Delay recorder.
        api_max_retries: Retries after the first attempt.
        api_backoff_base: First retry ceiling in seconds.
        api_backoff_cap: Maximum ceiling in seconds.

    Returns:
        A client whose jitter always takes the ceiling, so the schedule is fixed.
    """
    settings = Settings(
        _env_file=None,
        api_base_url="http://127.0.0.1:9/v3",
        api_max_retries=api_max_retries,
        api_backoff_base=api_backoff_base,
        api_backoff_cap=api_backoff_cap,
        api_connect_timeout=10,
        api_read_timeout=120,
    )
    client = ThetaDataClient(settings, sleep=sleep, uniform=lambda _low, high: high)
    client._session = cast(aiohttp.ClientSession, session)
    return client


def test_timeout_settings_and_client_wiring(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connect and read timeouts are separate, and the legacy name is the read timeout."""
    _clear_timeout_env(monkeypatch)
    defaults = Settings(_env_file=None)
    assert defaults.api_connect_timeout == 10
    assert defaults.api_read_timeout == 120
    assert defaults.api_timeout == 120

    legacy = Settings(_env_file=None, api_timeout=45)
    assert legacy.api_read_timeout == 45
    assert legacy.api_connect_timeout == 10
    assert legacy.api_timeout == 45

    explicit = Settings(_env_file=None, api_timeout=45, api_read_timeout=90, api_connect_timeout=4)
    assert explicit.api_read_timeout == 90
    assert explicit.api_connect_timeout == 4
    assert explicit.api_timeout == 90

    monkeypatch.setenv("THETADATA_API_TIMEOUT", "33")
    from_legacy_env = Settings(_env_file=None)
    assert from_legacy_env.api_read_timeout == 33
    assert from_legacy_env.api_connect_timeout == 10

    monkeypatch.setenv("THETADATA_API_READ_TIMEOUT", "80")
    monkeypatch.setenv("THETADATA_API_CONNECT_TIMEOUT", "6")
    from_new_env = Settings(_env_file=None)
    assert from_new_env.api_read_timeout == 80
    assert from_new_env.api_connect_timeout == 6

    with pytest.raises(ValidationError):
        Settings(_env_file=None, api_read_timeout=0)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, api_connect_timeout=-1)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, api_timeout=0)

    async def _timeouts() -> tuple[object, object, object, object]:
        settings = Settings(_env_file=None, api_connect_timeout=4, api_read_timeout=99)
        async with ThetaDataClient(settings) as client:
            timeout = client.session.timeout
            return timeout.connect, timeout.sock_connect, timeout.sock_read, timeout.total

    connect, sock_connect, sock_read, total = asyncio.run(_timeouts())
    assert connect == 4
    assert sock_connect == 4
    assert sock_read == 99
    assert total is None


def test_read_timeout_backs_off_then_fails_with_reason_timeout(caplog: pytest.LogCaptureFixture) -> None:
    """A read timeout waits 0.5s, 1s, then 2s, and the job fails as ``timeout``."""
    caplog.set_level(logging.ERROR, logger="curator.thetadata.workers.load")
    sleep = _Sleep()
    session = _Session(aiohttp.ServerTimeoutError())
    client = _client(session, sleep, api_max_retries=3, api_backoff_base=0.5, api_backoff_cap=8.0)

    async def _run() -> JobOutcome:
        return await load_one(client, _job(), None, 1000, _persist)

    outcome = asyncio.run(_run())
    assert session.calls == 4
    assert sleep.delays == [0.5, 1.0, 2.0]
    assert outcome.status == FAILED
    assert outcome.reason == "timeout"
    assert outcome.error is not None
    assert outcome.error.startswith("timeout:")
    assert "read timeout" in outcome.error
    assert "option/history/ohlc" in outcome.error
    failed = [record.message for record in caplog.records if record.levelno >= logging.ERROR]
    assert len(failed) == 1
    message = failed[0]
    assert "root=SPXW" in message
    assert "date=2023-02-07" in message
    assert "endpoint=option/history/ohlc" in message
    assert "reason=timeout" in message


def test_http_status_is_the_failure_reason(caplog: pytest.LogCaptureFixture) -> None:
    """An exhausted HTTP 500 is reason ``http_500`` and includes the status."""
    caplog.set_level(logging.ERROR, logger="curator.thetadata.workers.load")
    sleep = _Sleep()
    session = _Session(_Response(500, b"upstream password=supersecret"))
    client = _client(session, sleep, api_max_retries=2, api_backoff_base=1.0, api_backoff_cap=8.0)

    async def _run() -> JobOutcome:
        return await load_one(client, _job(), None, 1000, _persist)

    outcome = asyncio.run(_run())
    assert session.calls == 3
    assert sleep.delays == [1.0, 2.0]
    assert outcome.status == FAILED
    assert outcome.reason == "http_500"
    assert outcome.error is not None
    assert outcome.error.startswith("http_500:")
    assert "500" in outcome.error
    assert "supersecret" not in outcome.error
    message = caplog.records[-1].message
    assert "root=SPXW" in message
    assert "date=2023-02-07" in message
    assert "endpoint=option/history/ohlc" in message
    assert "reason=http_500" in message
    assert "supersecret" not in message


def test_summary_breaks_down_failure_reasons() -> None:
    """The day summary counts timeout and HTTP reasons next to ``jobs_failed``."""
    errors = (
        "timeout: ThetaData read timeout while requesting option/history/ohlc.",
        "timeout: ThetaData read timeout while requesting option/history/quote.",
        "http_500: ThetaData request failed (500).",
        "ValueError: row could not be mapped",
    )
    assert format_failure_reasons(errors) == "ValueError:1,http_500:1,timeout:2"
    report = LoadReport(
        rows={},
        relation_bytes=empty_bytes(),
        sessions_loaded=1,
        sessions_full=1,
        requests=4,
        new_rows=0,
        jobs_done=0,
        jobs_empty=0,
        jobs_failed=4,
        errors=errors,
    )
    text = format_load_report(report)
    assert "jobs_failed=4" in text
    assert "failure_reasons=ValueError:1,http_500:1,timeout:2" in text
    assert "postgresql" not in text.lower()
    nightly = format_nightly_report(
        NightlyReport(
            action=LOADED,
            sessions_due=1,
            requests=4,
            new_rows=0,
            jobs_done=0,
            jobs_empty=0,
            jobs_failed=4,
            errors=errors,
        )
    )
    assert "jobs_failed=4" in nightly
    assert "failure_reasons=ValueError:1,http_500:1,timeout:2" in nightly


def test_client_timeout_and_http_errors_carry_reason_codes() -> None:
    """The client tags a direct timeout and a direct HTTP error before the job layer."""
    sleep = _Sleep()
    timeout_session = _Session(TimeoutError())
    timeout_client = _client(timeout_session, sleep, api_max_retries=1, api_backoff_base=0.25, api_backoff_cap=8.0)

    async def _timeout() -> None:
        async for _row in timeout_client.iter_records(
            "option/list/expirations",
            {"symbol": "SPXW"},
        ):
            return

    with pytest.raises(ThetaDataTerminalUnreachableError) as timed_out:
        asyncio.run(_timeout())
    reason, message = classify_failure(timed_out.value)
    assert reason == "timeout"
    assert getattr(timed_out.value, FAILURE_REASON) == "timeout"
    assert "option/list/expirations" in message
    assert sleep.delays == [0.25]

    status_sleep = _Sleep()
    status_session = _Session(_Response(473, b"INVALID_PARAMS"))
    status_client = _client(status_session, status_sleep, api_max_retries=3)

    async def _status() -> None:
        async for _row in status_client.iter_records(
            "option/history/ohlc",
            {"symbol": "SPXW", "expiration": "20230210", "date": "20230207", "interval": "15m"},
        ):
            return

    with pytest.raises(ThetaDataRequestError) as rejected:
        asyncio.run(_status())
    assert classify_failure(rejected.value)[0] == "http_473"
    assert status_session.calls == 1
    assert status_sleep.delays == []
