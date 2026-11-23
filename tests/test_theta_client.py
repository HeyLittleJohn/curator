"""Fake-terminal tests for the Options Standard ThetaData client.

Nothing here opens Postgres or leaves the machine. The fake terminal listens
on 127.0.0.1 only.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import aiohttp.web as web
import pytest
from aiomultiprocess import Pool
from pydantic import ValidationError

from curator.thetadata.client import (
    STANDARD_ENDPOINTS,
    ThetaDataClient,
    ThetaDataMachineLockError,
    ThetaDataNoDataError,
    ThetaDataRateLimitError,
    ThetaDataRequestError,
    ThetaDataSubscriptionError,
    ThetaDataTerminalUnreachableError,
)
from curator.thetadata.client.retry import backoff_delay
from curator.thetadata.client.stream import iter_csv_records, iter_ndjson_records, iter_text_lines
from curator.thetadata.config import OPTIONS_STANDARD_CONCURRENCY, BackfillMode, Settings
from curator.thetadata.downloaders.option import OptionGreeksDownloader, OptionTradeGreeksDownloader
from curator.thetadata.limits import backfill_lock, reset_machine_lock, reset_request_slots
from curator.thetadata.workers.pool import WorkerTask, process_task, theta_pool_kwargs
from tests.theta_load import fetch_expiration_pid, hold_backfill_lock

PRO_ONLY_ENDPOINTS = (
    "option/history/greeks/all",
    "option/history/greeks/second_order",
    "option/history/greeks/third_order",
    "option/history/trade_greeks",
    "option/history/trade_greeks/all",
    "option/history/trade_greeks/first_order",
    "option/history/trade_greeks/second_order",
    "option/history/trade_greeks/third_order",
    "option/history/trade_greeks/implied_volatility",
)

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


@pytest.fixture(autouse=True)
def _reset_caps() -> Iterator[None]:
    """Give each test a fresh request semaphore and an unlocked machine lock."""
    reset_request_slots()
    reset_machine_lock()
    yield
    reset_machine_lock()
    reset_request_slots()


def _settings(
    base_url: str,
    *,
    api_max_retries: int = 0,
    api_timeout: int = 5,
    api_rate_limit: int = OPTIONS_STANDARD_CONCURRENCY,
    database_url: str | None = None,
) -> Settings:
    """Build settings that talk only to a fake terminal.

    Args:
        base_url: Fake terminal base URL.
        api_max_retries: Retries after the first attempt.
        api_timeout: Socket timeout in seconds.
        api_rate_limit: Account cap for this client.
        database_url: Optional DSN, used only to prove it never appears in errors.

    Returns:
        Settings with backoff collapsed.
    """
    return Settings(
        _env_file=None,
        api_base_url=base_url,
        api_max_retries=api_max_retries,
        api_backoff_base=0.0,
        api_backoff_cap=0.0,
        api_timeout=api_timeout,
        api_rate_limit=api_rate_limit,
        database_url=database_url,
    )


async def _serve(handler: Handler) -> tuple[web.AppRunner, str]:
    """Start a localhost terminal fake.

    Args:
        handler: Response callback for every GET.

    Returns:
        The runner, and a base URL ending in ``/v3``.
    """
    app = web.Application()
    app.router.add_get("/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    return runner, f"http://127.0.0.1:{port}/v3"


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    """Yield the given chunks.

    Args:
        *parts: Body pieces, which may split a line.

    Yields:
        Each part unchanged.
    """
    for part in parts:
        yield part


def test_standard_endpoints_exclude_pro_routes() -> None:
    """The default endpoint set has no Pro greeks or trade-greeks route."""
    assert "option/history/greeks/first_order" in STANDARD_ENDPOINTS
    for path in STANDARD_ENDPOINTS:
        assert "trade_greeks" not in path
        assert "second_order" not in path
        assert "third_order" not in path
        assert not path.endswith("greeks/all")
    assert Settings(_env_file=None).api_rate_limit == OPTIONS_STANDARD_CONCURRENCY
    assert Settings(_env_file=None).api_base_url == "http://127.0.0.1:25503/v3"


def test_rate_limit_cannot_exceed_account_cap() -> None:
    """The cap can be lowered and cannot be raised above 4."""
    assert Settings(_env_file=None, api_rate_limit=2).api_rate_limit == 2
    with pytest.raises(ValidationError):
        Settings(_env_file=None, api_rate_limit=5)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, api_rate_limit=0)


def test_backoff_is_bounded_exponential_with_jitter() -> None:
    """Retry delays double until the cap, and jitter stays inside that ceiling."""
    assert backoff_delay(0, base=0.5, cap=8.0, uniform=lambda _low, high: high) == 0.5
    assert backoff_delay(3, base=0.5, cap=8.0, uniform=lambda _low, high: high) == 4.0
    assert backoff_delay(10, base=0.5, cap=8.0, uniform=lambda _low, high: high) == 8.0
    assert backoff_delay(3, base=0.5, cap=8.0, uniform=lambda low, _high: low) == 0.0


def test_downloader_defaults_are_standard_only() -> None:
    """Greeks default to first order, and trade greeks are refused."""
    greeks = OptionGreeksDownloader.__new__(OptionGreeksDownloader)
    trade_greeks = OptionTradeGreeksDownloader.__new__(OptionTradeGreeksDownloader)
    assert greeks.get_history_endpoint("greeks") == "option/history/greeks/first_order"
    with pytest.raises(ThetaDataSubscriptionError) as caught:
        trade_greeks.get_history_endpoint("trade_greeks")
    assert isinstance(caught.value, PermissionError)


def test_pool_refuses_trade_greeks_without_a_request() -> None:
    """The worker map does not dispatch a Pro data type."""
    task = WorkerTask(
        task_id="pro",
        asset_type="option",
        data_type="trade_greeks",
        symbol="SPXW",
        start_date=date(2024, 1, 19),
        end_date=date(2024, 1, 19),
        mode=BackfillMode.FULL,
        extra={},
    )
    result = asyncio.run(process_task(task, Settings(_env_file=None)))
    assert result.result is None
    assert result.error is not None
    assert "trade_greeks" in result.error


def test_split_chunks_parse_without_the_whole_body() -> None:
    """A line split across chunks becomes one row, and the rest is not required first."""

    async def _rows() -> list[dict[str, str]]:
        lines = iter_text_lines(_chunks(b"symbol,expi", b"ration\nSPXW,2024-", b"01-19\nSPXW,2024-01-20\n"))
        return [row async for row in iter_csv_records(lines)]

    rows = asyncio.run(_rows())
    assert rows == [
        {"symbol": "SPXW", "expiration": "2024-01-19"},
        {"symbol": "SPXW", "expiration": "2024-01-20"},
    ]


def test_ndjson_objects_parse_per_line() -> None:
    """NDJSON yields one object per line, including a line split across chunks."""

    async def _rows() -> list[dict[str, object]]:
        lines = iter_text_lines(_chunks(b'{"symbol": "SPX', b'W"}\n', b'{"symbol": "SPX"}\n'))
        return [row async for row in iter_ndjson_records(lines)]

    rows = asyncio.run(_rows())
    assert rows == [{"symbol": "SPXW"}, {"symbol": "SPX"}]


def test_pro_endpoints_raise_before_connect() -> None:
    """Pro routes raise PermissionError and do not open a socket."""

    async def _refuse() -> None:
        settings = _settings("http://127.0.0.1:9/v3")
        async with ThetaDataClient(settings) as client:
            for endpoint in PRO_ONLY_ENDPOINTS:
                with pytest.raises(ThetaDataSubscriptionError) as caught:
                    async for _row in client.iter_records(endpoint, {"symbol": "SPXW"}):
                        pass
                assert isinstance(caught.value, PermissionError)
                assert "supersecret" not in str(caught.value)
            with pytest.raises(ThetaDataSubscriptionError):
                await client.get_stock_symbols()
            with pytest.raises(ThetaDataSubscriptionError):
                await client.get_index_symbols()
            with pytest.raises(ThetaDataRequestError):
                async for _row in client.iter_quotes(
                    "SPXW",
                    "*",
                    session_date=date(2024, 1, 19),
                    interval="1s",
                ):
                    pass
            with pytest.raises(ThetaDataRequestError):
                async for _row in client.iter_ohlc(
                    "SPXW",
                    "*",
                    start_date=date(2024, 1, 2),
                    end_date=date(2024, 1, 3),
                    interval="1m",
                ):
                    pass
            with pytest.raises(ThetaDataRequestError):
                await client.get("option/history/quote", {"symbol": "SPXW"})

    asyncio.run(_refuse())


def test_terminal_down_names_the_failure_without_secrets() -> None:
    """A refused connection becomes a terminal error and drops any DSN text."""

    async def _down() -> str:
        settings = _settings(
            "http://127.0.0.1:9/v3",
            database_url="postgresql://user:supersecret@localhost:5432/thetadata",
            api_timeout=1,
        )
        async with ThetaDataClient(settings) as client:
            try:
                async for _row in client.list_expirations("SPXW"):
                    pass
            except ThetaDataTerminalUnreachableError as exc:
                return str(exc)
        raise AssertionError("expected the terminal error")

    message = asyncio.run(_down())
    assert "not running" in message
    assert "supersecret" not in message
    assert "postgresql" not in message


def test_error_statuses_map_and_only_transient_ones_retry() -> None:
    """429 retries, while permission and no-data errors do not."""

    async def _run() -> None:
        hits = {"n": 0}

        async def rate_then_ok(request: web.Request) -> web.Response:
            del request
            hits["n"] += 1
            if hits["n"] < 3:
                return web.Response(status=429, text="OS_LIMIT", headers={"Retry-After": "0"})
            return web.Response(text="symbol,expiration\nSPXW,20240119\n", content_type="text/csv")

        runner, base_url = await _serve(rate_then_ok)
        try:
            async with ThetaDataClient(_settings(base_url, api_max_retries=2)) as client:
                rows = [row async for row in client.list_expirations("SPXW")]
            assert hits["n"] == 3
            assert rows[0].expiration == date(2024, 1, 19)
        finally:
            await runner.cleanup()

        hits["n"] = 0

        async def always_limited(request: web.Request) -> web.Response:
            del request
            hits["n"] += 1
            return web.Response(status=429, text="OS_LIMIT password=supersecret")

        runner, base_url = await _serve(always_limited)
        try:
            async with ThetaDataClient(_settings(base_url, api_max_retries=1)) as client:
                with pytest.raises(ThetaDataRateLimitError) as caught:
                    async for _row in client.list_expirations("SPXW"):
                        pass
            assert hits["n"] == 2
            assert "429" in str(caught.value)
            assert "supersecret" not in str(caught.value)
        finally:
            await runner.cleanup()

        hits["n"] = 0

        async def denied(request: web.Request) -> web.Response:
            del request
            hits["n"] += 1
            return web.Response(status=471, text="PERMISSION postgresql://user:supersecret@db/x")

        runner, base_url = await _serve(denied)
        try:
            async with ThetaDataClient(_settings(base_url, api_max_retries=3)) as client:
                with pytest.raises(ThetaDataSubscriptionError) as caught:
                    async for _row in client.list_expirations("SPXW"):
                        pass
            assert hits["n"] == 1
            assert isinstance(caught.value, PermissionError)
            assert "471" in str(caught.value)
            assert "supersecret" not in str(caught.value)
            assert "postgresql://" not in str(caught.value)
        finally:
            await runner.cleanup()

        hits["n"] = 0

        async def empty(request: web.Request) -> web.Response:
            del request
            hits["n"] += 1
            return web.Response(status=472, text="NO_DATA")

        runner, base_url = await _serve(empty)
        try:
            async with ThetaDataClient(_settings(base_url, api_max_retries=3)) as client:
                with pytest.raises(ThetaDataNoDataError) as caught:
                    async for _row in client.list_strikes("SPXW", date(2024, 1, 19)):
                        pass
            assert hits["n"] == 1
            assert "SPXW" in str(caught.value)
            assert "20240119" in str(caught.value)
            assert "472" in str(caught.value)
        finally:
            await runner.cleanup()

    asyncio.run(_run())


def test_standard_routes_stream_typed_rows() -> None:
    """Each Standard history and list route yields a typed row from synthetic CSV."""
    bodies = {
        "/v3/option/list/expirations": "symbol,expiration\nSPXW,20240119\n",
        "/v3/option/list/strikes": "symbol,strike\nSPXW,5000.5\n",
        "/v3/option/list/contracts/quote": "symbol,expiration,strike,right\nSPXW,20240119,5000,call\n",
        "/v3/option/history/quote": (
            "symbol,expiration,strike,right,timestamp,bid,ask\n"
            "SPXW,20240119,5000,CALL,2024-01-19T09:30:00.000,1.00,1.10\n"
        ),
        "/v3/option/history/ohlc": (
            "symbol,expiration,strike,right,timestamp,open,high,low,close,volume\n"
            "SPXW,20240119,5000,PUT,2024-01-19T09:30:00.000,1,2,0.5,1.5,10\n"
        ),
        "/v3/option/history/open_interest": (
            "symbol,expiration,strike,right,timestamp,open_interest\nSPXW,20240119,5000,C,2024-01-19T06:30:00.000,42\n"
        ),
        "/v3/option/history/eod": (
            "symbol,expiration,strike,right,open,high,low,close,volume\nSPXW,20240119,5000,P,1,2,0.5,1.5,9\n"
        ),
        "/v3/option/history/greeks/first_order": (
            "symbol,expiration,strike,right,timestamp,delta,lambda,implied_vol,underlying_price\n"
            "SPXW,20240119,5000,CALL,2024-01-19T09:30:00.000,0.5,1.25,0.2,5100.25\n"
        ),
    }
    seen: list[str] = []

    async def dispatch(request: web.Request) -> web.Response:
        seen.append(request.path)
        body = bodies.get(request.path)
        if body is None:
            return web.Response(status=404, text="NO_IMPL")
        return web.Response(text=body, content_type="text/csv")

    async def _run() -> None:
        runner, base_url = await _serve(dispatch)
        try:
            async with ThetaDataClient(_settings(base_url)) as client:
                expirations = [row async for row in client.list_expirations("SPXW")]
                strikes = [row async for row in client.list_strikes("SPXW", date(2024, 1, 19))]
                contracts = [row async for row in client.list_contracts(date(2024, 1, 19), "SPXW")]
                quotes = [
                    row
                    async for row in client.iter_quotes(
                        "SPXW",
                        "*",
                        session_date=date(2024, 1, 19),
                        interval="1m",
                    )
                ]
                bars = [
                    row
                    async for row in client.iter_ohlc(
                        "SPXW",
                        date(2024, 1, 19),
                        session_date=date(2024, 1, 19),
                        interval="1m",
                        strike=Decimal("5000"),
                        right="put",
                    )
                ]
                interest = [
                    row
                    async for row in client.iter_open_interest(
                        "SPXW",
                        date(2024, 1, 19),
                        session_date=date(2024, 1, 19),
                    )
                ]
                eod = [
                    row
                    async for row in client.iter_eod(
                        "SPXW",
                        date(2024, 1, 19),
                        start_date=date(2024, 1, 19),
                        end_date=date(2024, 1, 19),
                    )
                ]
                greeks = [
                    row
                    async for row in client.iter_greeks(
                        "SPXW",
                        date(2024, 1, 19),
                        session_date=date(2024, 1, 19),
                        interval="5m",
                    )
                ]

                async def ndjson(request: web.Request) -> web.Response:
                    del request
                    return web.Response(
                        text='{"symbol":"SPXW","expiration":"2024-01-19"}\n',
                        content_type="application/x-ndjson",
                    )

                ndjson_runner, ndjson_url = await _serve(ndjson)
                try:
                    async with ThetaDataClient(_settings(ndjson_url)) as ndjson_client:
                        ndjson_rows = [
                            row async for row in ndjson_client.list_expirations("SPXW", response_format="ndjson")
                        ]
                finally:
                    await ndjson_runner.cleanup()
        finally:
            await runner.cleanup()

        assert expirations[0].expiration == date(2024, 1, 19)
        assert strikes[0].strike == Decimal("5000.5")
        assert contracts[0].right.value == "CALL"
        assert quotes[0].timestamp == datetime(2024, 1, 19, 9, 30)
        assert quotes[0].bid == Decimal("1.00")
        assert bars[0].volume == 10
        assert interest[0].open_interest == 42
        assert eod[0].close == Decimal("1.5")
        assert greeks[0].implied_vol == Decimal("0.2")
        assert greeks[0].underlying_price == Decimal("5100.25")
        assert greeks[0].lambda_ == Decimal("1.25")
        assert ndjson_rows[0].symbol == "SPXW"
        assert "/v3/option/history/greeks/first_order" in seen
        assert not any("trade_greeks" in path or path.endswith("greeks/all") for path in seen)

    asyncio.run(_run())


def test_first_row_yields_before_the_body_finishes() -> None:
    """Parsing does not wait for the rest of a chain."""
    finished = {"at": 0.0}

    async def slow(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(status=200, headers={"Content-Type": "text/csv"})
        await response.prepare(request)
        await response.write(
            b"symbol,expiration,strike,right,timestamp,bid,ask\n"
            b"SPXW,20240119,5000,CALL,2024-01-19T09:30:00.000,1.00,1.10\n"
        )
        await asyncio.sleep(0.6)
        await response.write(b"SPXW,20240119,5000,CALL,2024-01-19T09:31:00.000,1.20,1.30\n")
        await response.write_eof()
        finished["at"] = asyncio.get_running_loop().time()
        return response

    async def _run() -> float:
        runner, base_url = await _serve(slow)
        try:
            async with ThetaDataClient(_settings(base_url, api_timeout=5)) as client:
                first_at: float | None = None
                count = 0
                async for _row in client.iter_quotes(
                    "SPXW",
                    date(2024, 1, 19),
                    session_date=date(2024, 1, 19),
                    interval="1m",
                ):
                    count += 1
                    if first_at is None:
                        first_at = asyncio.get_running_loop().time()
        finally:
            await runner.cleanup()
        assert first_at is not None
        assert count == 2
        return finished["at"] - first_at

    gap = asyncio.run(_run())
    assert gap > 0.3


def test_machine_lock_is_reentrant_and_blocks_a_second_process(tmp_path: Path) -> None:
    """One process may re-enter the lock. A second process is refused."""

    async def _reenter() -> None:
        async with backfill_lock(tmp_path / "theta.lock"):
            async with backfill_lock(tmp_path / "theta.lock"):
                return

    asyncio.run(_reenter())

    import multiprocessing

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    process = context.Process(
        target=hold_backfill_lock,
        args=(str(tmp_path / "other.lock"), ready, release),
    )
    process.start()
    assert ready.wait(20)
    with pytest.raises(ThetaDataMachineLockError) as caught:
        asyncio.run(_once(tmp_path / "other.lock"))
    assert "postgresql" not in str(caught.value)
    assert "password" not in str(caught.value).lower()
    release.set()
    process.join(20)
    assert process.exitcode == 0


async def _once(path: Path) -> None:
    """Take the machine lock once.

    Args:
        path: Lock file.
    """
    async with backfill_lock(path):
        return


def test_in_process_fanout_stops_at_four() -> None:
    """One event loop cannot open a fifth request while four are in flight."""
    max_seen, total = asyncio.run(_fanout(concurrency=4, tasks=10))
    assert max_seen == 4
    assert total == 10


def test_cross_process_cap_is_four(tmp_path: Path) -> None:
    """Fifty tasks on six workers keep the fake terminal at four in-flight requests."""
    max_seen, total, workers = asyncio.run(_across_processes(tmp_path, concurrency=4, worker_count=6, tasks=50))
    assert max_seen == 4
    assert total == 50
    assert workers >= 3


def test_cross_process_cap_can_be_lowered(tmp_path: Path) -> None:
    """A cap of 2 is the account limit even when more than two workers are running."""
    max_seen, total, workers = asyncio.run(_across_processes(tmp_path, concurrency=2, worker_count=5, tasks=20))
    assert max_seen == 2
    assert total == 20
    assert workers >= 3


async def _fanout(concurrency: int, tasks: int) -> tuple[int, int]:
    """Run ``tasks`` overlapping list calls in this process.

    Args:
        concurrency: Account cap to install.
        tasks: Number of concurrent list calls.

    Returns:
        The peak in-flight count and the number of requests the fake terminal saw.
    """
    probe = _Probe(concurrency)
    runner, base_url = await _serve(probe.hold)
    try:
        async with ThetaDataClient(_settings(base_url, api_rate_limit=concurrency, api_timeout=30)) as client:

            async def _one() -> None:
                async for _row in client.list_expirations("SPXW"):
                    return

            await asyncio.gather(*(_one() for _ in range(tasks)))
    finally:
        await runner.cleanup()
    return probe.max_seen, probe.total


async def _across_processes(
    tmp_path: Path,
    concurrency: int,
    worker_count: int,
    tasks: int,
) -> tuple[int, int, int]:
    """Spread list calls across worker processes.

    Args:
        tmp_path: Directory for this test's machine lock.
        concurrency: Account cap shared by the workers.
        worker_count: Worker processes. Must be greater than ``concurrency``.
        tasks: Queued requests.

    Returns:
        Peak in-flight requests, total requests, and distinct worker pids.
    """
    probe = _Probe(concurrency)
    runner, base_url = await _serve(probe.hold)
    try:
        reset_request_slots()
        async with backfill_lock(tmp_path / "theta.lock"):
            kwargs = theta_pool_kwargs(worker_count, concurrency=concurrency)
            async with Pool(**kwargs) as pool:
                pids = list(await pool.map(fetch_expiration_pid, [base_url] * tasks))
    finally:
        await runner.cleanup()
    return probe.max_seen, probe.total, len(set(pids))


class _Probe:
    """Count overlapping fake-terminal requests and hold them until the cap is full."""

    def __init__(self, open_at: int) -> None:
        """Store the overlap target.

        Args:
            open_at: Release held requests once this many are in flight.
        """
        self.open_at = open_at
        self.current = 0
        self.max_seen = 0
        self.total = 0
        self._lock = asyncio.Lock()
        self._gate = asyncio.Event()

    async def hold(self, request: web.Request) -> web.Response:
        """Hold the request until ``open_at`` requests overlap, then return one row.

        Args:
            request: Incoming fake-terminal request. The path is not consulted.

        Returns:
            A one-row expiration CSV body.
        """
        del request
        async with self._lock:
            self.current += 1
            self.total += 1
            self.max_seen = max(self.max_seen, self.current)
            if self.current >= self.open_at:
                self._gate.set()
        try:
            try:
                await asyncio.wait_for(self._gate.wait(), timeout=30)
            except TimeoutError:
                pass
            return web.Response(text="symbol,expiration\nSPXW,20240119\n", content_type="text/csv")
        finally:
            async with self._lock:
                self.current -= 1
