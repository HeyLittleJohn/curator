"""Fake-terminal tests for the SPXW downloader.

Rows are synthetic. Nothing here opens Postgres or leaves the machine.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import aiohttp.web as web
import pytest
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata.backfill import run_backfill
from curator.thetadata.client.rows import EodRow, GreeksRow, OhlcRow, OpenInterestRow, QuoteRow
from curator.thetadata.jobs import (
    DATASET_EOD,
    DATASET_GREEKS_FIRST_ORDER,
    DATASET_OHLC,
    DATASET_OPEN_INTEREST,
    DATASET_QUOTES,
    PlanJob,
)
from curator.thetadata.limits import reset_machine_lock, reset_request_slots
from curator.thetadata.mapping import (
    SPECS,
    LoadError,
    PendingFact,
    collapse_records,
    contract_insert_sql,
    fact_insert_sql,
    map_fact,
    theta_interval,
    wall_time_to_timestamptz,
)
from curator.thetadata.models.theta import CONFLICT_COLUMNS
from curator.thetadata.planner import run_plan
from curator.thetadata.progress import MemoryProgressStore
from curator.thetadata.report import LoadReport, empty_bytes, format_load_report, project_count
from curator.thetadata.time import parse_thetadata_timestamp
from curator.thetadata.window import (
    DEFAULT_STRIKE_RANGE,
    METHOD_CENTER,
    METHOD_PARITY,
    METHOD_UNLIMITED,
    StrikeWindowError,
    parse_strike_range,
    select_strike_window,
)

SESSION = date(2024, 1, 2)
NEAR = date(2024, 1, 5)
FAR = date(2024, 1, 19)
EMPTY = date(2024, 2, 16)
ATM = Decimal("5000")
STRIKES = tuple(Decimal(4700 + (index * 50)) for index in range(11))
WINDOW = frozenset({Decimal("4950"), Decimal("5000"), Decimal("5050")})

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


@pytest.fixture(autouse=True)
def _reset_caps() -> Iterator[None]:
    """Give each test a fresh request semaphore and an unlocked machine lock."""
    reset_request_slots()
    reset_machine_lock()
    yield
    reset_machine_lock()
    reset_request_slots()


def _premium(strike: Decimal, right: str) -> Decimal:
    """Return a synthetic open premium whose put-call forward is 5000.

    Args:
        strike: Listed strike.
        right: ``call`` or ``put``.

    Returns:
        The premium.
    """
    gap = ATM - strike if right == "call" else strike - ATM
    return Decimal("10") + max(Decimal("0"), gap)


def _quote_csv(expiration: date) -> str:
    """Build a two-bar quote chain.

    Args:
        expiration: Expiration date written into every row.

    Returns:
        CSV text, open bar first.
    """
    lines = ["symbol,expiration,strike,right,timestamp,bid,ask,bid_size,ask_size"]
    for stamp in ("2024-01-02T09:30:00.000", "2024-01-02T09:31:00.000"):
        for strike in STRIKES:
            for right in ("CALL", "PUT"):
                mid = _premium(strike, right.lower())
                bid = mid - Decimal("0.10")
                ask = mid + Decimal("0.10")
                lines.append(f"SPXW,{expiration:%Y%m%d},{strike},{right},{stamp},{bid},{ask},1,2")
    return "\n".join(lines) + "\n"


def _ohlc_csv(expiration: date) -> str:
    """Build a two-bar OHLC chain whose open matches the quote mid.

    Args:
        expiration: Expiration date.

    Returns:
        CSV text.
    """
    lines = ["symbol,expiration,strike,right,timestamp,open,high,low,close,volume,count,vwap"]
    for stamp in ("2024-01-02T09:30:00.000", "2024-01-02T09:31:00.000"):
        for strike in STRIKES:
            for right in ("CALL", "PUT"):
                mid = _premium(strike, right.lower())
                lines.append(f"SPXW,{expiration:%Y%m%d},{strike},{right},{stamp},{mid},{mid + 1},{mid},{mid},3,1,{mid}")
    return "\n".join(lines) + "\n"


def _greeks_csv(expiration: date) -> str:
    """Build greeks whose bid/ask match the quotes and whose underlying is 1.

    Args:
        expiration: Expiration date.

    Returns:
        CSV text. ``underlying_price`` is intentionally far from the forward.
    """
    header = (
        "symbol,expiration,strike,right,timestamp,bid,ask,delta,theta,vega,rho,epsilon,lambda,"
        "implied_vol,iv_error,underlying_timestamp,underlying_price"
    )
    lines = [header]
    for stamp in ("2024-01-02T09:30:00.000", "2024-01-02T09:31:00.000"):
        for strike in STRIKES:
            for right, delta in (("CALL", "0.40"), ("PUT", "-0.40")):
                mid = _premium(strike, right.lower())
                lines.append(
                    f"SPXW,{expiration:%Y%m%d},{strike},{right},{stamp},"
                    f"{mid - Decimal('0.10')},{mid + Decimal('0.10')},{delta},-0.01,0.02,0.03,0.04,1.1,"
                    f"0.20,0,{stamp},1"
                )
    return "\n".join(lines) + "\n"


def _open_interest_csv(expiration: date) -> str:
    """Build one open-interest row per strike and right.

    Args:
        expiration: Expiration date.

    Returns:
        CSV text.
    """
    lines = ["symbol,expiration,strike,right,timestamp,open_interest"]
    for strike in STRIKES:
        for right in ("C", "P"):
            lines.append(f"SPXW,{expiration:%Y%m%d},{strike},{right},2024-01-02T06:30:00.000,10")
    return "\n".join(lines) + "\n"


def _eod_csv(expiration: date) -> str:
    """Build one full-chain EOD row per strike and right.

    Args:
        expiration: Expiration date.

    Returns:
        CSV text.
    """
    header = (
        "symbol,expiration,strike,right,open,high,low,close,volume,count,created,last_trade,"
        "bid_size,bid_exchange,bid,bid_condition,ask_size,ask_exchange,ask,ask_condition"
    )
    lines = [header]
    for strike in STRIKES:
        for right in ("CALL", "PUT"):
            lines.append(
                f"SPXW,{expiration:%Y%m%d},{strike},{right},1,2,0.5,1.5,4,1,"
                "2024-01-02T16:00:00.000,2024-01-02T15:59:00.000,1,1,1.10,0,1,1,1.20,0"
            )
    return "\n".join(lines) + "\n"


def _strikes_csv() -> str:
    """Build the listed chain.

    Returns:
        CSV text.
    """
    lines = ["symbol,strike"]
    lines.extend(f"SPXW,{strike}" for strike in STRIKES)
    return "\n".join(lines) + "\n"


def _chain_handler(hits: list[str]) -> Handler:
    """Return a terminal fake that records every request.

    Args:
        hits: Appended with ``path?query`` for each GET.

    Returns:
        The aiohttp handler.
    """

    async def handler(request: web.Request) -> web.Response:
        hits.append(f"{request.path}?{request.query_string}")
        path = request.path
        if path.endswith("/option/list/expirations"):
            text = f"symbol,expiration\nSPXW,{NEAR:%Y%m%d}\nSPXW,{FAR:%Y%m%d}\nSPXW,{EMPTY:%Y%m%d}\n"
            return web.Response(text=text, content_type="text/csv")
        if path.endswith("/option/list/strikes"):
            return web.Response(text=_strikes_csv(), content_type="text/csv")
        expiration = request.query.get("expiration", "")
        if expiration == f"{EMPTY:%Y%m%d}":
            return web.Response(status=472, text="NO_DATA")
        bodies = {
            "/option/history/quote": _quote_csv,
            "/option/history/ohlc": _ohlc_csv,
            "/option/history/greeks/first_order": _greeks_csv,
            "/option/history/open_interest": _open_interest_csv,
            "/option/history/eod": _eod_csv,
        }
        for suffix, builder in bodies.items():
            if path.endswith(suffix):
                exp = date(int(expiration[0:4]), int(expiration[4:6]), int(expiration[6:8]))
                return web.Response(text=builder(exp), content_type="text/csv")
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


def _job(dataset: str, expiration: date, interval_s: int | None) -> PlanJob:
    """Build one plan job.

    Args:
        dataset: Dataset name.
        expiration: Expiration date.
        interval_s: Bar length, or ``None`` for a daily dataset.

    Returns:
        The job.
    """
    return PlanJob(
        dataset=dataset,
        trade_date=SESSION,
        expiration=expiration,
        interval_s=interval_s,
        calendar_dte=(expiration - SESSION).days,
    )


def test_strike_window_default_override_and_tie() -> None:
    """±125 is the default, zero and all override it, and a tie keeps the lower strike."""
    assert DEFAULT_STRIKE_RANGE == 125
    assert parse_strike_range("125") == 125
    assert parse_strike_range("0") == 0
    assert parse_strike_range("all") is None
    assert parse_strike_range("*") is None
    with pytest.raises(StrikeWindowError):
        parse_strike_range("-1")

    listed = tuple(Decimal(1000 + (index * 5)) for index in range(300))
    atm = listed[150]
    anchor = []
    for strike in listed:
        call = Decimal("10") + max(Decimal("0"), atm - strike)
        put = Decimal("10") + max(Decimal("0"), strike - atm)
        anchor.append((strike, "call", call))
        anchor.append((strike, "put", put))
    window = select_strike_window(listed, anchor, DEFAULT_STRIKE_RANGE)
    assert window.method == METHOD_PARITY
    assert window.atm == atm
    assert len(window.strikes) == 251
    assert atm in window.strikes
    assert listed[150 - 125] in window.strikes
    assert listed[150 - 126] not in window.strikes

    short = select_strike_window(STRIKES, anchor[:2], DEFAULT_STRIKE_RANGE)
    assert short.strikes == frozenset(STRIKES)
    only = select_strike_window(STRIKES, [(ATM, "call", Decimal("10")), (ATM, "put", Decimal("10"))], 0)
    assert only.strikes == frozenset({ATM})
    unlimited = select_strike_window(STRIKES, [], None)
    assert unlimited.method == METHOD_UNLIMITED
    assert unlimited.strikes == frozenset(STRIKES)

    tied = select_strike_window(
        (Decimal("4950"), Decimal("5000")),
        [(Decimal("4950"), "call", Decimal("30")), (Decimal("4950"), "put", Decimal("5"))],
        0,
    )
    assert tied.forward == Decimal("4975")
    assert tied.atm == Decimal("4950")
    center = select_strike_window(STRIKES, [(ATM, "call", Decimal("10"))], 1)
    assert center.method == METHOD_CENTER
    assert center.atm == Decimal("4950")


def test_dataset_rows_map_across_the_dst_boundary() -> None:
    """Each dataset maps onto its conflict key, and Eastern wall times survive DST."""
    fall = wall_time_to_timestamptz(datetime(2024, 11, 3, 1, 30))
    before = wall_time_to_timestamptz(datetime(2024, 3, 10, 1, 30))
    after = wall_time_to_timestamptz(datetime(2024, 3, 10, 3, 30))
    assert fall == parse_thetadata_timestamp("2024-11-03T01:30:00.000")
    assert fall.utcoffset() == timedelta(hours=-4)
    assert before.utcoffset() == timedelta(hours=-5)
    assert after.utcoffset() == timedelta(hours=-4)
    with pytest.raises(LoadError):
        wall_time_to_timestamptz(datetime(2024, 3, 10, 2, 30))

    quote = map_fact(
        _job(DATASET_QUOTES, NEAR, 60),
        QuoteRow(
            symbol="spxw",
            expiration=NEAR,
            strike=Decimal("5000"),
            right="CALL",
            timestamp=datetime(2024, 11, 3, 1, 30),
            bid=Decimal("1"),
            ask=Decimal("1.2"),
        ),
    )
    assert quote.values["ts"] == fall
    assert quote.values["interval_s"] == 60
    assert quote.contract.right == "call"
    assert quote.contract.symbol == "SPXW"

    ohlc = map_fact(
        _job(DATASET_OHLC, FAR, 900),
        OhlcRow(
            symbol="SPXW",
            expiration=FAR,
            strike=Decimal("5000"),
            right="PUT",
            timestamp=datetime(2024, 1, 2, 9, 30),
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("1"),
            close=Decimal("1.5"),
            volume=3,
        ),
    )
    assert ohlc.values["interval_s"] == 900
    assert ohlc.values["vwap"] == Decimal("0")

    greek = map_fact(
        _job(DATASET_GREEKS_FIRST_ORDER, NEAR, 60),
        GreeksRow(
            symbol="SPXW",
            expiration=NEAR,
            strike=Decimal("5000"),
            right="PUT",
            timestamp=datetime(2024, 11, 3, 1, 30),
            bid=Decimal("1"),
            ask=Decimal("1.2"),
            delta=Decimal("-0.4"),
            theta=Decimal("-0.01"),
            vega=Decimal("0.02"),
            rho=Decimal("0.03"),
            epsilon=Decimal("0.04"),
            lambda_=Decimal("1.1"),
            implied_vol=Decimal("0.2"),
            underlying_timestamp=datetime(2024, 11, 3, 1, 30),
            underlying_price=Decimal("1"),
        ),
    )
    assert greek.values["underlying_price"] == Decimal("1")
    assert greek.values["delta"] == Decimal("-0.4")
    assert "gamma" not in greek.values
    with pytest.raises(LoadError):
        map_fact(
            _job(DATASET_GREEKS_FIRST_ORDER, NEAR, 60),
            GreeksRow(
                symbol="SPXW",
                expiration=NEAR,
                strike=Decimal("5000"),
                right="CALL",
                timestamp=datetime(2024, 11, 3, 1, 30),
                bid=Decimal("1"),
                ask=Decimal("1.2"),
            ),
        )

    interest = map_fact(
        _job(DATASET_OPEN_INTEREST, NEAR, None),
        OpenInterestRow(
            symbol="SPXW",
            expiration=NEAR,
            strike=Decimal("5000"),
            right="C",
            timestamp=datetime(2024, 1, 2, 6, 30),
            open_interest=10,
        ),
    )
    assert interest.values["date"] == SESSION
    eod = map_fact(
        _job(DATASET_EOD, NEAR, None),
        EodRow(
            symbol="SPXW",
            expiration=NEAR,
            strike=Decimal("5000"),
            right="P",
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1.5"),
            volume=4,
            bid_size=8,
            bid=Decimal("1.1"),
            ask=Decimal("1.2"),
        ),
    )
    assert eod.values["bid_size"] == 8
    assert eod.values["date"] == SESSION
    assert theta_interval(60) == "1m"
    assert theta_interval(900) == "15m"


def test_upsert_sql_uses_the_schema_conflict_keys() -> None:
    """Fact upserts target TD-2's conflict columns and collapse duplicate keys."""
    for dataset, spec in SPECS.items():
        assert spec.conflict == CONFLICT_COLUMNS[dataset]
        assert "gamma" not in spec.column_names
    quote_sql = " ".join(fact_insert_sql(SPECS[DATASET_QUOTES]).split())
    eod_sql = " ".join(fact_insert_sql(SPECS[DATASET_EOD]).split())
    assert "ON CONFLICT (contract_id, ts)" in quote_sql
    assert 'ON CONFLICT (contract_id, "date")' in eod_sql
    assert 'ON CONFLICT (symbol, expiration, strike, "right")' in " ".join(contract_insert_sql().split())
    key = map_fact(
        _job(DATASET_QUOTES, NEAR, 60),
        QuoteRow(
            symbol="SPXW",
            expiration=NEAR,
            strike=Decimal("5000"),
            right="CALL",
            timestamp=datetime(2024, 1, 2, 9, 30),
            bid=Decimal("1"),
            ask=Decimal("1.1"),
        ),
    )
    first = {"contract_id": 1, **dict(key.values)}
    second = {"contract_id": 1, **dict(key.values), "bid": Decimal("9")}
    collapsed = collapse_records([first, second], SPECS[DATASET_QUOTES].conflict)
    assert len(collapsed) == 1
    assert collapsed[0]["bid"] == Decimal("9")


def test_claim_stays_inside_the_requested_session() -> None:
    """A dated claim does not take a pending job from another session."""
    store = MemoryProgressStore()
    run_plan([NEAR], SESSION, date(2024, 1, 3), datasets=["quotes"], queue=store)
    claimed = store.claim(10, start=date(2024, 1, 3), end=date(2024, 1, 3), datasets=["quotes"])
    assert len(claimed) == 1
    assert claimed[0].trade_date == date(2024, 1, 3)
    assert store.census(start=SESSION, end=SESSION, datasets=["quotes"]).pending == 1


def test_report_text_is_counts_only() -> None:
    """The printed report has sizes and counts, not prices or a connection string."""
    report = LoadReport(
        rows={("quotes", "1m"): 12, ("quotes", "15m"): 24, ("eod", "daily"): 22},
        relation_bytes=empty_bytes() | {"quotes": 1000, "eod": 200},
        sessions_loaded=1,
        sessions_full=10,
        requests=4,
        new_rows=36,
        jobs_done=2,
        jobs_empty=1,
        jobs_failed=0,
    )
    text = format_load_report(report)
    assert "rows_quotes_1m=12" in text
    assert "rows_quotes_15m=24" in text
    assert "projected_rows_quotes=360" in text
    assert "projected_bytes_quotes=10000" in text
    assert "sessions_from_floor=10" in text
    assert "." not in text
    assert "postgresql" not in text.lower()
    assert project_count(12, 1, 10) == 120
    assert project_count(0, 1, 10) == 0


def _strikes_for(captured: Sequence[tuple[PlanJob, PendingFact]], dataset: str, expiration: date) -> set[Decimal]:
    """Collect stored strikes for one dataset and expiration.

    Args:
        captured: Writer output.
        dataset: Dataset name.
        expiration: Expiration date.

    Returns:
        The strike set.
    """
    return {fact.contract.strike for job, fact in captured if job.dataset == dataset and job.expiration == expiration}


async def _load(
    *,
    strike_range: int | None,
    datasets: list[str] | None,
    max_dte: int | None,
    hits: list[str],
    captured: list[tuple[PlanJob, PendingFact]],
    queue: MemoryProgressStore,
    lock: Path,
) -> LoadReport:
    """Run one in-process load against the shared fake.

    Args:
        strike_range: Window width.
        datasets: Dataset filter.
        max_dte: Optional DTE cap.
        hits: Request log. The handler appends to it.
        captured: Writer output.
        queue: Progress queue reused across reruns.
        lock: Machine lock path.

    Returns:
        The load report.
    """
    runner, base_url = await _serve(_chain_handler(hits))
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

        return await run_backfill(
            start=SESSION,
            end=SESSION,
            api_base_url=base_url,
            database_url=None,
            datasets=datasets,
            max_dte=max_dte,
            strike_range=strike_range,
            lock_path=lock,
            queue=queue,
            use_pool=False,
            writer=writer,
        )
    finally:
        await runner.cleanup()


def test_load_uses_job_intervals_window_and_is_idempotent(tmp_path: Path) -> None:
    """All expirations load at the job interval, and a rerun makes no requests."""
    hits: list[str] = []
    captured: list[tuple[PlanJob, PendingFact]] = []
    queue = MemoryProgressStore()

    async def _run() -> None:
        report = await _load(
            strike_range=1,
            datasets=None,
            max_dte=None,
            hits=hits,
            captured=captured,
            queue=queue,
            lock=tmp_path / "theta.lock",
        )
        assert report.jobs_failed == 0, report.errors
        assert report.jobs_empty == 5
        assert report.jobs_done == 10
        assert report.requests == len(hits)
        assert report.new_rows == len(captured)
        before = len(hits)
        stored = len(captured)
        again = await _load(
            strike_range=1,
            datasets=None,
            max_dte=None,
            hits=hits,
            captured=captured,
            queue=queue,
            lock=tmp_path / "theta.lock",
        )
        assert len(hits) == before
        assert len(captured) == stored
        assert again.requests == 0
        assert again.new_rows == 0
        assert again.jobs_done == 0

    asyncio.run(_run())
    near_quotes = [hit for hit in hits if "history/quote" in hit and f"expiration={NEAR:%Y%m%d}" in hit]
    far_quotes = [hit for hit in hits if "history/quote" in hit and f"expiration={FAR:%Y%m%d}" in hit]
    assert near_quotes and "interval=1m" in near_quotes[0]
    assert far_quotes and "interval=15m" in far_quotes[0]
    assert any(f"expiration={EMPTY:%Y%m%d}" in hit for hit in hits)
    assert any("history/eod" in hit and "interval=" not in hit for hit in hits)
    assert _strikes_for(captured, DATASET_QUOTES, NEAR) == WINDOW
    assert _strikes_for(captured, DATASET_QUOTES, FAR) == WINDOW
    assert _strikes_for(captured, DATASET_OHLC, NEAR) == WINDOW
    assert _strikes_for(captured, DATASET_GREEKS_FIRST_ORDER, NEAR) == WINDOW
    assert _strikes_for(captured, DATASET_EOD, NEAR) == frozenset(STRIKES)
    assert _strikes_for(captured, DATASET_OPEN_INTEREST, FAR) == frozenset(STRIKES)
    assert _strikes_for(captured, DATASET_QUOTES, EMPTY) == set()
    near_interval = {
        fact.values["interval_s"] for job, fact in captured if job.dataset == DATASET_QUOTES and job.expiration == NEAR
    }
    far_interval = {
        fact.values["interval_s"] for job, fact in captured if job.dataset == DATASET_QUOTES and job.expiration == FAR
    }
    assert near_interval == {60}
    assert far_interval == {900}
    underlying = {
        fact.values["underlying_price"]
        for job, fact in captured
        if job.dataset == DATASET_GREEKS_FIRST_ORDER and job.expiration == NEAR
    }
    assert underlying == {Decimal("1")}
    census = queue.census(
        start=SESSION, end=SESSION, datasets=["quotes", "ohlc", "greeks_first_order", "open_interest", "eod"]
    )
    assert census.pending == 0
    assert census.failed == 0
    assert census.done == 10
    assert census.empty == 5


def test_full_chain_and_max_dte(tmp_path: Path) -> None:
    """``all`` keeps every strike, and max DTE drops the far expiration."""
    hits: list[str] = []
    captured: list[tuple[PlanJob, PendingFact]] = []

    async def _run() -> None:
        wide = await _load(
            strike_range=None,
            datasets=["quotes"],
            max_dte=None,
            hits=hits,
            captured=captured,
            queue=MemoryProgressStore(),
            lock=tmp_path / "wide.lock",
        )
        assert wide.jobs_failed == 0, wide.errors
        capped_hits: list[str] = []
        await _load(
            strike_range=1,
            datasets=["quotes"],
            max_dte=5,
            hits=capped_hits,
            captured=[],
            queue=MemoryProgressStore(),
            lock=tmp_path / "cap.lock",
        )
        assert not any("/option/list/strikes" in hit for hit in hits)
        assert not any(f"expiration={FAR:%Y%m%d}" in hit for hit in capped_hits)
        assert any(f"expiration={NEAR:%Y%m%d}" in hit for hit in capped_hits)

    asyncio.run(_run())
    assert _strikes_for(captured, DATASET_QUOTES, NEAR) == frozenset(STRIKES)
    assert Decimal("4700") in _strikes_for(captured, DATASET_QUOTES, FAR)


def test_cli_lists_load_commands_and_requires_a_database(tmp_path: Path) -> None:
    """Help lists the load commands, and a load without a database does not print a URL."""
    help_result = CliRunner().invoke(app, ["theta", "--help"])
    assert help_result.exit_code == 0
    assert "backfill" in help_result.stdout
    assert "load-day" in help_result.stdout
    env = {"PATH": os.environ.get("PATH", ""), "THETADATA_LOCK_PATH": str(tmp_path / "theta.lock")}
    result = CliRunner().invoke(app, ["theta", "load-day", "--date", "2024-01-02"], env=env)
    combined = f"{result.stdout}\n{result.stderr or ''}"
    assert result.exit_code == 1
    assert "database URL is required" in combined
    assert "postgresql" not in combined.lower()
    assert "://" not in combined
