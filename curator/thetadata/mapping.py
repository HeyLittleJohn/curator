"""Map ThetaData rows onto theta-schema insert values.

Timestamps go through ``parse_thetadata_timestamp``, so a zone-less Eastern
wall time becomes a ``timestamptz``. The bar length stored on each intraday
row is the plan job's ``interval_s``, not a length inferred from the payload.
``underlying_price`` is copied onto the greek row and is not read back as spot.

Conflict targets are TD-2's ``CONFLICT_COLUMNS``. Contracts are keyed by
``(symbol, expiration, strike, right)`` and inserted before the fact rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping

from curator.thetadata.client.rows import EodRow, GreeksRow, OhlcRow, OpenInterestRow, QuoteRow, ThetaRow
from curator.thetadata.identity import normalize_symbol, parse_right, parse_strike
from curator.thetadata.jobs import (
    DATASET_EOD,
    DATASET_GREEKS_FIRST_ORDER,
    DATASET_OHLC,
    DATASET_OPEN_INTEREST,
    DATASET_QUOTES,
    PlanJob,
)
from curator.thetadata.models.theta import CONFLICT_COLUMNS
from curator.thetadata.time import parse_thetadata_timestamp

Cell = Decimal | int | datetime | date | None
_NONEGATIVE_GREEKS = frozenset({"bid", "ask", "implied_vol", "underlying_price"})


class LoadError(ValueError):
    """A ThetaData row cannot be stored."""


@dataclass(frozen=True)
class ContractKey:
    """Natural key of one ``theta.contracts`` row.

    Args:
        symbol: Normalized underlying.
        expiration: Expiration date.
        strike: Strike in dollars, scale 3.
        right: ``call`` or ``put``.
    """

    symbol: str
    expiration: date
    strike: Decimal
    right: str


@dataclass(frozen=True)
class PendingFact:
    """One fact row waiting for its ``contract_id``.

    Args:
        contract: Contract natural key. The loader upserts this first.
        values: Fact columns except ``contract_id``.
    """

    contract: ContractKey
    values: Mapping[str, Cell]


@dataclass(frozen=True)
class FactSpec:
    """Insert shape for one fact table.

    Args:
        table: Unqualified theta table name.
        columns: ``(column, postgres type)`` in COPY order, including ``contract_id``.
        conflict: Columns passed to ``ON CONFLICT``. These match ``CONFLICT_COLUMNS``.
    """

    table: str
    columns: tuple[tuple[str, str], ...]
    conflict: tuple[str, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return the column names in COPY order.

        Returns:
            Column names, including ``contract_id``.
        """
        return tuple(name for name, _type in self.columns)

    @property
    def update_names(self) -> tuple[str, ...]:
        """Return the columns overwritten on conflict.

        Returns:
            Every insert column that is not part of the conflict key.
        """
        keys = set(self.conflict)
        return tuple(name for name in self.column_names if name not in keys)


def _spec(table: str, columns: tuple[tuple[str, str], ...]) -> FactSpec:
    """Build a spec whose conflict key is the schema's unique key.

    Args:
        table: Fact table name.
        columns: Copy columns and types.

    Returns:
        The spec.

    Raises:
        LoadError: The table has no conflict key.
    """
    conflict = CONFLICT_COLUMNS.get(table)
    if conflict is None:
        raise LoadError("missing conflict columns")
    return FactSpec(table=table, columns=columns, conflict=conflict)


_PRICE = "numeric(18,8)"
_WHOLE = "bigint"
_INT = "integer"

SPECS: dict[str, FactSpec] = {
    DATASET_QUOTES: _spec(
        DATASET_QUOTES,
        (
            ("contract_id", "bigint"),
            ("interval_s", "smallint"),
            ("ts", "timestamptz"),
            ("bid_size", _INT),
            ("bid_exchange", _INT),
            ("bid", _PRICE),
            ("bid_condition", _INT),
            ("ask_size", _INT),
            ("ask_exchange", _INT),
            ("ask", _PRICE),
            ("ask_condition", _INT),
        ),
    ),
    DATASET_OHLC: _spec(
        DATASET_OHLC,
        (
            ("contract_id", "bigint"),
            ("interval_s", "smallint"),
            ("ts", "timestamptz"),
            ("open", _PRICE),
            ("high", _PRICE),
            ("low", _PRICE),
            ("close", _PRICE),
            ("volume", _WHOLE),
            ("count", _WHOLE),
            ("vwap", _PRICE),
        ),
    ),
    DATASET_GREEKS_FIRST_ORDER: _spec(
        DATASET_GREEKS_FIRST_ORDER,
        (
            ("contract_id", "bigint"),
            ("interval_s", "smallint"),
            ("ts", "timestamptz"),
            ("bid", _PRICE),
            ("ask", _PRICE),
            ("delta", _PRICE),
            ("theta", _PRICE),
            ("vega", _PRICE),
            ("rho", _PRICE),
            ("epsilon", _PRICE),
            ("lambda", _PRICE),
            ("implied_vol", _PRICE),
            ("iv_error", _PRICE),
            ("underlying_timestamp", "timestamptz"),
            ("underlying_price", _PRICE),
        ),
    ),
    DATASET_OPEN_INTEREST: _spec(
        DATASET_OPEN_INTEREST,
        (
            ("contract_id", "bigint"),
            ("date", "date"),
            ("open_interest", _WHOLE),
            ("reported_at", "timestamptz"),
        ),
    ),
    DATASET_EOD: _spec(
        DATASET_EOD,
        (
            ("contract_id", "bigint"),
            ("date", "date"),
            ("created", "timestamptz"),
            ("last_trade", "timestamptz"),
            ("open", _PRICE),
            ("high", _PRICE),
            ("low", _PRICE),
            ("close", _PRICE),
            ("volume", _WHOLE),
            ("count", _WHOLE),
            ("bid_size", _INT),
            ("bid_exchange", _INT),
            ("bid", _PRICE),
            ("bid_condition", _INT),
            ("ask_size", _INT),
            ("ask_exchange", _INT),
            ("ask", _PRICE),
            ("ask_condition", _INT),
        ),
    ),
}

_INTERVALS = {60: "1m", 300: "5m", 600: "10m", 900: "15m", 1800: "30m", 3600: "1h"}
_RESERVED = frozenset({"right", "date", "lambda"})


def theta_interval(interval_s: int) -> str:
    """Return the ThetaData interval token for a job bar length.

    Args:
        interval_s: Bar length in seconds from the plan job.

    Returns:
        A token the Options Standard client accepts, such as ``1m`` or ``15m``.

    Raises:
        LoadError: The bar length is not one of the standard tokens.
    """
    try:
        return _INTERVALS[interval_s]
    except KeyError as exc:
        raise LoadError("job interval is not a ThetaData standard bar") from exc


def sql_ident(name: str) -> str:
    """Quote a column name the loader is allowed to emit.

    Args:
        name: A column from a fact spec or the contracts key.

    Returns:
        A safe identifier. ``right``, ``date``, and ``lambda`` are quoted.

    Raises:
        LoadError: ``name`` is not one of the loader's columns.
    """
    if name in _RESERVED:
        return f'"{name}"'
    if not name.isidentifier() or not name.islower() or name != name.strip():
        raise LoadError("unsafe column name")
    return name


def wall_time_to_timestamptz(value: datetime) -> datetime:
    """Convert a zone-less terminal timestamp to America/New_York.

    The fall-back hour uses the earlier, daylight instant, which is what
    ``parse_thetadata_timestamp`` does. A wall time that does not exist on the
    spring-forward morning is rejected.

    Args:
        value: Naive timestamp from the row parser.

    Returns:
        A timezone-aware America/New_York datetime.

    Raises:
        LoadError: The timestamp has a zone or is not a New York wall time.
    """
    if value.tzinfo is not None:
        raise LoadError("ThetaData v3 timestamps are zone-less America/New_York times")
    text = value.isoformat(sep="T", timespec="milliseconds")
    try:
        return parse_thetadata_timestamp(text)
    except ValueError as exc:
        raise LoadError("ThetaData v3 timestamp is not an America/New_York time") from exc


def map_fact(job: PlanJob, row: ThetaRow) -> PendingFact:
    """Map one parsed terminal row into a fact waiting for ``contract_id``.

    Args:
        job: Plan job. Intraday jobs supply ``interval_s``. Daily jobs use
            ``trade_date`` as the session key.
        row: A typed quote, OHLC, greek, open-interest, or EOD row.

    Returns:
        The contract key and fact values.

    Raises:
        LoadError: The row is the wrong type or is missing a required field.
    """
    if job.dataset == DATASET_QUOTES:
        return _map_quote(job, _expect(row, QuoteRow))
    if job.dataset == DATASET_OHLC:
        return _map_ohlc(job, _expect(row, OhlcRow))
    if job.dataset == DATASET_GREEKS_FIRST_ORDER:
        return _map_greeks(job, _expect(row, GreeksRow))
    if job.dataset == DATASET_OPEN_INTEREST:
        return _map_open_interest(job, _expect(row, OpenInterestRow))
    if job.dataset == DATASET_EOD:
        return _map_eod(job, _expect(row, EodRow))
    raise LoadError("unknown dataset")


def collapse_records(records: list[dict[str, Cell]], conflict: tuple[str, ...]) -> list[dict[str, Cell]]:
    """Keep one record per conflict key.

    PostgreSQL rejects an insert that updates the same conflict key twice.
    The last record in ``records`` wins.

    Args:
        records: Fact records that already include ``contract_id``.
        conflict: Conflict columns.

    Returns:
        Records unique on ``conflict``, in first-seen key order.
    """
    chosen: dict[tuple[Cell, ...], dict[str, Cell]] = {}
    for record in records:
        chosen[tuple(record[name] for name in conflict)] = record
    return list(chosen.values())


def contract_insert_sql() -> str:
    """Return the contract upsert statement.

    Returns:
        ``INSERT ... ON CONFLICT`` SQL that reads ``tmp_theta_contracts`` and
        returns the surrogate id. The conflict key is the contracts unique key.
    """
    return """
INSERT INTO theta.contracts (symbol, expiration, strike, "right")
SELECT DISTINCT symbol, expiration, strike, "right"
FROM tmp_theta_contracts
ON CONFLICT (symbol, expiration, strike, "right")
DO UPDATE SET symbol = EXCLUDED.symbol
RETURNING id, symbol, expiration, strike, "right"
"""


def fact_insert_sql(spec: FactSpec) -> str:
    """Return the fact upsert statement for one dataset.

    Args:
        spec: Fact table shape.

    Returns:
        ``INSERT ... ON CONFLICT`` SQL that reads ``tmp_theta_facts``.
    """
    columns = ", ".join(sql_ident(name) for name in spec.column_names)
    conflict = ", ".join(sql_ident(name) for name in spec.conflict)
    assignments = ", ".join(f"{sql_ident(name)} = EXCLUDED.{sql_ident(name)}" for name in spec.update_names)
    return f"""
INSERT INTO theta.{spec.table} ({columns})
SELECT {columns}
FROM tmp_theta_facts
ON CONFLICT ({conflict})
DO UPDATE SET {assignments}
"""


def _expect[T: ThetaRow](row: ThetaRow, kind: type[T]) -> T:
    """Return ``row`` when it is ``kind``.

    Args:
        row: Parsed terminal row.
        kind: Expected row class.

    Returns:
        The same row.

    Raises:
        LoadError: ``row`` is a different dataset's row.
    """
    if isinstance(row, kind):
        return row
    raise LoadError("row does not match the job dataset")


def _contract(row: QuoteRow | OhlcRow | GreeksRow | OpenInterestRow | EodRow) -> ContractKey:
    """Build the contracts natural key.

    Args:
        row: A typed terminal row with symbol, expiration, strike, and right.

    Returns:
        The normalized key.

    Raises:
        LoadError: The identity fields are not a contract key.
    """
    try:
        return ContractKey(
            symbol=normalize_symbol(row.symbol),
            expiration=row.expiration,
            strike=parse_strike(row.strike),
            right=parse_right(row.right.value),
        )
    except (TypeError, ValueError) as exc:
        raise LoadError("row contract identity is not usable") from exc


def _interval(job: PlanJob) -> int:
    """Return the job bar length.

    Args:
        job: Intraday plan job.

    Returns:
        ``interval_s`` in seconds.

    Raises:
        LoadError: The job has no positive bar length.
    """
    if job.interval_s is None or job.interval_s <= 0:
        raise LoadError("intraday job is missing its bar length")
    return job.interval_s


def _whole(value: int | None) -> int:
    """Return a non-negative integer, using zero when the terminal omitted it.

    Args:
        value: Size, exchange, condition, count, or volume.

    Returns:
        The value, or 0 when it was omitted.

    Raises:
        LoadError: The value is negative.
    """
    if value is None:
        return 0
    if value < 0:
        raise LoadError("a row has a negative size or count")
    return value


def _premium(value: Decimal | None, *, missing_zero: bool) -> Decimal:
    """Return a non-negative price.

    Args:
        value: Terminal price.
        missing_zero: When true, a missing price is stored as zero. EOD quote
            fields use that. A missing OHLC or NBBO price is an error.

    Returns:
        The price.

    Raises:
        LoadError: The price is missing or negative.
    """
    if value is None:
        if missing_zero:
            return Decimal("0")
        raise LoadError("a row is missing a price")
    if value < 0:
        raise LoadError("a row has a negative price")
    return value


def _map_quote(job: PlanJob, row: QuoteRow) -> PendingFact:
    """Map one NBBO quote.

    Args:
        job: Quote job. ``interval_s`` is stored as-is.
        row: Parsed quote.

    Returns:
        The pending fact.
    """
    return PendingFact(
        contract=_contract(row),
        values={
            "interval_s": _interval(job),
            "ts": wall_time_to_timestamptz(row.timestamp),
            "bid_size": _whole(row.bid_size),
            "bid_exchange": _whole(row.bid_exchange),
            "bid": _premium(row.bid, missing_zero=False),
            "bid_condition": _whole(row.bid_condition),
            "ask_size": _whole(row.ask_size),
            "ask_exchange": _whole(row.ask_exchange),
            "ask": _premium(row.ask, missing_zero=False),
            "ask_condition": _whole(row.ask_condition),
        },
    )


def _map_ohlc(job: PlanJob, row: OhlcRow) -> PendingFact:
    """Map one OHLC bar.

    Args:
        job: OHLC job.
        row: Parsed bar. A missing count or vwap is stored as zero.

    Returns:
        The pending fact.
    """
    return PendingFact(
        contract=_contract(row),
        values={
            "interval_s": _interval(job),
            "ts": wall_time_to_timestamptz(row.timestamp),
            "open": _premium(row.open, missing_zero=False),
            "high": _premium(row.high, missing_zero=False),
            "low": _premium(row.low, missing_zero=False),
            "close": _premium(row.close, missing_zero=False),
            "volume": _whole(row.volume),
            "count": _whole(row.count),
            "vwap": _premium(row.vwap, missing_zero=True),
        },
    )


def _greek_value(name: str, value: Decimal | None) -> Decimal:
    """Return one greek, IV, or underlying field.

    Args:
        name: Column name. Used only to name a missing field, not its value.
        value: Parsed number. ``iv_error`` may be omitted and is then zero.

    Returns:
        The number to store.

    Raises:
        LoadError: A required field is missing, or a non-negative field is negative.
    """
    if value is None:
        if name == "iv_error":
            return Decimal("0")
        raise LoadError(f"a greeks row is missing {name}")
    if name in _NONEGATIVE_GREEKS and value < 0:
        raise LoadError("a greeks row has a negative price")
    return value


def _map_greeks(job: PlanJob, row: GreeksRow) -> PendingFact:
    """Map one first-order greek.

    Gamma is not stored. ``underlying_price`` is stored and is not spot.

    Args:
        job: Greeks job.
        row: Parsed greek row.

    Returns:
        The pending fact.
    """
    underlying_timestamp = row.underlying_timestamp or row.timestamp
    return PendingFact(
        contract=_contract(row),
        values={
            "interval_s": _interval(job),
            "ts": wall_time_to_timestamptz(row.timestamp),
            "bid": _greek_value("bid", row.bid),
            "ask": _greek_value("ask", row.ask),
            "delta": _greek_value("delta", row.delta),
            "theta": _greek_value("theta", row.theta),
            "vega": _greek_value("vega", row.vega),
            "rho": _greek_value("rho", row.rho),
            "epsilon": _greek_value("epsilon", row.epsilon),
            "lambda": _greek_value("lambda", row.lambda_),
            "implied_vol": _greek_value("implied_vol", row.implied_vol),
            "iv_error": _greek_value("iv_error", row.iv_error),
            "underlying_timestamp": wall_time_to_timestamptz(underlying_timestamp),
            "underlying_price": _greek_value("underlying_price", row.underlying_price),
        },
    )


def _map_open_interest(job: PlanJob, row: OpenInterestRow) -> PendingFact:
    """Map one open-interest print.

    The session key is the job's trade date, which is the session that was
    requested. ``reported_at`` is the terminal timestamp.

    Args:
        job: Open-interest job.
        row: Parsed print.

    Returns:
        The pending fact.
    """
    return PendingFact(
        contract=_contract(row),
        values={
            "date": job.trade_date,
            "open_interest": _whole(row.open_interest),
            "reported_at": wall_time_to_timestamptz(row.timestamp),
        },
    )


def _session_close(day: date) -> datetime:
    """Return 16:00 America/New_York on a session date.

    Args:
        day: Trade date.

    Returns:
        The close timestamp used when an EOD row omits ``created``.
    """
    return wall_time_to_timestamptz(datetime(day.year, day.month, day.day, 16, 0))


def _map_eod(job: PlanJob, row: EodRow) -> PendingFact:
    """Map one end-of-day report.

    The session key is the job's trade date. Missing bid, ask, size, exchange,
    condition, and count fields are stored as zero. A missing ``created`` is
    the session close. ``last_trade`` stays null when the terminal omits it.

    Args:
        job: EOD job.
        row: Parsed report.

    Returns:
        The pending fact.
    """
    created = wall_time_to_timestamptz(row.created) if row.created is not None else _session_close(job.trade_date)
    last_trade = wall_time_to_timestamptz(row.last_trade) if row.last_trade is not None else None
    return PendingFact(
        contract=_contract(row),
        values={
            "date": job.trade_date,
            "created": created,
            "last_trade": last_trade,
            "open": _premium(row.open, missing_zero=False),
            "high": _premium(row.high, missing_zero=False),
            "low": _premium(row.low, missing_zero=False),
            "close": _premium(row.close, missing_zero=False),
            "volume": _whole(row.volume),
            "count": _whole(row.count),
            "bid_size": _whole(row.bid_size),
            "bid_exchange": _whole(row.bid_exchange),
            "bid": _premium(row.bid, missing_zero=True),
            "bid_condition": _whole(row.bid_condition),
            "ask_size": _whole(row.ask_size),
            "ask_exchange": _whole(row.ask_exchange),
            "ask": _premium(row.ask, missing_zero=True),
            "ask_condition": _whole(row.ask_condition),
        },
    )
