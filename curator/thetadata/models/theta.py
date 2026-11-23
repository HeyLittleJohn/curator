"""SPXW Options Standard tables in the ``theta`` schema.

Conflict keys are the columns TD-5 should use in ``ON CONFLICT``. Partitioned
``quotes`` includes ``ts`` in its primary key, which PostgreSQL requires of a
unique constraint on a partitioned table. ``underlying_price`` is stored for
greeks and IV only and is not the SPX spot source.

v3 timestamps are zone-less America/New_York wall times. Parse them with
``curator.thetadata.time.parse_thetadata_timestamp`` before insert.
"""

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from curator.thetadata.config import THETA_SCHEMA
from curator.thetadata.identity import RIGHT_CALL, RIGHT_PUT

TS_COMMENT = (
    "v3 zone-less America/New_York wall time, parsed DST-aware by "
    "curator.thetadata.time.parse_thetadata_timestamp and stored as timestamptz."
)
STRIKE_COMMENT = "Dollars, exact to one thousandth (numeric scale 3). Not a float."
UNDERLYING_PRICE_COMMENT = "Underlying price ThetaData used for greeks and IV. Reference only; not the SPX spot source."
SESSION_DATE_COMMENT = "Eastern session date. Part of the ON CONFLICT key with contract_id."
SCHEMA_COMMENT = (
    "SPXW Options Standard (ThetaData v3) inside the markets database. "
    "Timestamps are timestamptz from America/New_York wall times. "
    "underlying_price is not the SPX spot source."
)
QUOTES_COMMENT = (
    "Intraday NBBO from v3 history/quote. Monthly RANGE partitions on ts. "
    "interval_s is 60 for calendar DTE 0-5 and 900 otherwise. Unique key is (contract_id, ts)."
)
OHLC_COMMENT = (
    "Intraday bars from v3 history/ohlc, including vwap. "
    "interval_s is 60 for calendar DTE 0-5 and 900 otherwise. Unique key is (contract_id, ts)."
)
GREEKS_COMMENT = (
    "Intraday first-order greeks from v3 history/greeks/first_order. "
    "No second-order greeks and no trade greeks. Unique key is (contract_id, ts)."
)
OPEN_INTEREST_COMMENT = "Daily open interest from v3 history/open_interest. Unique key is (contract_id, date)."
EOD_COMMENT = "Daily national EOD report from v3 history/eod. Unique key is (contract_id, date)."
PROGRESS_COMMENT = "Backfill progress for TD-4. Unique key is (dataset, date, expiration)."

QUOTES_NONEGATIVE_SQL = "bid >= 0 AND ask >= 0 AND bid_size >= 0 AND ask_size >= 0"
OHLC_NONEGATIVE_SQL = "open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0 AND count >= 0 AND vwap >= 0"
GREEKS_NONEGATIVE_SQL = "bid >= 0 AND ask >= 0 AND implied_vol >= 0 AND underlying_price >= 0"
EOD_NONEGATIVE_SQL = (
    "open >= 0 AND high >= 0 AND low >= 0 AND close >= 0 AND volume >= 0 AND count >= 0 "
    "AND bid >= 0 AND ask >= 0 AND bid_size >= 0 AND ask_size >= 0"
)
OPEN_INTEREST_NONEGATIVE_SQL = "open_interest >= 0"
RIGHT_SQL = f"\"right\" IN ('{RIGHT_CALL}', '{RIGHT_PUT}')"
PROGRESS_STATUS_SQL = "status IN ('pending', 'running', 'done', 'failed', 'empty')"
PROGRESS_INTERVAL_SQL = "interval_s IS NULL OR interval_s > 0"
PROGRESS_INTERVAL_COMMENT = "Intraday bar length in seconds. Null for daily open interest and EOD."

UQ_CONTRACTS = "uq_contracts_symbol_expiration_strike_right"

# Database column names TD-5 passes to ON CONFLICT. They match the unique indexes.
CONFLICT_COLUMNS: dict[str, tuple[str, ...]] = {
    "contracts": ("symbol", "expiration", "strike", "right"),
    "quotes": ("contract_id", "ts"),
    "ohlc": ("contract_id", "ts"),
    "greeks_first_order": ("contract_id", "ts"),
    "open_interest": ("contract_id", "date"),
    "eod": ("contract_id", "date"),
    "progress": ("dataset", "date", "expiration"),
}


class ThetaBase(DeclarativeBase):
    """Declarative base whose tables live in the theta schema."""

    metadata = MetaData(schema=THETA_SCHEMA)


def _contract_id(table: str) -> Mapped[int]:
    """Build a contract foreign key that leads the primary key.

    Args:
        table: Child table name, used in the constraint name.

    Returns:
        The ``contract_id`` column.
    """
    return mapped_column(
        BigInteger,
        ForeignKey(f"{THETA_SCHEMA}.contracts.id", name=f"fk_{table}_contract_id"),
        primary_key=True,
    )


def _interval() -> Mapped[int]:
    """Build the intraday bar length column.

    Returns:
        ``interval_s`` in seconds. 60 or 900 for the default DTE tiers.
    """
    return mapped_column(SmallInteger, nullable=False)


def _ts() -> Mapped[datetime]:
    """Build the intraday timestamp column.

    Returns:
        A timestamptz primary-key column.
    """
    return mapped_column(DateTime(timezone=True), primary_key=True, comment=TS_COMMENT)


def _price() -> Mapped[Decimal]:
    """Build a non-null exact price column.

    Returns:
        A numeric column. This is not a float.
    """
    return mapped_column(Numeric(18, 8), nullable=False)


def _whole() -> Mapped[int]:
    """Build a non-null integer count column.

    Returns:
        A big integer column.
    """
    return mapped_column(BigInteger, nullable=False)


class Contract(ThetaBase):
    """One SPXW option contract.

    The surrogate ``id`` is what fact tables store. The natural key is
    ``(symbol, expiration, strike, right)``.
    """

    __tablename__ = "contracts"
    __table_args__ = (
        UniqueConstraint("symbol", "expiration", "strike", "right", name=UQ_CONTRACTS),
        CheckConstraint("char_length(symbol) > 0", name="ck_contracts_symbol"),
        CheckConstraint(RIGHT_SQL, name="ck_contracts_right"),
        CheckConstraint("strike > 0", name="ck_contracts_strike_positive"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False, comment=STRIKE_COMMENT)
    right: Mapped[str] = mapped_column(
        String(4),
        nullable=False,
        comment="v3 call or put. RIGHT is reserved in PostgreSQL; quote it in raw SQL.",
    )


class Quote(ThetaBase):
    """Intraday NBBO quote. Partitioned monthly on ``ts``."""

    __tablename__ = "quotes"
    __table_args__ = (
        CheckConstraint("interval_s > 0", name="ck_quotes_interval_s"),
        CheckConstraint(QUOTES_NONEGATIVE_SQL, name="ck_quotes_nonnegative"),
        {"postgresql_partition_by": "RANGE (ts)", "comment": QUOTES_COMMENT},
    )

    contract_id: Mapped[int] = _contract_id("quotes")
    interval_s: Mapped[int] = _interval()
    ts: Mapped[datetime] = _ts()
    bid_size: Mapped[int] = mapped_column(Integer, nullable=False)
    bid_exchange: Mapped[int] = mapped_column(Integer, nullable=False)
    bid: Mapped[Decimal] = _price()
    bid_condition: Mapped[int] = mapped_column(Integer, nullable=False)
    ask_size: Mapped[int] = mapped_column(Integer, nullable=False)
    ask_exchange: Mapped[int] = mapped_column(Integer, nullable=False)
    ask: Mapped[Decimal] = _price()
    ask_condition: Mapped[int] = mapped_column(Integer, nullable=False)


class OHLC(ThetaBase):
    """Intraday OHLC bar, including the v3 ``vwap`` field."""

    __tablename__ = "ohlc"
    __table_args__ = (
        CheckConstraint("interval_s > 0", name="ck_ohlc_interval_s"),
        CheckConstraint(OHLC_NONEGATIVE_SQL, name="ck_ohlc_nonnegative"),
        {"comment": OHLC_COMMENT},
    )

    contract_id: Mapped[int] = _contract_id("ohlc")
    interval_s: Mapped[int] = _interval()
    ts: Mapped[datetime] = _ts()
    open: Mapped[Decimal] = _price()
    high: Mapped[Decimal] = _price()
    low: Mapped[Decimal] = _price()
    close: Mapped[Decimal] = _price()
    volume: Mapped[int] = _whole()
    count: Mapped[int] = _whole()
    vwap: Mapped[Decimal] = _price()


class GreeksFirstOrder(ThetaBase):
    """Intraday first-order greeks and IV. Standard subscription only."""

    __tablename__ = "greeks_first_order"
    __table_args__ = (
        CheckConstraint("interval_s > 0", name="ck_greeks_first_order_interval_s"),
        CheckConstraint(GREEKS_NONEGATIVE_SQL, name="ck_greeks_first_order_nonnegative"),
        {"comment": GREEKS_COMMENT},
    )

    contract_id: Mapped[int] = _contract_id("greeks_first_order")
    interval_s: Mapped[int] = _interval()
    ts: Mapped[datetime] = _ts()
    bid: Mapped[Decimal] = _price()
    ask: Mapped[Decimal] = _price()
    delta: Mapped[Decimal] = _price()
    theta: Mapped[Decimal] = _price()
    vega: Mapped[Decimal] = _price()
    rho: Mapped[Decimal] = _price()
    epsilon: Mapped[Decimal] = _price()
    lambda_: Mapped[Decimal] = mapped_column("lambda", Numeric(18, 8), nullable=False)
    implied_vol: Mapped[Decimal] = _price()
    iv_error: Mapped[Decimal] = _price()
    underlying_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, comment=TS_COMMENT)
    underlying_price: Mapped[Decimal] = mapped_column(
        Numeric(18, 8),
        nullable=False,
        comment=UNDERLYING_PRICE_COMMENT,
    )


class OpenInterest(ThetaBase):
    """Daily open interest. ``date`` is the report's Eastern session date."""

    __tablename__ = "open_interest"
    __table_args__ = (
        CheckConstraint(OPEN_INTEREST_NONEGATIVE_SQL, name="ck_open_interest_nonnegative"),
        {"comment": OPEN_INTEREST_COMMENT},
    )

    contract_id: Mapped[int] = _contract_id("open_interest")
    session_date: Mapped[date] = mapped_column("date", Date, primary_key=True, comment=SESSION_DATE_COMMENT)
    open_interest: Mapped[int] = _whole()
    reported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, comment=TS_COMMENT)


class EOD(ThetaBase):
    """Daily v3 EOD report. ``date`` is the session, ``created`` is the report time."""

    __tablename__ = "eod"
    __table_args__ = (
        CheckConstraint(EOD_NONEGATIVE_SQL, name="ck_eod_nonnegative"),
        {"comment": EOD_COMMENT},
    )

    contract_id: Mapped[int] = _contract_id("eod")
    session_date: Mapped[date] = mapped_column("date", Date, primary_key=True, comment=SESSION_DATE_COMMENT)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, comment=TS_COMMENT)
    last_trade: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, comment=TS_COMMENT)
    open: Mapped[Decimal] = _price()
    high: Mapped[Decimal] = _price()
    low: Mapped[Decimal] = _price()
    close: Mapped[Decimal] = _price()
    volume: Mapped[int] = _whole()
    count: Mapped[int] = _whole()
    bid_size: Mapped[int] = mapped_column(Integer, nullable=False)
    bid_exchange: Mapped[int] = mapped_column(Integer, nullable=False)
    bid: Mapped[Decimal] = _price()
    bid_condition: Mapped[int] = mapped_column(Integer, nullable=False)
    ask_size: Mapped[int] = mapped_column(Integer, nullable=False)
    ask_exchange: Mapped[int] = mapped_column(Integer, nullable=False)
    ask: Mapped[Decimal] = _price()
    ask_condition: Mapped[int] = mapped_column(Integer, nullable=False)


class Progress(ThetaBase):
    """Resumable backfill progress.

    Status is ``pending``, ``running``, ``done``, ``failed``, or ``empty``.
    ``interval_s`` is the intraday bar length for quotes, OHLC, and first-order
    greeks. Daily open interest and EOD leave it null. The partial index on
    pending rows is what workers use for ``FOR UPDATE SKIP LOCKED`` claims.
    """

    __tablename__ = "progress"
    __table_args__ = (
        CheckConstraint("char_length(dataset) > 0 AND char_length(status) > 0", name="ck_progress_labels"),
        CheckConstraint("row_count >= 0", name="ck_progress_row_count"),
        CheckConstraint(PROGRESS_STATUS_SQL, name="ck_progress_status"),
        CheckConstraint(PROGRESS_INTERVAL_SQL, name="ck_progress_interval_s"),
        Index(
            "ix_progress_claim",
            "date",
            "expiration",
            "dataset",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "ix_progress_running",
            "updated_at",
            postgresql_where=text("status = 'running'"),
        ),
        {"comment": PROGRESS_COMMENT},
    )

    dataset: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_date: Mapped[date] = mapped_column("date", Date, primary_key=True)
    expiration: Mapped[date] = mapped_column(Date, primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    interval_s: Mapped[int | None] = mapped_column(
        SmallInteger,
        nullable=True,
        comment=PROGRESS_INTERVAL_COMMENT,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
