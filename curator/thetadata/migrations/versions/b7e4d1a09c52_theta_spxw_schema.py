"""Theta SPXW Options Standard schema.

Creates schema ``theta`` in the markets database and the contracts, quotes,
ohlc, greeks_first_order, open_interest, eod, and progress tables. This revision
is the head of the theta Alembic chain. It does not touch the main chain
(head 34551fa734e8) or that chain's version table.

v3 timestamps are zone-less America/New_York wall times, including DST. Loaders
parse them with ``curator.thetadata.time.parse_thetadata_timestamp`` and store
timestamptz. ``underlying_price`` is the price ThetaData used for greeks and IV.
It is not the SPX spot source.

``quotes`` is partitioned by month on ``ts``. Bounds are America/New_York
midnights. The unique key ``(contract_id, ts)`` includes the partition key,
which PostgreSQL requires. ``ohlc`` and ``greeks_first_order`` use the same
unique key and are not partitioned. ``interval_s`` is 60 for calendar DTE 0-5
and 900 otherwise, so one contract-session has one bar size and the key holds.

ON CONFLICT targets are ``CONFLICT_COLUMNS`` in ``curator.thetadata.models.theta``.
Strike is numeric(12, 3), not float. The initial quote partitions cover
2022-05 through 2027-12. Add later months with ``add_partitions_ahead`` instead
of editing this revision.

Revision ID: b7e4d1a09c52
Revises:
Create Date: 2026-11-23

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from curator.thetadata.config import INITIAL_PARTITION_START, INITIAL_PARTITION_UNTIL, THETA_SCHEMA
from curator.thetadata.models.theta import (
    EOD_COMMENT,
    EOD_NONEGATIVE_SQL,
    GREEKS_COMMENT,
    GREEKS_NONEGATIVE_SQL,
    OHLC_COMMENT,
    OHLC_NONEGATIVE_SQL,
    OPEN_INTEREST_COMMENT,
    OPEN_INTEREST_NONEGATIVE_SQL,
    PROGRESS_COMMENT,
    QUOTES_COMMENT,
    QUOTES_NONEGATIVE_SQL,
    RIGHT_SQL,
    SCHEMA_COMMENT,
    SESSION_DATE_COMMENT,
    STRIKE_COMMENT,
    TS_COMMENT,
    UNDERLYING_PRICE_COMMENT,
    UQ_CONTRACTS,
)
from curator.thetadata.partitions import monthly_partition_statements, quote_ident, quote_literal

revision: str = "b7e4d1a09c52"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCHEMA = quote_ident(THETA_SCHEMA)


def upgrade() -> None:
    """Create the theta schema, tables, and the initial quote partitions.

    Returns:
        None.
    """
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}")
    op.execute(f"COMMENT ON SCHEMA {_SCHEMA} IS {quote_literal(SCHEMA_COMMENT)}")
    _create_contracts()
    _create_quotes()
    _create_ohlc()
    _create_greeks()
    _create_open_interest()
    _create_eod()
    _create_progress()
    for statement in monthly_partition_statements(INITIAL_PARTITION_START, INITIAL_PARTITION_UNTIL):
        op.execute(statement)


def downgrade() -> None:
    """Drop theta data tables. The schema and version table stay.

    Returns:
        None.
    """
    for name in ("progress", "eod", "open_interest", "greeks_first_order", "ohlc", "quotes", "contracts"):
        op.drop_table(name, schema=_SCHEMA)


def _money(name: str, *, comment: str | None = None) -> sa.Column:
    """Return a non-null numeric price column.

    Args:
        name: Column name.
        comment: Optional database comment.

    Returns:
        A Numeric(18, 8) column.
    """
    return sa.Column(name, sa.Numeric(18, 8), nullable=False, comment=comment)


def _ts(name: str, *, nullable: bool = False) -> sa.Column:
    """Return a timestamptz column documented as an Eastern v3 timestamp.

    Args:
        name: Column name.
        nullable: Whether the column accepts null.

    Returns:
        A timezone-aware timestamp column.
    """
    return sa.Column(name, sa.DateTime(timezone=True), nullable=nullable, comment=TS_COMMENT)


def _contract_fk(table: str) -> sa.ForeignKeyConstraint:
    """Return the foreign key to ``contracts``.

    Args:
        table: Child table name.

    Returns:
        A named foreign-key constraint on ``contract_id``.
    """
    return sa.ForeignKeyConstraint(
        ["contract_id"],
        [f"{_SCHEMA}.contracts.id"],
        name=f"fk_{table}_contract_id",
    )


def _create_contracts() -> None:
    """Create ``theta.contracts``.

    Returns:
        None.
    """
    op.create_table(
        "contracts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True, nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(12, 3), nullable=False, comment=STRIKE_COMMENT),
        sa.Column(
            "right",
            sa.String(4),
            nullable=False,
            comment="v3 call or put. RIGHT is reserved in PostgreSQL; quote it in raw SQL.",
        ),
        sa.UniqueConstraint("symbol", "expiration", "strike", "right", name=UQ_CONTRACTS),
        sa.CheckConstraint("char_length(symbol) > 0", name="ck_contracts_symbol"),
        sa.CheckConstraint(RIGHT_SQL, name="ck_contracts_right"),
        sa.CheckConstraint("strike > 0", name="ck_contracts_strike_positive"),
        schema=_SCHEMA,
    )


def _create_quotes() -> None:
    """Create partitioned ``theta.quotes``.

    Returns:
        None.
    """
    op.create_table(
        "quotes",
        sa.Column("contract_id", sa.BigInteger(), nullable=False),
        sa.Column("interval_s", sa.SmallInteger(), nullable=False),
        _ts("ts"),
        sa.Column("bid_size", sa.Integer(), nullable=False),
        sa.Column("bid_exchange", sa.Integer(), nullable=False),
        _money("bid"),
        sa.Column("bid_condition", sa.Integer(), nullable=False),
        sa.Column("ask_size", sa.Integer(), nullable=False),
        sa.Column("ask_exchange", sa.Integer(), nullable=False),
        _money("ask"),
        sa.Column("ask_condition", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("contract_id", "ts", name="pk_quotes"),
        _contract_fk("quotes"),
        sa.CheckConstraint("interval_s > 0", name="ck_quotes_interval_s"),
        sa.CheckConstraint(QUOTES_NONEGATIVE_SQL, name="ck_quotes_nonnegative"),
        schema=_SCHEMA,
        postgresql_partition_by="RANGE (ts)",
        comment=QUOTES_COMMENT,
    )


def _create_ohlc() -> None:
    """Create ``theta.ohlc``.

    Returns:
        None.
    """
    op.create_table(
        "ohlc",
        sa.Column("contract_id", sa.BigInteger(), nullable=False),
        sa.Column("interval_s", sa.SmallInteger(), nullable=False),
        _ts("ts"),
        _money("open"),
        _money("high"),
        _money("low"),
        _money("close"),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("count", sa.BigInteger(), nullable=False),
        _money("vwap"),
        sa.PrimaryKeyConstraint("contract_id", "ts", name="pk_ohlc"),
        _contract_fk("ohlc"),
        sa.CheckConstraint("interval_s > 0", name="ck_ohlc_interval_s"),
        sa.CheckConstraint(OHLC_NONEGATIVE_SQL, name="ck_ohlc_nonnegative"),
        schema=_SCHEMA,
        comment=OHLC_COMMENT,
    )


def _create_greeks() -> None:
    """Create ``theta.greeks_first_order``.

    Returns:
        None.
    """
    op.create_table(
        "greeks_first_order",
        sa.Column("contract_id", sa.BigInteger(), nullable=False),
        sa.Column("interval_s", sa.SmallInteger(), nullable=False),
        _ts("ts"),
        _money("bid"),
        _money("ask"),
        _money("delta"),
        _money("theta"),
        _money("vega"),
        _money("rho"),
        _money("epsilon"),
        sa.Column("lambda", sa.Numeric(18, 8), nullable=False),
        _money("implied_vol"),
        _money("iv_error"),
        _ts("underlying_timestamp"),
        sa.Column("underlying_price", sa.Numeric(18, 8), nullable=False, comment=UNDERLYING_PRICE_COMMENT),
        sa.PrimaryKeyConstraint("contract_id", "ts", name="pk_greeks_first_order"),
        _contract_fk("greeks_first_order"),
        sa.CheckConstraint("interval_s > 0", name="ck_greeks_first_order_interval_s"),
        sa.CheckConstraint(GREEKS_NONEGATIVE_SQL, name="ck_greeks_first_order_nonnegative"),
        schema=_SCHEMA,
        comment=GREEKS_COMMENT,
    )


def _create_open_interest() -> None:
    """Create ``theta.open_interest``.

    Returns:
        None.
    """
    op.create_table(
        "open_interest",
        sa.Column("contract_id", sa.BigInteger(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False, comment=SESSION_DATE_COMMENT),
        sa.Column("open_interest", sa.BigInteger(), nullable=False),
        _ts("reported_at"),
        sa.PrimaryKeyConstraint("contract_id", "date", name="pk_open_interest"),
        _contract_fk("open_interest"),
        sa.CheckConstraint(OPEN_INTEREST_NONEGATIVE_SQL, name="ck_open_interest_nonnegative"),
        schema=_SCHEMA,
        comment=OPEN_INTEREST_COMMENT,
    )


def _create_eod() -> None:
    """Create ``theta.eod``.

    Returns:
        None.
    """
    op.create_table(
        "eod",
        sa.Column("contract_id", sa.BigInteger(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False, comment=SESSION_DATE_COMMENT),
        _ts("created"),
        _ts("last_trade", nullable=True),
        _money("open"),
        _money("high"),
        _money("low"),
        _money("close"),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("count", sa.BigInteger(), nullable=False),
        sa.Column("bid_size", sa.Integer(), nullable=False),
        sa.Column("bid_exchange", sa.Integer(), nullable=False),
        _money("bid"),
        sa.Column("bid_condition", sa.Integer(), nullable=False),
        sa.Column("ask_size", sa.Integer(), nullable=False),
        sa.Column("ask_exchange", sa.Integer(), nullable=False),
        _money("ask"),
        sa.Column("ask_condition", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("contract_id", "date", name="pk_eod"),
        _contract_fk("eod"),
        sa.CheckConstraint(EOD_NONEGATIVE_SQL, name="ck_eod_nonnegative"),
        schema=_SCHEMA,
        comment=EOD_COMMENT,
    )


def _create_progress() -> None:
    """Create ``theta.progress``.

    Returns:
        None.
    """
    op.create_table(
        "progress",
        sa.Column("dataset", sa.String(64), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("dataset", "date", "expiration", name="pk_progress"),
        sa.CheckConstraint("char_length(dataset) > 0 AND char_length(status) > 0", name="ck_progress_labels"),
        sa.CheckConstraint("row_count >= 0", name="ck_progress_row_count"),
        schema=_SCHEMA,
        comment=PROGRESS_COMMENT,
    )
