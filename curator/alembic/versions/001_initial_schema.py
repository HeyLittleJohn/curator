"""Initial schema with all tables.

Revision ID: 001_initial_schema
Revises: 
Create Date: 2026-01-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Equity OHLC
    op.create_table(
        "equity_ohlc",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("count", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_equity_ohlc_symbol", "equity_ohlc", ["symbol"])
    op.create_index("ix_equity_ohlc_symbol_timestamp", "equity_ohlc", ["symbol", "timestamp"], unique=True)

    # Equity Trade
    op.create_table(
        "equity_trade",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("condition", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.Integer(), nullable=True),
        sa.Column("ext_condition1", sa.Integer(), nullable=True),
        sa.Column("ext_condition2", sa.Integer(), nullable=True),
        sa.Column("ext_condition3", sa.Integer(), nullable=True),
        sa.Column("ext_condition4", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_equity_trade_symbol", "equity_trade", ["symbol"])
    op.create_index("ix_equity_trade_symbol_timestamp", "equity_trade", ["symbol", "timestamp"])
    op.create_index("ix_equity_trade_symbol_sequence", "equity_trade", ["symbol", "sequence"], unique=True)

    # Equity Quote
    op.create_table(
        "equity_quote",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bid", sa.Numeric(18, 8), nullable=False),
        sa.Column("bid_size", sa.BigInteger(), nullable=False),
        sa.Column("bid_exchange", sa.Integer(), nullable=True),
        sa.Column("bid_condition", sa.Integer(), nullable=True),
        sa.Column("ask", sa.Numeric(18, 8), nullable=False),
        sa.Column("ask_size", sa.BigInteger(), nullable=False),
        sa.Column("ask_exchange", sa.Integer(), nullable=True),
        sa.Column("ask_condition", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_equity_quote_symbol", "equity_quote", ["symbol"])
    op.create_index("ix_equity_quote_symbol_timestamp", "equity_quote", ["symbol", "timestamp"])

    # Equity EOD
    op.create_table(
        "equity_eod",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("trade_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("report_created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_trade", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("count", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("bid_size", sa.BigInteger(), nullable=True),
        sa.Column("ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("ask_size", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_equity_eod_symbol", "equity_eod", ["symbol"])
    op.create_index("ix_equity_eod_symbol_date", "equity_eod", ["symbol", "trade_date"], unique=True)

    # Option OHLC
    op.create_table(
        "option_ohlc",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(18, 4), nullable=False),
        sa.Column("right", sa.String(4), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("count", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_option_ohlc_symbol", "option_ohlc", ["symbol"])
    op.create_index("ix_option_ohlc_expiration", "option_ohlc", ["expiration"])
    op.create_index(
        "ix_option_ohlc_contract_timestamp",
        "option_ohlc",
        ["symbol", "expiration", "strike", "right", "timestamp"],
        unique=True,
    )

    # Option Trade
    op.create_table(
        "option_trade",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(18, 4), nullable=False),
        sa.Column("right", sa.String(4), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("condition", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.Integer(), nullable=True),
        sa.Column("ext_condition1", sa.Integer(), nullable=True),
        sa.Column("ext_condition2", sa.Integer(), nullable=True),
        sa.Column("ext_condition3", sa.Integer(), nullable=True),
        sa.Column("ext_condition4", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_option_trade_symbol", "option_trade", ["symbol"])
    op.create_index(
        "ix_option_trade_contract_timestamp",
        "option_trade",
        ["symbol", "expiration", "strike", "right", "timestamp"],
    )
    op.create_index(
        "ix_option_trade_contract_sequence",
        "option_trade",
        ["symbol", "expiration", "strike", "right", "sequence"],
        unique=True,
    )

    # Option Quote
    op.create_table(
        "option_quote",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(18, 4), nullable=False),
        sa.Column("right", sa.String(4), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bid", sa.Numeric(18, 8), nullable=False),
        sa.Column("bid_size", sa.BigInteger(), nullable=False),
        sa.Column("bid_exchange", sa.Integer(), nullable=True),
        sa.Column("bid_condition", sa.Integer(), nullable=True),
        sa.Column("ask", sa.Numeric(18, 8), nullable=False),
        sa.Column("ask_size", sa.BigInteger(), nullable=False),
        sa.Column("ask_exchange", sa.Integer(), nullable=True),
        sa.Column("ask_condition", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_option_quote_symbol", "option_quote", ["symbol"])
    op.create_index(
        "ix_option_quote_contract_timestamp",
        "option_quote",
        ["symbol", "expiration", "strike", "right", "timestamp"],
    )

    # Option Greeks
    op.create_table(
        "option_greeks",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(18, 4), nullable=False),
        sa.Column("right", sa.String(4), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("underlying_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("bid", sa.Numeric(18, 8), nullable=False),
        sa.Column("ask", sa.Numeric(18, 8), nullable=False),
        sa.Column("underlying_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("implied_vol", sa.Numeric(18, 8), nullable=True),
        sa.Column("iv_error", sa.Numeric(18, 8), nullable=True),
        # First-order Greeks
        sa.Column("delta", sa.Numeric(18, 8), nullable=True),
        sa.Column("gamma", sa.Numeric(18, 8), nullable=True),
        sa.Column("theta", sa.Numeric(18, 8), nullable=True),
        sa.Column("vega", sa.Numeric(18, 8), nullable=True),
        sa.Column("rho", sa.Numeric(18, 8), nullable=True),
        sa.Column("epsilon", sa.Numeric(18, 8), nullable=True),
        sa.Column("lambda_val", sa.Numeric(18, 8), nullable=True),
        # Second-order Greeks
        sa.Column("vanna", sa.Numeric(18, 8), nullable=True),
        sa.Column("charm", sa.Numeric(18, 8), nullable=True),
        sa.Column("vomma", sa.Numeric(18, 8), nullable=True),
        sa.Column("veta", sa.Numeric(18, 8), nullable=True),
        sa.Column("color", sa.Numeric(18, 8), nullable=True),
        sa.Column("zomma", sa.Numeric(18, 8), nullable=True),
        sa.Column("speed", sa.Numeric(18, 8), nullable=True),
        sa.Column("ultima", sa.Numeric(18, 8), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_option_greeks_symbol", "option_greeks", ["symbol"])
    op.create_index("ix_option_greeks_expiration", "option_greeks", ["expiration"])
    op.create_index(
        "ix_option_greeks_contract_timestamp",
        "option_greeks",
        ["symbol", "expiration", "strike", "right", "timestamp"],
    )

    # Option Trade Greeks
    op.create_table(
        "option_trade_greeks",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(18, 4), nullable=False),
        sa.Column("right", sa.String(4), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("condition", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.Integer(), nullable=True),
        sa.Column("underlying_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("underlying_price", sa.Numeric(18, 8), nullable=True),
        sa.Column("implied_vol", sa.Numeric(18, 8), nullable=True),
        sa.Column("iv_error", sa.Numeric(18, 8), nullable=True),
        # First-order Greeks
        sa.Column("delta", sa.Numeric(18, 8), nullable=True),
        sa.Column("gamma", sa.Numeric(18, 8), nullable=True),
        sa.Column("theta", sa.Numeric(18, 8), nullable=True),
        sa.Column("vega", sa.Numeric(18, 8), nullable=True),
        sa.Column("rho", sa.Numeric(18, 8), nullable=True),
        sa.Column("epsilon", sa.Numeric(18, 8), nullable=True),
        sa.Column("lambda_val", sa.Numeric(18, 8), nullable=True),
        # Second-order Greeks
        sa.Column("vanna", sa.Numeric(18, 8), nullable=True),
        sa.Column("charm", sa.Numeric(18, 8), nullable=True),
        sa.Column("vomma", sa.Numeric(18, 8), nullable=True),
        sa.Column("veta", sa.Numeric(18, 8), nullable=True),
        sa.Column("color", sa.Numeric(18, 8), nullable=True),
        sa.Column("zomma", sa.Numeric(18, 8), nullable=True),
        sa.Column("speed", sa.Numeric(18, 8), nullable=True),
        sa.Column("ultima", sa.Numeric(18, 8), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_option_trade_greeks_symbol", "option_trade_greeks", ["symbol"])
    op.create_index(
        "ix_option_trade_greeks_contract_seq",
        "option_trade_greeks",
        ["symbol", "expiration", "strike", "right", "sequence"],
        unique=True,
    )

    # Option Open Interest
    op.create_table(
        "option_open_interest",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(18, 4), nullable=False),
        sa.Column("right", sa.String(4), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open_interest", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_option_oi_symbol", "option_open_interest", ["symbol"])
    op.create_index(
        "ix_option_oi_contract_date",
        "option_open_interest",
        ["symbol", "expiration", "strike", "right", "trade_date"],
        unique=True,
    )

    # Option EOD
    op.create_table(
        "option_eod",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("expiration", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(18, 4), nullable=False),
        sa.Column("right", sa.String(4), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("report_created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_trade", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("count", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("open_interest", sa.BigInteger(), nullable=True),
        sa.Column("bid", sa.Numeric(18, 8), nullable=True),
        sa.Column("ask", sa.Numeric(18, 8), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_option_eod_symbol", "option_eod", ["symbol"])
    op.create_index(
        "ix_option_eod_contract_date",
        "option_eod",
        ["symbol", "expiration", "strike", "right", "trade_date"],
        unique=True,
    )

    # Index OHLC
    op.create_table(
        "index_ohlc",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("count", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_index_ohlc_symbol", "index_ohlc", ["symbol"])
    op.create_index("ix_index_ohlc_symbol_timestamp", "index_ohlc", ["symbol", "timestamp"], unique=True)

    # Index Price
    op.create_table(
        "index_price",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_index_price_symbol", "index_price", ["symbol"])
    op.create_index("ix_index_price_symbol_timestamp", "index_price", ["symbol", "timestamp"])

    # Index EOD
    op.create_table(
        "index_eod",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("report_created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_trade", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("count", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_index_eod_symbol", "index_eod", ["symbol"])
    op.create_index("ix_index_eod_symbol_date", "index_eod", ["symbol", "trade_date"], unique=True)


def downgrade() -> None:
    op.drop_table("index_eod")
    op.drop_table("index_price")
    op.drop_table("index_ohlc")
    op.drop_table("option_eod")
    op.drop_table("option_open_interest")
    op.drop_table("option_trade_greeks")
    op.drop_table("option_greeks")
    op.drop_table("option_quote")
    op.drop_table("option_trade")
    op.drop_table("option_ohlc")
    op.drop_table("equity_eod")
    op.drop_table("equity_quote")
    op.drop_table("equity_trade")
    op.drop_table("equity_ohlc")
