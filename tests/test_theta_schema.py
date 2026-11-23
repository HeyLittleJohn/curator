"""Offline tests for the theta schema, timestamps, and Alembic chain.

These tests do not open a database connection.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import DateTime, Float, UniqueConstraint
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata.config import (
    INITIAL_PARTITION_START,
    INITIAL_PARTITION_UNTIL,
    INTERVAL_1M_SECONDS,
    INTERVAL_15M_SECONDS,
    THETA_SCHEMA,
)
from curator.thetadata.identity import (
    calendar_dte,
    interval_seconds_for_dte,
    normalize_symbol,
    parse_right,
    parse_strike,
)
from curator.thetadata.migrate import render_migration_sql, theta_head, theta_script_location
from curator.thetadata.models.theta import CONFLICT_COLUMNS, ThetaBase
from curator.thetadata.partitions import iter_monthly_partitions, monthly_partition_statements
from curator.thetadata.secrets import scrub_secrets
from curator.thetadata.time import parse_thetadata_timestamp

ROOT = Path(__file__).resolve().parents[1]
MAIN_HEAD = "34551fa734e8"
FORBIDDEN_COLUMNS = {
    "gamma",
    "vanna",
    "charm",
    "vomma",
    "veta",
    "vera",
    "color",
    "zomma",
    "speed",
    "ultima",
    "dual_delta",
    "dual_gamma",
}


def test_schema_name_is_one_constant() -> None:
    """Every theta table uses the single schema constant."""
    assert THETA_SCHEMA == "theta"
    for table in ThetaBase.metadata.tables.values():
        assert table.schema == THETA_SCHEMA


def test_conflict_keys_match_unique_indexes() -> None:
    """ON CONFLICT columns are the primary key, except the contracts natural key."""
    for table_name, columns in CONFLICT_COLUMNS.items():
        table = ThetaBase.metadata.tables[f"{THETA_SCHEMA}.{table_name}"]
        if table_name == "contracts":
            unique = next(constraint for constraint in table.constraints if isinstance(constraint, UniqueConstraint))
            assert tuple(unique.columns.keys()) == columns
        else:
            assert tuple(table.primary_key.columns.keys()) == columns
        assert "interval_s" not in columns


def test_quotes_are_the_only_partitioned_table() -> None:
    """Quotes partition on ts, and that column is part of the primary key."""
    quotes = ThetaBase.metadata.tables[f"{THETA_SCHEMA}.quotes"]
    assert quotes.dialect_options["postgresql"]["partition_by"] == "RANGE (ts)"
    assert "ts" in quotes.primary_key.columns
    for table_name in ("ohlc", "greeks_first_order", "open_interest", "eod", "contracts", "progress"):
        table = ThetaBase.metadata.tables[f"{THETA_SCHEMA}.{table_name}"]
        assert not table.dialect_options["postgresql"].get("partition_by")


def test_types_are_exact_and_timezone_aware() -> None:
    """Strikes and prices are numeric, and every timestamp is timestamptz."""
    for table in ThetaBase.metadata.tables.values():
        assert FORBIDDEN_COLUMNS.isdisjoint(table.columns.keys())
        for column in table.columns:
            assert not isinstance(column.type, Float)
            if isinstance(column.type, DateTime):
                assert column.type.timezone is True
    strike = ThetaBase.metadata.tables[f"{THETA_SCHEMA}.contracts"].columns["strike"]
    assert "NUMERIC(12, 3)" in str(strike.type).upper()
    assert "lambda" in ThetaBase.metadata.tables[f"{THETA_SCHEMA}.greeks_first_order"].columns


def test_main_alembic_head_is_unchanged() -> None:
    """The repository-root chain still ends at the silver MBO index revision."""
    script = ScriptDirectory.from_config(Config(str(ROOT / "alembic.ini")))
    assert script.get_current_head() == MAIN_HEAD
    assert theta_head() != MAIN_HEAD
    assert theta_script_location().name == "migrations"
    assert "thetadata" in str(theta_script_location())


def test_offline_upgrade_sql_builds_theta_chain() -> None:
    """``upgrade --sql`` describes the theta schema and does not connect."""
    sql = render_migration_sql("upgrade", "head")
    normalized = sql.replace('"', "").lower()
    assert "://" not in sql
    assert "password" not in sql.lower()
    assert normalized.index(f"create schema if not exists {THETA_SCHEMA}") < normalized.index("alembic_version_theta")
    assert f"{THETA_SCHEMA}.alembic_version_theta" in normalized
    assert "public.alembic_version " not in normalized
    assert MAIN_HEAD not in sql
    assert "partition by range (ts)" in normalized
    assert "quotes_2022_05" in normalized
    assert "quotes_2027_12" in normalized
    assert "america/new_york" in normalized
    assert "not the spx spot source" in normalized
    for table_name in CONFLICT_COLUMNS:
        assert f"create table {THETA_SCHEMA}.{table_name}" in normalized
        table = ThetaBase.metadata.tables[f"{THETA_SCHEMA}.{table_name}"]
        rest = normalized.split(f"create table {THETA_SCHEMA}.{table_name}", 1)[1]
        block = rest.split("create table", 1)[0]
        for column in table.columns:
            if column.name in block:
                continue
            # A later theta revision may add the column after the version table
            # is created, which ends the original CREATE TABLE chunk.
            assert f"add column {column.name}" in rest
    assert "gamma" not in normalized
    assert "vanna" not in normalized
    downgrade = render_migration_sql("downgrade", "base").replace('"', "").lower()
    assert downgrade.index("drop table theta.quotes") < downgrade.index("drop table theta.contracts")


def test_cli_db_help_and_offline_sql() -> None:
    """``curator theta db`` lists the migration commands and can render SQL."""
    help_result = CliRunner().invoke(app, ["theta", "db", "--help"])
    assert help_result.exit_code == 0
    assert "upgrade" in help_result.stdout
    assert "downgrade" in help_result.stdout
    assert "current" in help_result.stdout
    sql_result = CliRunner().invoke(app, ["theta", "db", "upgrade", "--sql"])
    assert sql_result.exit_code == 0, sql_result.output
    assert "CREATE SCHEMA" in sql_result.stdout
    assert "://" not in sql_result.stdout
    assert "://" not in (sql_result.stderr or "")


def test_parse_thetadata_timestamp_is_dst_aware() -> None:
    """July is EDT and January is EST. Offsets and missing times are rejected."""
    july = parse_thetadata_timestamp("2024-07-01T09:30:00.000")
    january = parse_thetadata_timestamp("2024-01-02 09:30:00")
    assert july.utcoffset() == timedelta(hours=-4)
    assert january.utcoffset() == timedelta(hours=-5)
    assert july.astimezone(timezone.utc) == datetime(2024, 7, 1, 13, 30, tzinfo=timezone.utc)
    assert january.astimezone(timezone.utc) == datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc)
    overlap = parse_thetadata_timestamp("2024-11-03T01:30:00")
    assert overlap.utcoffset() == timedelta(hours=-4)
    for rejected in ("2024-07-01T09:30:00Z", "2024-07-01T09:30:00-04:00", "2024-07-01", "2024-03-10T02:30:00"):
        try:
            parse_thetadata_timestamp(rejected)
        except ValueError:
            continue
        raise AssertionError(rejected)


def test_strike_right_and_interval_rules() -> None:
    """Strikes stay exact, rights use v3 words, and DTE picks one interval."""
    assert parse_strike("4500.000") == Decimal("4500.000")
    assert parse_strike(4500) == Decimal("4500.000")
    assert normalize_symbol(" spxw ") == "SPXW"
    assert parse_right("C") == "call"
    assert parse_right("Put") == "put"
    expiration = date(2022, 5, 20)
    assert calendar_dte(date(2022, 5, 20), expiration) == 0
    assert calendar_dte(date(2022, 5, 15), expiration) == 5
    assert interval_seconds_for_dte(0) == INTERVAL_1M_SECONDS
    assert interval_seconds_for_dte(5) == INTERVAL_1M_SECONDS
    assert interval_seconds_for_dte(6) == INTERVAL_15M_SECONDS
    assert interval_seconds_for_dte(-1) == INTERVAL_15M_SECONDS
    for rejected in (1.5, True, None):
        try:
            parse_strike(rejected)
        except TypeError:
            continue
        raise AssertionError(type(rejected).__name__)


def test_partition_bounds_follow_eastern_time() -> None:
    """May 2022 is EDT, December 2022 is EST, and the initial window is fixed."""
    may = monthly_partition_statements(date(2022, 5, 1), date(2022, 6, 1))
    assert may == [
        "CREATE TABLE IF NOT EXISTS theta.quotes_2022_05 "
        "PARTITION OF theta.quotes "
        "FOR VALUES FROM (TIMESTAMPTZ '2022-05-01 00:00:00-04:00') "
        "TO (TIMESTAMPTZ '2022-06-01 00:00:00-04:00')"
    ]
    december = monthly_partition_statements(date(2022, 12, 1), date(2023, 1, 1))
    assert "2022-12-01 00:00:00-05:00" in december[0]
    november = monthly_partition_statements(date(2022, 11, 1), date(2022, 12, 1))
    assert "2022-11-01 00:00:00-04:00" in november[0]
    initial = iter_monthly_partitions(INITIAL_PARTITION_START, INITIAL_PARTITION_UNTIL)
    assert initial[0][0] == "quotes_2022_05"
    assert initial[-1][0] == "quotes_2027_12"
    assert len(initial) == 68


def test_scrub_secrets_removes_urls_and_passwords() -> None:
    """Errors that echo a driver URL or a password assignment are redacted."""
    leaked = "could not connect to postgresql+psycopg://loader:s3cret@db.example/markets"
    assert "s3cret" not in scrub_secrets(leaked)
    assert "<database-url>" in scrub_secrets(leaked)
    assert "password=<redacted>" in scrub_secrets("password=s3cret")
