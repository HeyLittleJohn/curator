"""Scratch-Postgres round trip for the theta chain.

Opt in with ``THETADATA_TEST_DATABASE_URL`` and ``pytest -m postgres``.
CI does not set that variable and deselects this marker. Rows are synthetic.
The URL is never printed.
"""

import os
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError
from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata.migrate import theta_head, to_sync_database_url
from curator.thetadata.partitions import add_partitions_ahead
from curator.thetadata.secrets import scrub_secrets
from curator.thetadata.time import parse_thetadata_timestamp

pytestmark = pytest.mark.postgres


def _require_database() -> None:
    """Skip unless a scratch database URL is configured.

    Returns:
        None.

    Raises:
        pytest.skip.Exception: When the variable is unset.
    """
    if not os.environ.get("THETADATA_TEST_DATABASE_URL", "").strip():
        pytest.skip("THETADATA_TEST_DATABASE_URL is not set")


def _engine() -> Engine:
    """Open a SQLAlchemy engine without logging the URL.

    Returns:
        An engine with parameters hidden.

    Raises:
        pytest.fail.Exception: If connecting fails. The message is scrubbed.
    """
    _require_database()
    url = to_sync_database_url(os.environ["THETADATA_TEST_DATABASE_URL"].strip())
    try:
        engine = create_engine(url, pool_pre_ping=True, echo=False, hide_parameters=True)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except Exception as exc:
        pytest.fail(scrub_secrets(str(exc)))


def _cli(*args: str) -> str:
    """Run a theta db command and return stdout.

    Args:
        args: Arguments after ``theta db``.

    Returns:
        Command stdout.

    Raises:
        AssertionError: If the command fails or prints a URL. The failure
            text is scrubbed.
    """
    _require_database()
    env = os.environ.copy()
    env["THETADATA_DATABASE_URL"] = env["THETADATA_TEST_DATABASE_URL"]
    result = CliRunner().invoke(app, ["theta", "db", *args], env=env)
    combined = f"{result.stdout}\n{result.stderr or ''}"
    lowered = combined.lower()
    if "postgresql://" in lowered or "postgres://" in lowered or "postgresql+" in lowered:
        raise AssertionError("command printed a database URL")
    if result.exit_code != 0:
        raise AssertionError(scrub_secrets(combined))
    return result.stdout


def _reset(connection: Connection) -> None:
    """Drop a previous theta schema on the scratch database.

    Args:
        connection: Open connection. This commits.

    Returns:
        None.
    """
    connection.execute(text("DROP SCHEMA IF EXISTS theta CASCADE"))
    connection.commit()


def test_upgrade_downgrade_upgrade() -> None:
    """upgrade, downgrade base, and upgrade again on a scratch database."""
    _require_database()
    engine = _engine()
    head = theta_head()
    try:
        with engine.connect() as connection:
            _reset(connection)
        _cli("upgrade")
        with engine.connect() as connection:
            _assert_catalog(connection)
            _assert_synthetic_rows(connection)
            connection.commit()
        assert head in _cli("current")
        _cli("upgrade")
        with engine.connect() as connection:
            names = add_partitions_ahead(connection, months=1, today=date(2028, 6, 15))
            connection.commit()
            assert "quotes_2028_06" in names
            assert "quotes_2028_07" in names
            found = connection.execute(text("SELECT 1 FROM pg_class WHERE relname = 'quotes_2028_06'")).scalar_one()
            assert found == 1
        _cli("downgrade", "base")
        with engine.connect() as connection:
            tables = _table_names(connection)
            assert "contracts" not in tables
            assert "quotes" not in tables
        _cli("upgrade")
        with engine.connect() as connection:
            assert "contracts" in _table_names(connection)
            assert head in _cli("current")
    finally:
        with engine.connect() as connection:
            _reset(connection)
        engine.dispose()


def _table_names(connection: Connection) -> set[str]:
    """Return table names in schema theta.

    Args:
        connection: Open connection.

    Returns:
        Unqualified table names.
    """
    rows = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'theta'"))
    return {row[0] for row in rows}


def _assert_catalog(connection: Connection) -> None:
    """Check tables, the version table, strike scale, and timestamptz.

    Args:
        connection: Open connection after upgrade.

    Returns:
        None.
    """
    tables = _table_names(connection)
    expected = {
        "contracts",
        "quotes",
        "ohlc",
        "greeks_first_order",
        "open_interest",
        "eod",
        "progress",
        "alembic_version_theta",
    }
    assert expected <= tables
    version_tables = connection.execute(
        text(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_name IN ('alembic_version', 'alembic_version_theta')"
        )
    ).all()
    assert version_tables == [("theta", "alembic_version_theta")]
    strike = connection.execute(
        text(
            "SELECT data_type, numeric_scale FROM information_schema.columns "
            "WHERE table_schema = 'theta' AND table_name = 'contracts' AND column_name = 'strike'"
        )
    ).one()
    assert strike == ("numeric", 3)
    timestamps = connection.execute(
        text(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'theta' AND data_type = 'timestamp without time zone'"
        )
    ).all()
    assert timestamps == []
    partition = connection.execute(
        text("SELECT relname FROM pg_class WHERE relname = 'quotes_2022_05' AND relkind = 'r'")
    ).scalar_one()
    assert partition == "quotes_2022_05"


def _assert_synthetic_rows(connection: Connection) -> None:
    """Insert synthetic rows and check uniqueness, time zones, and partitions.

    Args:
        connection: Open connection. The caller commits.

    Returns:
        None.
    """
    contract_id = connection.execute(
        text(
            'INSERT INTO theta.contracts (symbol, expiration, strike, "right") '
            "VALUES (:symbol, :expiration, :strike, :right) RETURNING id"
        ),
        {"symbol": "SPXW", "expiration": date(2022, 5, 20), "strike": Decimal("4500.000"), "right": "call"},
    ).scalar_one()
    summer = parse_thetadata_timestamp("2022-07-01T09:30:00")
    winter = parse_thetadata_timestamp("2023-01-03T09:30:00")
    connection.execute(
        text(
            "INSERT INTO theta.quotes "
            "(contract_id, interval_s, ts, bid_size, bid_exchange, bid, bid_condition, "
            "ask_size, ask_exchange, ask, ask_condition) "
            "VALUES (:contract_id, 60, :ts, 10, 5, 1.25, 0, 12, 5, 1.35, 0)"
        ),
        {"contract_id": contract_id, "ts": summer},
    )
    stored = connection.execute(
        text("SELECT ts, tableoid::regclass::text FROM theta.quotes WHERE contract_id = :contract_id"),
        {"contract_id": contract_id},
    ).one()
    assert stored[0] == summer
    assert stored[1] == "theta.quotes_2022_07"
    with pytest.raises(IntegrityError):
        with connection.begin_nested():
            connection.execute(
                text(
                    "INSERT INTO theta.quotes "
                    "(contract_id, interval_s, ts, bid_size, bid_exchange, bid, bid_condition, "
                    "ask_size, ask_exchange, ask, ask_condition) "
                    "VALUES (:contract_id, 900, :ts, 1, 5, 1, 0, 1, 5, 2, 0)"
                ),
                {"contract_id": contract_id, "ts": summer},
            )
    connection.execute(
        text(
            "INSERT INTO theta.ohlc "
            "(contract_id, interval_s, ts, open, high, low, close, volume, count, vwap) "
            "VALUES (:contract_id, 60, :ts, 1, 2, 0.5, 1.5, 3, 1, 1.4)"
        ),
        {"contract_id": contract_id, "ts": winter},
    )
    connection.execute(
        text(
            "INSERT INTO theta.greeks_first_order "
            '(contract_id, interval_s, ts, bid, ask, delta, theta, vega, rho, epsilon, "lambda", '
            "implied_vol, iv_error, underlying_timestamp, underlying_price) "
            "VALUES (:contract_id, 60, :ts, 1.2, 1.3, 0.5, -0.1, 0.2, 0.01, -0.02, 12.5, "
            "0.2, 0.01, :underlying_timestamp, 4500)"
        ),
        {"contract_id": contract_id, "ts": winter, "underlying_timestamp": winter},
    )
    connection.execute(
        text(
            'INSERT INTO theta.open_interest (contract_id, "date", open_interest, reported_at) '
            "VALUES (:contract_id, :session_date, 10, :reported_at)"
        ),
        {
            "contract_id": contract_id,
            "session_date": date(2022, 7, 1),
            "reported_at": parse_thetadata_timestamp("2022-07-01T06:30:00"),
        },
    )
    connection.execute(
        text(
            "INSERT INTO theta.eod "
            '(contract_id, "date", created, last_trade, open, high, low, close, volume, count, '
            "bid_size, bid_exchange, bid, bid_condition, ask_size, ask_exchange, ask, ask_condition) "
            "VALUES (:contract_id, :session_date, :created, :last_trade, 1, 2, 0.5, 1.5, 4, 2, "
            "10, 5, 1.2, 0, 11, 5, 1.3, 0)"
        ),
        {
            "contract_id": contract_id,
            "session_date": date(2022, 7, 1),
            "created": parse_thetadata_timestamp("2022-07-01T17:15:00"),
            "last_trade": summer,
        },
    )
    connection.execute(
        text(
            'INSERT INTO theta.progress (dataset, "date", expiration, status, row_count) '
            "VALUES ('quotes', :session_date, :expiration, 'done', 1)"
        ),
        {"session_date": date(2022, 7, 1), "expiration": date(2022, 5, 20)},
    )
    with pytest.raises(IntegrityError) as caught:
        with connection.begin_nested():
            connection.execute(
                text(
                    "INSERT INTO theta.quotes "
                    "(contract_id, interval_s, ts, bid_size, bid_exchange, bid, bid_condition, "
                    "ask_size, ask_exchange, ask, ask_condition) "
                    "VALUES (:contract_id, 60, :ts, 1, 5, 1, 0, 1, 5, 2, 0)"
                ),
                {"contract_id": contract_id, "ts": parse_thetadata_timestamp("2029-01-02T09:30:00")},
            )
    assert "partition" in str(caught.value).lower()
