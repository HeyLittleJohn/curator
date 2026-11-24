"""Batch upserts for theta fact tables.

Each batch is copied into a temporary table and inserted with ``ON CONFLICT``
on TD-2's unique keys. Contracts are upserted first so fact rows can use
``contract_id``. Quote batches also ensure the Eastern month partition exists,
including the months ``add_partitions_ahead`` creates from today.

The engine hides parameters and SQLAlchemy statement logs stay quiet, so a
failed insert does not dump row values.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, datetime

import psycopg
from psycopg import sql
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, create_engine

from curator.thetadata.jobs import DATASET_QUOTES, PlanJob
from curator.thetadata.mapping import (
    SPECS,
    Cell,
    ContractKey,
    FactSpec,
    LoadError,
    PendingFact,
    collapse_records,
    contract_insert_sql,
    fact_insert_sql,
)
from curator.thetadata.migrate import MigrationError, to_sync_database_url
from curator.thetadata.partitions import add_partitions_ahead, monthly_partition_statements
from curator.thetadata.time import EASTERN

_CONTRACT_COLUMNS = (
    ("symbol", "text"),
    ("expiration", "date"),
    ("strike", "numeric(12,3)"),
    ("right", "text"),
)
_SQL_TYPES = {
    "text": sql.SQL("text"),
    "date": sql.SQL("date"),
    "numeric(12,3)": sql.SQL("numeric(12,3)"),
    "numeric(18,8)": sql.SQL("numeric(18,8)"),
    "bigint": sql.SQL("bigint"),
    "integer": sql.SQL("integer"),
    "smallint": sql.SQL("smallint"),
    "timestamptz": sql.SQL("timestamptz"),
}


def open_engine(database_url: str) -> Engine:
    """Open a sync engine for one worker.

    Args:
        database_url: Postgres URL. Not logged.

    Returns:
        An engine with echo and parameter logging off.

    Raises:
        LoadError: The URL is not a postgresql URL. The message has no URL.
    """
    _quiet_database_logs()
    try:
        url = to_sync_database_url(database_url)
    except MigrationError as exc:
        raise LoadError(str(exc)) from None
    return create_engine(url, echo=False, hide_parameters=True, pool_pre_ping=True)


def write_batch(engine: Engine, job: PlanJob, facts: Sequence[PendingFact]) -> int:
    """Upsert one batch of facts.

    Args:
        engine: Worker engine. The caller owns its lifetime.
        job: Job the facts belong to.
        facts: Rows after the strike window.

    Returns:
        Rows written after collapsing duplicate conflict keys.

    Raises:
        LoadError: The dataset has no spec, or the driver is not psycopg.
    """
    if not facts:
        return 0
    spec = SPECS[job.dataset]
    with engine.begin() as connection:
        if job.dataset == DATASET_QUOTES:
            _ensure_quote_partitions(connection, facts)
        identities = _upsert_contracts(connection, [fact.contract for fact in facts])
        records: list[dict[str, Cell]] = []
        for fact in facts:
            try:
                contract_id = identities[fact.contract]
            except KeyError as exc:
                raise LoadError("contract id was not returned") from exc
            record: dict[str, Cell] = {"contract_id": contract_id}
            record.update(fact.values)
            records.append(record)
        collapsed = collapse_records(records, spec.conflict)
        if not collapsed:
            return 0
        _copy_upsert(connection, spec, collapsed)
        return len(collapsed)


def _quiet_database_logs() -> None:
    """Keep SQLAlchemy from logging statements or parameters.

    Returns:
        None.
    """
    for name in ("sqlalchemy", "sqlalchemy.engine", "sqlalchemy.pool"):
        logging.getLogger(name).setLevel(logging.CRITICAL)


def _ensure_quote_partitions(connection: Connection, facts: Sequence[PendingFact]) -> None:
    """Ensure quote partitions for today and for the batch's timestamps.

    Args:
        connection: Open transaction.
        facts: Quote facts. ``ts`` is a timestamptz.

    Returns:
        None.
    """
    add_partitions_ahead(connection, months=3)
    days: list[date] = []
    for fact in facts:
        stamp = fact.values.get("ts")
        if isinstance(stamp, datetime):
            days.append(stamp.astimezone(EASTERN).date())
    if not days:
        return
    last = max(days)
    if last.month == 12:
        until = date(last.year + 1, 1, 1)
    else:
        until = date(last.year, last.month + 1, 1)
    for statement in monthly_partition_statements(min(days), until):
        connection.execute(text(statement))


def _driver(connection: Connection) -> psycopg.Connection:
    """Return the psycopg connection behind a SQLAlchemy connection.

    Args:
        connection: Open SQLAlchemy connection.

    Returns:
        The psycopg connection in the same transaction.

    Raises:
        LoadError: The driver is not psycopg.
    """
    raw = connection.connection
    if isinstance(raw, psycopg.Connection):
        return raw
    for attr in ("dbapi_connection", "driver_connection"):
        candidate = getattr(raw, attr, None)
        if isinstance(candidate, psycopg.Connection):
            return candidate
    raise LoadError("theta upserts require psycopg")


def _sql_type(name: str) -> sql.SQL:
    """Return a column type the loader is allowed to declare.

    Args:
        name: A type from a fact spec or the contracts temp table.

    Returns:
        The SQL type fragment.

    Raises:
        LoadError: ``name`` is not one of the loader's types.
    """
    try:
        return _SQL_TYPES[name]
    except KeyError as exc:
        raise LoadError("unsafe column type") from exc


def _create_temp(cursor: psycopg.Cursor, table: str, columns: Sequence[tuple[str, str]]) -> None:
    """Create an on-commit-drop temp table.

    Args:
        cursor: Cursor on the worker transaction.
        table: Temp table name. Composed as an identifier, not interpolated text.
        columns: Column names and SQL types from the loader's allow-list.

    Returns:
        None.
    """
    body = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(name), _sql_type(sql_type)) for name, sql_type in columns
    )
    statement = sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(sql.Identifier(table), body)
    cursor.execute(statement)


def _copy(cursor: psycopg.Cursor, table: str, columns: Sequence[str], rows: Sequence[tuple[object, ...]]) -> None:
    """Copy rows into a temp table.

    Args:
        cursor: Cursor on the worker transaction.
        table: Temp table name.
        columns: Column names, in tuple order.
        rows: Values to copy.

    Returns:
        None.
    """
    names = sql.SQL(", ").join(sql.Identifier(name) for name in columns)
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier(table), names)
    with cursor.copy(statement) as copy:
        for row in rows:
            copy.write_row(row)


def _upsert_contracts(connection: Connection, keys: Sequence[ContractKey]) -> dict[ContractKey, int]:
    """Insert contract keys and return their ids.

    Args:
        connection: Open transaction.
        keys: Contract natural keys, possibly with duplicates.

    Returns:
        A map from natural key to ``theta.contracts.id``.
    """
    driver = _driver(connection)
    rows = [(key.symbol, key.expiration, key.strike, key.right) for key in keys]
    with driver.cursor() as cursor:
        _create_temp(cursor, "tmp_theta_contracts", _CONTRACT_COLUMNS)
        _copy(cursor, "tmp_theta_contracts", [name for name, _type in _CONTRACT_COLUMNS], rows)
    found: dict[ContractKey, int] = {}
    result = connection.execute(text(contract_insert_sql())).mappings()
    for row in result:
        key = ContractKey(
            symbol=str(row["symbol"]),
            expiration=row["expiration"],
            strike=row["strike"],
            right=str(row["right"]),
        )
        found[key] = int(row["id"])
    return found


def _copy_upsert(connection: Connection, spec: FactSpec, records: Sequence[dict[str, Cell]]) -> None:
    """Copy fact records and upsert them on the conflict key.

    Args:
        connection: Open transaction.
        spec: Fact table shape.
        records: Records including ``contract_id``.

    Returns:
        None.
    """
    driver = _driver(connection)
    names = spec.column_names
    rows = [tuple(record[name] for name in names) for record in records]
    with driver.cursor() as cursor:
        _create_temp(cursor, "tmp_theta_facts", spec.columns)
        _copy(cursor, "tmp_theta_facts", names, rows)
    connection.execute(text(fact_insert_sql(spec)))
