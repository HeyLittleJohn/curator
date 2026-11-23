"""Monthly range partitions for theta intraday tables.

Bounds are America/New_York midnights so a partition month matches the Eastern
trading calendar, including DST. ``quotes`` is the partitioned table.
"""

import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Connection

from curator.thetadata.config import THETA_SCHEMA
from curator.thetadata.time import EASTERN

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
PARTITIONED_TABLES = frozenset({"quotes"})
_MAX_AHEAD_MONTHS = 120


def quote_ident(name: str) -> str:
    """Return a safe bare SQL identifier.

    Args:
        name: Schema, table, or partition name.

    Returns:
        ``name`` unchanged when it is a lowercase identifier.

    Raises:
        ValueError: If ``name`` is not a safe identifier.
    """
    if _IDENT.fullmatch(name) is None:
        raise ValueError("unsafe SQL identifier")
    return name


def quote_literal(value: str) -> str:
    """Quote a string as a SQL literal.

    Args:
        value: Text to embed in DDL, such as a schema comment.

    Returns:
        A single-quoted literal with quotes escaped.
    """
    return "'" + value.replace("'", "''") + "'"


def eastern_midnight_literal(day: date) -> str:
    """Format America/New_York midnight as a timestamptz literal.

    Args:
        day: Calendar date. The time is midnight in New York.

    Returns:
        A timestamp such as ``2022-05-01 00:00:00-04:00``.
    """
    moment = datetime(day.year, day.month, day.day, tzinfo=EASTERN)
    raw_offset = moment.strftime("%z")
    offset = f"{raw_offset[:3]}:{raw_offset[3:]}"
    return moment.strftime("%Y-%m-%d %H:%M:%S") + offset


def partition_name(table: str, month_start: date) -> str:
    """Return the partition table name for one month.

    Args:
        table: Parent table name.
        month_start: First day of the partition month.

    Returns:
        A name such as ``quotes_2022_05``.
    """
    quote_ident(table)
    return f"{table}_{month_start:%Y_%m}"


def iter_monthly_partitions(
    start: date,
    until: date,
    *,
    table: str = "quotes",
) -> list[tuple[str, str]]:
    """Build idempotent monthly partition DDL.

    Args:
        start: Inclusive. The month containing this date is the first partition.
        until: Exclusive end. A mid-month value still covers that month. A value
            that is already the first of a month does not cover that month.
        table: Partitioned table in the theta schema.

    Returns:
        ``(partition_name, create_sql)`` pairs, oldest month first. SQL uses
        ``CREATE TABLE IF NOT EXISTS`` and Eastern month bounds.

    Raises:
        ValueError: If ``table`` is not partitioned, the name is unsafe, or
            ``until`` is before ``start``.
    """
    if table not in PARTITIONED_TABLES:
        raise ValueError("table is not partitioned")
    if until < start:
        raise ValueError("until must not be before start")
    schema = quote_ident(THETA_SCHEMA)
    parent = quote_ident(table)
    cursor = date(start.year, start.month, 1)
    end = until if until.day == 1 else _next_month(until)
    statements: list[tuple[str, str]] = []
    while cursor < end:
        nxt = _next_month(cursor)
        name = partition_name(parent, cursor)
        sql = (
            f"CREATE TABLE IF NOT EXISTS {schema}.{name} "
            f"PARTITION OF {schema}.{parent} "
            f"FOR VALUES FROM (TIMESTAMPTZ '{eastern_midnight_literal(cursor)}') "
            f"TO (TIMESTAMPTZ '{eastern_midnight_literal(nxt)}')"
        )
        statements.append((name, sql))
        cursor = nxt
    return statements


def monthly_partition_statements(
    start: date,
    until: date,
    *,
    table: str = "quotes",
) -> list[str]:
    """Return monthly partition DDL without executing it.

    Args:
        start: Inclusive start date. See ``iter_monthly_partitions``.
        until: Exclusive end date.
        table: Partitioned table name.

    Returns:
        CREATE TABLE statements, oldest month first.
    """
    return [sql for _, sql in iter_monthly_partitions(start, until, table=table)]


def add_partitions_ahead(
    connection: Connection,
    *,
    months: int = 3,
    today: date | None = None,
    table: str = "quotes",
) -> list[str]:
    """Create monthly partitions from the current month through later months.

    Args:
        connection: Open SQLAlchemy connection. The caller commits.
        months: How many months past the current Eastern month to create.
            ``0`` creates only the current month. ``3`` creates four months.
        today: Clock override. Defaults to today's date in America/New_York.
        table: Partitioned table. Only ``quotes`` is partitioned.

    Returns:
        Partition names that were ensured, including ones that already existed.

    Raises:
        ValueError: If ``months`` is outside 0..120.
    """
    if months < 0 or months > _MAX_AHEAD_MONTHS:
        raise ValueError("months must be between 0 and 120")
    clock = today if today is not None else datetime.now(ZoneInfo("America/New_York")).date()
    start = date(clock.year, clock.month, 1)
    until = start
    for _ in range(months + 1):
        until = _next_month(until)
    created: list[str] = []
    for name, statement in iter_monthly_partitions(start, until, table=table):
        connection.execute(text(statement))
        created.append(name)
    return created


def _next_month(day: date) -> date:
    """Return the first day of the month after ``day``.

    Args:
        day: Any date. Only the year and month are used.

    Returns:
        The first day of the following month.
    """
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)
