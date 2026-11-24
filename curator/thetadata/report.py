"""Count-only load report.

Row counts are split by dataset and by bar tier (1-minute, 15-minute, or
daily). Bytes are ``pg_total_relation_size`` for each theta data table.
Partitioned ``quotes`` includes its partitions. The projection multiplies the
loaded range by the number of NYSE sessions from 2022-05-16 through the loaded
end:

    projected = measured * sessions_from_floor // sessions_loaded

That scales a measured day to the whole backfill. It assumes the relations
currently hold the loaded sessions, so fixed overhead such as empty monthly
partitions is scaled too. The report prints counts and byte sizes only.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Connection

from curator.thetadata.calendar import trading_sessions
from curator.thetadata.config import BACKFILL_START, INTERVAL_1M_SECONDS, INTERVAL_15M_SECONDS
from curator.thetadata.jobs import DATASETS, DATASETS_DAILY, DATASETS_INTRADAY
from curator.thetadata.planner import resolve_range
from curator.thetadata.time import EASTERN
from curator.thetadata.upsert import open_engine

RELATIONS: tuple[str, ...] = (
    "contracts",
    "quotes",
    "ohlc",
    "greeks_first_order",
    "open_interest",
    "eod",
)
TIER_1M = "1m"
TIER_15M = "15m"
TIER_DAILY = "daily"


@dataclass(frozen=True)
class LoadReport:
    """Counts for one load. No row values and no connection string.

    Args:
        rows: ``(dataset, tier)`` to stored row count for the loaded range.
        relation_bytes: On-disk bytes per relation.
        sessions_loaded: NYSE sessions in the loaded range.
        sessions_full: NYSE sessions from 2022-05-16 through the loaded end.
        requests: Logical terminal requests this run, including a rerun's zero.
        new_rows: Fact rows written by this run. A finished rerun is zero.
        jobs_done: Jobs marked done.
        jobs_empty: Jobs marked empty.
        jobs_failed: Jobs marked failed.
        errors: Scrubbed failure texts. The formatter does not print them.
    """

    rows: Mapping[tuple[str, str], int]
    relation_bytes: Mapping[str, int]
    sessions_loaded: int
    sessions_full: int
    requests: int
    new_rows: int
    jobs_done: int
    jobs_empty: int
    jobs_failed: int
    errors: tuple[str, ...] = ()


def project_count(measured: int, sessions_loaded: int, sessions_full: int) -> int:
    """Scale a measurement from the loaded sessions to the full backfill.

    Args:
        measured: Rows or bytes for the loaded sessions.
        sessions_loaded: NYSE sessions in the loaded range.
        sessions_full: NYSE sessions from 2022-05-16 through the loaded end.

    Returns:
        ``measured * sessions_full // sessions_loaded``, or 0 when the loaded
        range has no sessions or the measurement is empty.
    """
    if sessions_loaded <= 0 or measured <= 0:
        return 0
    return measured * sessions_full // sessions_loaded


def session_span(start: date, end: date) -> tuple[date, date, int, int]:
    """Count NYSE sessions in the loaded range and from the backfill floor.

    Args:
        start: Requested first date. Dates before 2022-05-16 are clamped.
        end: Requested last date.

    Returns:
        Effective start, effective end, loaded session count, and session count
        from the floor through the effective end.

    Raises:
        PlanRangeError: ``end`` is before ``start``.
        PlanFloorError: ``end`` is before 2022-05-16.
    """
    effective_start, effective_end, _clamped = resolve_range(start, end)
    loaded = len(trading_sessions(effective_start, effective_end))
    full = len(trading_sessions(BACKFILL_START, effective_end))
    return effective_start, effective_end, loaded, full


def tier_name(interval_s: int | None) -> str:
    """Return the report tier for a stored bar length.

    Args:
        interval_s: Seconds, or ``None`` for a daily dataset.

    Returns:
        ``1m``, ``15m``, ``daily``, or the raw seconds for another length.
    """
    if interval_s is None:
        return TIER_DAILY
    if interval_s == INTERVAL_1M_SECONDS:
        return TIER_1M
    if interval_s == INTERVAL_15M_SECONDS:
        return TIER_15M
    return f"{interval_s}s"


def empty_bytes() -> dict[str, int]:
    """Return a zero byte count for every reported relation.

    Returns:
        Relation name to 0.
    """
    return {name: 0 for name in RELATIONS}


def read_storage(database_url: str, start: date, end: date) -> tuple[dict[tuple[str, str], int], dict[str, int]]:
    """Read stored row counts and on-disk bytes.

    Args:
        database_url: Postgres URL. Not logged.
        start: First session date, inclusive.
        end: Last session date, inclusive.

    Returns:
        Row counts keyed by dataset and tier, and bytes keyed by relation.
    """
    engine = open_engine(database_url)
    try:
        with engine.connect() as connection:
            rows = _row_counts(connection, start, end)
            sizes = _relation_bytes(connection)
        return rows, sizes
    finally:
        engine.dispose()


def format_load_report(report: LoadReport) -> str:
    """Format a load as counts and byte sizes.

    Args:
        report: Load summary.

    Returns:
        One ``name=count`` line per figure, ending in a newline. No dates,
        strikes, premiums, or connection strings.
    """
    lines: list[str] = []
    for dataset, tier in _tier_slots(report.rows):
        lines.append(f"rows_{dataset}_{tier}={report.rows.get((dataset, tier), 0)}")
    for name in RELATIONS:
        lines.append(f"bytes_{name}={report.relation_bytes.get(name, 0)}")
    lines.append(f"sessions_loaded={report.sessions_loaded}")
    lines.append(f"sessions_from_floor={report.sessions_full}")
    for dataset in DATASETS:
        measured = sum(count for (name, _tier), count in report.rows.items() if name == dataset)
        lines.append(
            f"projected_rows_{dataset}={project_count(measured, report.sessions_loaded, report.sessions_full)}"
        )
    total = 0
    for name in RELATIONS:
        projected = project_count(report.relation_bytes.get(name, 0), report.sessions_loaded, report.sessions_full)
        total += projected
        lines.append(f"projected_bytes_{name}={projected}")
    lines.append(f"projected_bytes_total={total}")
    lines.append(f"requests={report.requests}")
    lines.append(f"new_rows={report.new_rows}")
    lines.append(f"jobs_done={report.jobs_done}")
    lines.append(f"jobs_empty={report.jobs_empty}")
    lines.append(f"jobs_failed={report.jobs_failed}")
    return "\n".join(lines) + "\n"


def _tier_slots(rows: Mapping[tuple[str, str], int]) -> list[tuple[str, str]]:
    """Return the tier lines in a stable order.

    Args:
        rows: Measured counts. Extra tiers are appended after the standard ones.

    Returns:
        ``(dataset, tier)`` pairs.
    """
    slots: list[tuple[str, str]] = []
    for dataset in DATASETS_INTRADAY:
        slots.append((dataset, TIER_1M))
        slots.append((dataset, TIER_15M))
        extras = sorted(tier for name, tier in rows if name == dataset and tier not in {TIER_1M, TIER_15M})
        slots.extend((dataset, tier) for tier in extras)
    for dataset in DATASETS_DAILY:
        slots.append((dataset, TIER_DAILY))
    return slots


def _bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """Return Eastern midnights covering the inclusive session range.

    Args:
        start: First session date.
        end: Last session date.

    Returns:
        Start inclusive and the next Eastern midnight after ``end``.
    """
    start_ts = datetime(start.year, start.month, start.day, tzinfo=EASTERN)
    next_day = end + timedelta(days=1)
    end_ts = datetime(next_day.year, next_day.month, next_day.day, tzinfo=EASTERN)
    return start_ts, end_ts


def _row_counts(connection: Connection, start: date, end: date) -> dict[tuple[str, str], int]:
    """Count stored facts in the session range.

    Args:
        connection: Open SQLAlchemy connection.
        start: First session date.
        end: Last session date.

    Returns:
        Counts keyed by dataset and tier.
    """
    start_ts, end_ts = _bounds(start, end)
    counts: dict[tuple[str, str], int] = {}
    for dataset in DATASETS_INTRADAY:
        grouped = connection.execute(
            text(
                f"""
                SELECT interval_s, count(*) AS n
                FROM theta.{dataset}
                WHERE ts >= :start_ts AND ts < :end_ts
                GROUP BY interval_s
                """
            ),
            {"start_ts": start_ts, "end_ts": end_ts},
        )
        for interval_s, count in grouped:
            counts[(dataset, tier_name(None if interval_s is None else int(interval_s)))] = int(count)
    for dataset in DATASETS_DAILY:
        count = connection.execute(
            text(f'SELECT count(*) FROM theta.{dataset} WHERE "date" BETWEEN :start AND :end'),
            {"start": start, "end": end},
        ).scalar_one()
        counts[(dataset, TIER_DAILY)] = int(count)
    return counts


def _relation_bytes(connection: Connection) -> dict[str, int]:
    """Read on-disk bytes for the theta data tables.

    Args:
        connection: Open SQLAlchemy connection.

    Returns:
        Bytes keyed by relation. A missing relation is zero. Partitioned
        tables use the larger of the parent size and the sum of its partitions
        so either PostgreSQL size rule is covered without double counting.
    """
    names = ", ".join(f"'{name}'" for name in RELATIONS)
    own = connection.execute(
        text(
            f"""
            SELECT c.relname AS relation, pg_total_relation_size(c.oid) AS bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'theta' AND c.relname IN ({names})
            """
        )
    )
    sizes = empty_bytes()
    for relation, nbytes in own:
        sizes[str(relation)] = int(nbytes)
    parts = connection.execute(
        text(
            f"""
            SELECT parent.relname AS relation,
                   COALESCE(SUM(pg_total_relation_size(child.oid)), 0) AS bytes
            FROM pg_inherits
            JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
            JOIN pg_class child ON child.oid = pg_inherits.inhrelid
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            WHERE n.nspname = 'theta' AND parent.relname IN ({names})
            GROUP BY parent.relname
            """
        )
    )
    for relation, nbytes in parts:
        name = str(relation)
        sizes[name] = max(sizes.get(name, 0), int(nbytes))
    return sizes
