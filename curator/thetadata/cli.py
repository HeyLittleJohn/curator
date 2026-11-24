"""ThetaData subcommands for the curator CLI.

``plan`` builds the SPXW job queue. ``backfill`` and ``load-day`` download the
queued jobs and print counts only. ``nightly`` plans and loads today's session
after 17:15 America/New_York. ``db`` runs the theta schema Alembic chain.
"""

import os
from datetime import date
from pathlib import Path
from typing import Annotated, NoReturn, Optional

import typer

from curator.thetadata.config import BackfillMode

theta_app = typer.Typer(help="ThetaData historical backfill commands")

_NOT_IMPLEMENTED = "not implemented"


def _not_implemented() -> None:
    """Stop a stub command that later tickets will implement.

    Raises:
        typer.Exit: Always, with status code 1.
    """
    typer.echo(_NOT_IMPLEMENTED)
    raise typer.Exit(code=1)


DatabaseUrlOption = Annotated[
    Optional[str],
    typer.Option("--database-url", "-d", envvar="THETADATA_DATABASE_URL", help="PostgreSQL connection string"),
]
ApiUrlOption = Annotated[
    str,
    typer.Option("--api-url", envvar="THETADATA_API_BASE_URL", help="ThetaData API base URL"),
]
_DEFAULT_API_URL = "http://127.0.0.1:25503/v3"
WorkerCountOption = Annotated[
    Optional[int],
    typer.Option(
        "--workers",
        "-w",
        envvar="THETADATA_WORKER_COUNT",
        help="Worker processes. Default 4, the Options Standard concurrency cap.",
    ),
]
DebugOption = Annotated[bool, typer.Option("--debug", help="Print the sanitized traceback on failure.")]
NearDteOption = Annotated[
    int,
    typer.Option("--near-dte", help="Keep every listed expiration through this calendar DTE. Default 40."),
]
WeeklyDteOption = Annotated[
    int,
    typer.Option(
        "--weekly-dte",
        help="Keep weekly Fridays that are not the third Friday through this DTE. Default 50.",
    ),
]
StandardDteOption = Annotated[
    int,
    typer.Option(
        "--standard-dte",
        help="Keep monthlies, quarterlies, and end-of-month expirations through this DTE. Default 183.",
    ),
]
RollInDteOption = Annotated[
    int,
    typer.Option(
        "--roll-in-dte",
        help="Also keep the next monthly, quarterly, or end-of-month expiration through this DTE. Default 220.",
    ),
]
ModeOption = Annotated[
    BackfillMode,
    typer.Option("--mode", "-m", help="Backfill mode: 'full' or 'incremental'"),
]
SymbolsOption = Annotated[
    Optional[str],
    typer.Option("--symbols", "-s", help="Comma-separated symbols (default: the configured universe)"),
]
StartDateOption = Annotated[
    Optional[str],
    typer.Option("--start-date", help="Start date (YYYY-MM-DD)"),
]
EndDateOption = Annotated[
    Optional[str],
    typer.Option("--end-date", help="End date (YYYY-MM-DD)"),
]


@theta_app.command("equities")
def equities(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
) -> None:
    """Backfill equity OHLC, trades, quotes, and EOD."""
    del database_url, api_url, workers, mode, symbols, start_date, end_date
    _not_implemented()


@theta_app.command("options")
def options(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
) -> None:
    """Backfill options quotes, OHLC, open interest, EOD, and first-order greeks."""
    del database_url, api_url, workers, mode, symbols, start_date, end_date
    _not_implemented()


@theta_app.command("indices")
def indices(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
) -> None:
    """Backfill index data. Not part of the Options Standard subscription."""
    del database_url, api_url, workers, mode, symbols, start_date, end_date
    _not_implemented()


@theta_app.command("all")
def backfill_all(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
) -> None:
    """Backfill every dataset that later tickets enable."""
    del database_url, api_url, workers, mode, symbols, start_date, end_date
    _not_implemented()


@theta_app.command("status")
def status(database_url: DatabaseUrlOption = None) -> None:
    """Show backfill record counts."""
    del database_url
    _not_implemented()


def _blank(value: str | None) -> str | None:
    """Treat a blank database URL as unset.

    Args:
        value: URL from the option or the environment.

    Returns:
        The stripped URL, or ``None`` when it is missing or blank.
    """
    if value is None or not value.strip():
        return None
    return value.strip()


def _plan_dates(start: str, end: str) -> tuple[date, date]:
    """Parse plan dates.

    Args:
        start: ``YYYY-MM-DD`` start.
        end: ``YYYY-MM-DD`` end.

    Returns:
        The two dates.

    Raises:
        typer.Exit: Either value is not an ISO date.
    """
    try:
        return date.fromisoformat(start), date.fromisoformat(end)
    except ValueError:
        typer.echo("dates must be YYYY-MM-DD", err=True)
        raise typer.Exit(code=1) from None


@theta_app.command("plan")
def plan(
    start: Annotated[str, typer.Option("--start", help="First date (YYYY-MM-DD)")],
    end: Annotated[str, typer.Option("--end", help="Last date (YYYY-MM-DD)")],
    dataset: Annotated[
        Optional[list[str]],
        typer.Option(
            "--dataset",
            help="Dataset to plan. Repeat the option or comma-separate names. Default: all SPXW datasets.",
        ),
    ] = None,
    max_dte: Annotated[
        Optional[int],
        typer.Option(
            "--max-dte",
            help="Optional extra calendar DTE cap applied after expiration selection.",
        ),
    ] = None,
    near_dte: NearDteOption = 40,
    weekly_dte: WeeklyDteOption = 50,
    standard_dte: StandardDteOption = 183,
    roll_in_dte: RollInDteOption = 220,
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    database_url: DatabaseUrlOption = None,
    debug: DebugOption = False,
) -> None:
    """Plan SPXW jobs and print counts only.

    Expirations come from ``option/list/expirations?symbol=SPXW`` and are
    filtered to the VS3D windows. A start before 2022-05-16 is clamped. The
    output is counts, not dates or rows. Sessions are synced one at a time.
    """
    from curator.thetadata.planner import (
        PlanError,
        fetch_spxw_expirations,
        format_plan_counts,
        parse_datasets,
        resolve_range,
        run_plan,
    )
    from curator.thetadata.progress import open_progress_store

    start_day, end_day = _plan_dates(start, end)
    try:
        resolve_range(start_day, end_day)
        parse_datasets(dataset)
        if max_dte is not None and max_dte < 0:
            raise PlanError("max_dte must be >= 0")
        expirations = fetch_spxw_expirations(api_url)
        with open_progress_store(_blank(database_url)) as store:
            result = run_plan(
                expirations,
                start_day,
                end_day,
                datasets=dataset,
                max_dte=max_dte,
                queue=store,
                near_dte=near_dte,
                weekly_dte=weekly_dte,
                standard_dte=standard_dte,
                roll_in_dte=roll_in_dte,
                materialize=False,
            )
    except PlanError as exc:
        _fail(exc, debug=debug)
    except Exception as exc:
        _fail(exc, debug=debug)
    typer.echo(format_plan_counts(result), nl=False)


def _fail(exc: Exception, *, debug: bool) -> NoReturn:
    """Print a sanitized error, log the traceback, and exit.

    Args:
        exc: Failure from planning or loading.
        debug: When true, also print the sanitized traceback.

    Raises:
        typer.Exit: Always, with status code 1.
    """
    from curator.thetadata.exceptions import log_sanitized_exception, sanitize_text

    logged = log_sanitized_exception(exc)
    typer.echo(sanitize_text(str(exc)), err=True)
    if debug:
        typer.echo(logged, err=True)
    raise typer.Exit(code=1) from None


def _strike_range_option(value: str) -> int | None:
    """Parse ``--strike-range``.

    Args:
        value: Width, or ``all`` for the full chain.

    Returns:
        Strikes on each side of ATM, or ``None`` for the full chain.

    Raises:
        typer.Exit: The value is not a width or ``all``.
    """
    from curator.thetadata.window import StrikeWindowError, parse_strike_range

    try:
        return parse_strike_range(value)
    except StrikeWindowError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None


def _run_load(
    *,
    start: str,
    end: str,
    dataset: list[str] | None,
    max_dte: int | None,
    strike_range: str,
    api_url: str,
    database_url: str | None,
    workers: int | None,
    order: str,
    near_dte: int,
    weekly_dte: int,
    standard_dte: int,
    roll_in_dte: int,
    debug: bool,
) -> None:
    """Run a backfill or a single day and print counts.

    Args:
        start: First ``YYYY-MM-DD``.
        end: Last ``YYYY-MM-DD``.
        dataset: Dataset filters. ``None`` loads every SPXW dataset.
        max_dte: Optional calendar DTE cap.
        strike_range: Window width or ``all``.
        api_url: Theta Terminal base URL.
        database_url: Postgres URL. Not printed.
        workers: Worker processes. ``None`` uses 4.
        order: ``newest`` or ``oldest``.
        near_dte: Inclusive DTE that keeps every listed expiration.
        weekly_dte: Inclusive DTE for non-third-Friday weeklies.
        standard_dte: Inclusive DTE for monthlies, quarterlies, and end-of-month.
        roll_in_dte: Inclusive DTE for the next standard expiration.
        debug: Print the sanitized traceback when the load fails.

    Returns:
        None.

    Raises:
        typer.Exit: The load fails, or any job is marked failed.
    """
    import asyncio

    from curator.thetadata.backfill import run_backfill
    from curator.thetadata.report import format_load_report

    start_day, end_day = _plan_dates(start, end)
    width = _strike_range_option(strike_range)
    try:
        lock_env = os.environ.get("THETADATA_LOCK_PATH", "").strip()
        report = asyncio.run(
            run_backfill(
                start=start_day,
                end=end_day,
                api_base_url=api_url,
                database_url=_blank(database_url),
                datasets=dataset,
                max_dte=max_dte,
                strike_range=width,
                worker_count=workers,
                lock_path=Path(lock_env) if lock_env else None,
                order=order,
                near_dte=near_dte,
                weekly_dte=weekly_dte,
                standard_dte=standard_dte,
                roll_in_dte=roll_in_dte,
            )
        )
    except Exception as exc:
        _fail(exc, debug=debug)
    typer.echo(format_load_report(report), nl=False)
    if report.jobs_failed:
        raise typer.Exit(code=1)


DatasetOption = Annotated[
    Optional[list[str]],
    typer.Option(
        "--dataset",
        help="Dataset to load. Repeat the option or comma-separate names. Default: all SPXW datasets.",
    ),
]
MaxDteOption = Annotated[
    Optional[int],
    typer.Option("--max-dte", help="Optional extra calendar DTE cap applied after expiration selection."),
]
StrikeRangeOption = Annotated[
    str,
    typer.Option(
        "--strike-range",
        help="Strikes on each side of ATM for quotes, OHLC, and greeks. Default 125. 'all' is the full chain.",
    ),
]


@theta_app.command("backfill")
def backfill(
    start: Annotated[str, typer.Option("--start", help="First date (YYYY-MM-DD)")],
    end: Annotated[str, typer.Option("--end", help="Last date (YYYY-MM-DD)")],
    dataset: DatasetOption = None,
    max_dte: MaxDteOption = None,
    strike_range: StrikeRangeOption = "125",
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    database_url: DatabaseUrlOption = None,
    workers: WorkerCountOption = None,
    order: Annotated[
        str,
        typer.Option("--order", help="Walk sessions newest-first (default) or oldest-first."),
    ] = "newest",
    near_dte: NearDteOption = 40,
    weekly_dte: WeeklyDteOption = 50,
    standard_dte: StandardDteOption = 183,
    roll_in_dte: RollInDteOption = 220,
    debug: DebugOption = False,
) -> None:
    """Download SPXW jobs for a date range and print counts only.

    Sessions are planned and drained one at a time, newest first unless
    ``--order oldest`` is set.
    """
    _run_load(
        start=start,
        end=end,
        dataset=dataset,
        max_dte=max_dte,
        strike_range=strike_range,
        api_url=api_url,
        database_url=database_url,
        workers=workers,
        order=order,
        near_dte=near_dte,
        weekly_dte=weekly_dte,
        standard_dte=standard_dte,
        roll_in_dte=roll_in_dte,
        debug=debug,
    )


@theta_app.command("load-day")
def load_day(
    day: Annotated[str, typer.Option("--date", help="Session date (YYYY-MM-DD)")],
    dataset: DatasetOption = None,
    max_dte: MaxDteOption = None,
    strike_range: StrikeRangeOption = "125",
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    database_url: DatabaseUrlOption = None,
    workers: WorkerCountOption = None,
    near_dte: NearDteOption = 40,
    weekly_dte: WeeklyDteOption = 50,
    standard_dte: StandardDteOption = 183,
    roll_in_dte: RollInDteOption = 220,
    debug: DebugOption = False,
) -> None:
    """Download one SPXW session and print row counts, bytes, and the projected backfill."""
    _run_load(
        start=day,
        end=day,
        dataset=dataset,
        max_dte=max_dte,
        strike_range=strike_range,
        api_url=api_url,
        database_url=database_url,
        workers=workers,
        order="newest",
        near_dte=near_dte,
        weekly_dte=weekly_dte,
        standard_dte=standard_dte,
        roll_in_dte=roll_in_dte,
        debug=debug,
    )


@theta_app.command("nightly")
def nightly(
    dataset: DatasetOption = None,
    max_dte: MaxDteOption = None,
    strike_range: StrikeRangeOption = "125",
    lookback: Annotated[
        int,
        typer.Option(
            "--lookback",
            envvar="THETADATA_NIGHTLY_LOOKBACK",
            help="NYSE sessions to catch up, including today. Default 10. Clamped to 2022-05-16.",
        ),
    ] = 10,
    wait: Annotated[
        bool,
        typer.Option(
            "--wait",
            help="Sleep until 17:15 America/New_York. Default: exit 0 with a message if started earlier.",
        ),
    ] = False,
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    database_url: DatabaseUrlOption = None,
    workers: WorkerCountOption = None,
    near_dte: NearDteOption = 40,
    weekly_dte: WeeklyDteOption = 50,
    standard_dte: StandardDteOption = 183,
    roll_in_dte: RollInDteOption = 220,
    debug: DebugOption = False,
) -> None:
    """Plan and load today plus unfinished sessions in the lookback.

    The default exits before 17:15 America/New_York and on weekends and NYSE
    holidays. A finished window makes no requests. Job failures exit non-zero.
    """
    import asyncio

    from curator.thetadata.nightly import (
        SKIP_CLOSED,
        SKIP_CLOSED_MESSAGE,
        SKIP_EARLY,
        SKIP_EARLY_MESSAGE,
        format_nightly_report,
        run_nightly,
    )

    width = _strike_range_option(strike_range)
    try:
        lock_env = os.environ.get("THETADATA_LOCK_PATH", "").strip()
        report = asyncio.run(
            run_nightly(
                api_base_url=api_url,
                database_url=_blank(database_url),
                datasets=dataset,
                max_dte=max_dte,
                strike_range=width,
                worker_count=workers,
                lock_path=Path(lock_env) if lock_env else None,
                lookback=lookback,
                wait=wait,
                near_dte=near_dte,
                weekly_dte=weekly_dte,
                standard_dte=standard_dte,
                roll_in_dte=roll_in_dte,
            )
        )
    except Exception as exc:
        _fail(exc, debug=debug)
    typer.echo(format_nightly_report(report), nl=False)
    if report.action == SKIP_EARLY:
        typer.echo(SKIP_EARLY_MESSAGE, err=True)
    elif report.action == SKIP_CLOSED:
        typer.echo(SKIP_CLOSED_MESSAGE, err=True)
    if report.jobs_failed:
        raise typer.Exit(code=1)


@theta_app.command("migrate")
def migrate(
    database_url: DatabaseUrlOption = None,
    revision: Annotated[str, typer.Option(help="Target revision")] = "head",
) -> None:
    """Run ThetaData database migrations.

    The theta schema chain is ``curator theta db``.
    """
    del database_url, revision
    _not_implemented()


db_app = typer.Typer(help="Theta schema migrations. Separate from the main Alembic chain.", no_args_is_help=True)


def _run_db(direction: str, revision: str, database_url: str | None, *, sql: bool) -> None:
    """Run one theta migration command.

    Args:
        direction: ``upgrade``, ``downgrade``, or ``current``.
        revision: Alembic target. Ignored for ``current``.
        database_url: Postgres URL. Ignored when ``sql`` is true. Not printed.
        sql: Render SQL and do not connect.

    Returns:
        None.
    """
    from curator.thetadata.migrate import MigrationError, render_migration_sql, run_migration

    try:
        if sql:
            typer.echo(render_migration_sql(direction, revision))
            return
        output = run_migration(direction, revision, database_url)
        if output:
            typer.echo(output, nl=not output.endswith("\n"))
        return
    except MigrationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from None


@db_app.command("upgrade")
def db_upgrade(
    revision: Annotated[str, typer.Argument(help="Target revision")] = "head",
    database_url: DatabaseUrlOption = None,
    sql: Annotated[bool, typer.Option("--sql", help="Render SQL and do not connect")] = False,
) -> None:
    """Apply theta schema migrations up to a revision."""
    _run_db("upgrade", revision, database_url, sql=sql)


@db_app.command("downgrade")
def db_downgrade(
    revision: Annotated[str, typer.Argument(help="Target revision. Use 'base' to drop the theta tables.")] = "-1",
    database_url: DatabaseUrlOption = None,
    sql: Annotated[bool, typer.Option("--sql", help="Render SQL and do not connect")] = False,
) -> None:
    """Revert theta schema migrations to a revision."""
    _run_db("downgrade", revision, database_url, sql=sql)


@db_app.command("current")
def db_current(database_url: DatabaseUrlOption = None) -> None:
    """Print the current theta revision."""
    _run_db("current", "head", database_url, sql=False)


theta_app.add_typer(db_app, name="db")
