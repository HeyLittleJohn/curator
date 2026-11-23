"""ThetaData subcommands for the curator CLI.

Backfill commands stay stubs until later tickets fill in the client and loaders.
``plan`` builds the SPXW job queue and prints counts only. ``db`` runs the theta
schema Alembic chain.
"""

from datetime import date
from typing import Annotated, Optional

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
    typer.Option("--workers", "-w", envvar="THETADATA_WORKER_COUNT", help="Number of worker processes"),
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
            help="Optional calendar DTE cap. Default: every listed expiration inside the horizon.",
        ),
    ] = None,
    api_url: ApiUrlOption = _DEFAULT_API_URL,
    database_url: DatabaseUrlOption = None,
) -> None:
    """Plan SPXW jobs and print counts only.

    Expirations come from ``option/list/expirations?symbol=SPXW``. A start before
    2022-05-16 is clamped. The output is counts, not dates or rows.
    """
    from curator.thetadata.exceptions import sanitize_text
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
                materialize=False,
            )
    except PlanError as exc:
        typer.echo(sanitize_text(str(exc)), err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        typer.echo(sanitize_text(str(exc)), err=True)
        raise typer.Exit(code=1) from None
    typer.echo(format_plan_counts(result), nl=False)


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
