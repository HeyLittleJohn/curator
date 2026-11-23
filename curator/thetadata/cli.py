"""ThetaData subcommands for the curator CLI.

Backfill commands stay stubs until later tickets fill in the client and loaders.
``db`` runs the theta schema Alembic chain.
"""

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
    api_url: ApiUrlOption = "http://localhost:25503/v3",
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
    api_url: ApiUrlOption = "http://localhost:25503/v3",
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
    api_url: ApiUrlOption = "http://localhost:25503/v3",
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
    api_url: ApiUrlOption = "http://localhost:25503/v3",
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
