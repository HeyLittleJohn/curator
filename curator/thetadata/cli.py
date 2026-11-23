"""ThetaData subcommands for the curator CLI.

The commands are registered like ``silver``. Their bodies stay stubs until
later tickets fill in the client, schema, and backfill.
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


@theta_app.command("migrate")
def migrate(
    database_url: DatabaseUrlOption = None,
    revision: Annotated[str, typer.Option(help="Target revision")] = "head",
) -> None:
    """Run ThetaData database migrations."""
    del database_url, revision
    _not_implemented()
