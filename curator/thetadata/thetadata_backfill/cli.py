"""CLI interface for ThetaData backfill system.

Supports all configuration via CLI arguments or environment variables.
"""

import asyncio
from datetime import date
from typing import Annotated, Optional

import typer
from rich.console import Console

from thetadata_backfill import __version__
from thetadata_backfill.config import BackfillMode, Settings
from thetadata_backfill.orchestrator import Orchestrator, BackfillJob
from thetadata_backfill.workers import install_uvloop

app = typer.Typer(
    name="thetadata-backfill",
    help="High-performance ThetaData historical data backfill system",
    add_completion=False,
)

console = Console()

# Common options
DatabaseUrlOption = Annotated[
    Optional[str],
    typer.Option(
        "--database-url",
        "-d",
        envvar="THETADATA_DATABASE_URL",
        help="PostgreSQL connection string",
    ),
]

ApiUrlOption = Annotated[
    str,
    typer.Option(
        "--api-url",
        envvar="THETADATA_API_BASE_URL",
        help="ThetaData API base URL",
    ),
]

WorkerCountOption = Annotated[
    Optional[int],
    typer.Option(
        "--workers",
        "-w",
        envvar="THETADATA_WORKER_COUNT",
        help="Number of worker processes",
    ),
]

ModeOption = Annotated[
    BackfillMode,
    typer.Option(
        "--mode",
        "-m",
        help="Backfill mode: 'full' or 'incremental'",
    ),
]

SymbolsOption = Annotated[
    Optional[str],
    typer.Option(
        "--symbols",
        "-s",
        help="Comma-separated list of symbols (default: all)",
    ),
]

StartDateOption = Annotated[
    Optional[str],
    typer.Option(
        "--start-date",
        help="Start date (YYYY-MM-DD, default: earliest available)",
    ),
]

EndDateOption = Annotated[
    Optional[str],
    typer.Option(
        "--end-date",
        help="End date (YYYY-MM-DD, default: today)",
    ),
]


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse date string to date object."""
    if date_str is None:
        return None
    return date.fromisoformat(date_str)


def parse_symbols(symbols_str: Optional[str]) -> Optional[list[str]]:
    """Parse comma-separated symbols."""
    if symbols_str is None:
        return None
    return [s.strip().upper() for s in symbols_str.split(",")]


def create_settings(
    database_url: Optional[str] = None,
    api_url: str = "http://localhost:25503/v3",
    worker_count: Optional[int] = None,
    mode: BackfillMode = BackfillMode.FULL,
) -> Settings:
    """Create settings from CLI options."""
    settings = Settings(
        api_base_url=api_url,
        database_url=database_url,
        worker_count=worker_count,
        backfill_mode=mode,
    )
    
    if settings.database_url is None:
        console.print("[red]Error: Database URL not configured![/red]")
        console.print("Set THETADATA_DATABASE_URL or pass --database-url")
        raise typer.Exit(1)
    
    return settings


@app.command()
def version():
    """Show version information."""
    console.print(f"thetadata-backfill v{__version__}")


@app.command()
def migrate(
    database_url: DatabaseUrlOption = None,
    revision: str = typer.Option("head", help="Target revision"),
):
    """Run database migrations."""
    import os
    import subprocess
    
    if database_url:
        os.environ["THETADATA_DATABASE_URL"] = database_url
    
    console.print(f"Running migrations to revision: {revision}")
    
    result = subprocess.run(
        ["alembic", "upgrade", revision],
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    )
    
    if result.returncode == 0:
        console.print("[green]Migrations complete![/green]")
    else:
        console.print("[red]Migration failed![/red]")
        raise typer.Exit(1)


@app.command("equities")
def backfill_equities(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = "http://localhost:25503/v3",
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
    data_types: str = typer.Option(
        "ohlc,eod",
        "--data-types",
        help="Comma-separated data types: ohlc,trade,quote,eod",
    ),
):
    """Backfill equity (stock) data."""
    settings = create_settings(database_url, api_url, workers, mode)
    
    job = BackfillJob(
        asset_type="equity",
        data_types=[dt.strip() for dt in data_types.split(",")],
        symbols=parse_symbols(symbols),
        start_date=parse_date(start_date),
        end_date=parse_date(end_date),
        mode=mode,
    )
    
    install_uvloop()
    orchestrator = Orchestrator(settings)
    asyncio.run(orchestrator.run_backfill(job))


@app.command("options")
def backfill_options(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = "http://localhost:25503/v3",
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
    data_types: str = typer.Option(
        "trade_greeks,open_interest",
        "--data-types",
        help="Comma-separated data types: ohlc,trade_greeks,greeks,open_interest",
    ),
):
    """Backfill options data with Greeks."""
    settings = create_settings(database_url, api_url, workers, mode)
    
    job = BackfillJob(
        asset_type="option",
        data_types=[dt.strip() for dt in data_types.split(",")],
        symbols=parse_symbols(symbols),
        start_date=parse_date(start_date),
        end_date=parse_date(end_date),
        mode=mode,
    )
    
    install_uvloop()
    orchestrator = Orchestrator(settings)
    asyncio.run(orchestrator.run_backfill(job))


@app.command("indices")
def backfill_indices(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = "http://localhost:25503/v3",
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
    data_types: str = typer.Option(
        "ohlc,eod",
        "--data-types",
        help="Comma-separated data types: ohlc,price,eod",
    ),
):
    """Backfill index data."""
    settings = create_settings(database_url, api_url, workers, mode)
    
    job = BackfillJob(
        asset_type="index",
        data_types=[dt.strip() for dt in data_types.split(",")],
        symbols=parse_symbols(symbols),
        start_date=parse_date(start_date),
        end_date=parse_date(end_date),
        mode=mode,
    )
    
    install_uvloop()
    orchestrator = Orchestrator(settings)
    asyncio.run(orchestrator.run_backfill(job))


@app.command("all")
def backfill_all(
    database_url: DatabaseUrlOption = None,
    api_url: ApiUrlOption = "http://localhost:25503/v3",
    workers: WorkerCountOption = None,
    mode: ModeOption = BackfillMode.FULL,
    symbols: SymbolsOption = None,
    start_date: StartDateOption = None,
    end_date: EndDateOption = None,
):
    """Backfill all data types (equities, options, indices)."""
    settings = create_settings(database_url, api_url, workers, mode)
    install_uvloop()
    orchestrator = Orchestrator(settings)
    
    parsed_symbols = parse_symbols(symbols)
    parsed_start = parse_date(start_date)
    parsed_end = parse_date(end_date)
    
    # Equities
    console.print("\n[bold]Phase 1: Equities[/bold]")
    asyncio.run(orchestrator.run_backfill(BackfillJob(
        asset_type="equity",
        data_types=["ohlc", "trade", "quote", "eod"],
        symbols=parsed_symbols,
        start_date=parsed_start,
        end_date=parsed_end,
        mode=mode,
    )))
    
    # Options
    console.print("\n[bold]Phase 2: Options[/bold]")
    asyncio.run(orchestrator.run_backfill(BackfillJob(
        asset_type="option",
        data_types=["ohlc", "trade_greeks", "greeks", "open_interest"],
        symbols=parsed_symbols,
        start_date=parsed_start,
        end_date=parsed_end,
        mode=mode,
    )))
    
    # Indices
    console.print("\n[bold]Phase 3: Indices[/bold]")
    asyncio.run(orchestrator.run_backfill(BackfillJob(
        asset_type="index",
        data_types=["ohlc", "price", "eod"],
        symbols=parsed_symbols,
        start_date=parsed_start,
        end_date=parsed_end,
        mode=mode,
    )))
    
    console.print("\n[bold green]All backfills complete![/bold green]")


@app.command()
def status(
    database_url: DatabaseUrlOption = None,
):
    """Show current database status and record counts."""
    import asyncio
    from sqlalchemy import text
    
    from thetadata_backfill.db import DatabaseManager
    
    settings = Settings(database_url=database_url)
    
    if settings.database_url is None:
        console.print("[red]Error: Database URL not configured![/red]")
        raise typer.Exit(1)
    
    async def get_counts():
        db = DatabaseManager(settings)
        await db.initialize()
        
        tables = [
            "equity_ohlc", "equity_trade", "equity_quote", "equity_eod",
            "option_ohlc", "option_trade", "option_quote", "option_greeks",
            "option_trade_greeks", "option_open_interest", "option_eod",
            "index_ohlc", "index_price", "index_eod",
        ]
        
        counts = {}
        async with db.session_factory() as session:
            for table in tables:
                try:
                    result = await session.execute(
                        text(f"SELECT COUNT(*) FROM {table}")
                    )
                    counts[table] = result.scalar()
                except Exception:
                    counts[table] = "N/A"
        
        await db.close()
        return counts
    
    counts = asyncio.run(get_counts())
    
    from rich.table import Table
    
    table = Table(title="Database Status")
    table.add_column("Table", style="cyan")
    table.add_column("Records", style="green", justify="right")
    
    for name, count in counts.items():
        table.add_row(name, f"{count:,}" if isinstance(count, int) else count)
    
    console.print(table)


if __name__ == "__main__":
    app()
