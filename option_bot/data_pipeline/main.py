import asyncio
from datetime import datetime

import typer
from data_pipeline.orchestrator import (
    import_all,
    import_partial,
    remove_tickers_from_universe,
)

from option_bot.utils import months_ago

DEFAULT_DAYS = 500
DEFAULT_MONTHS_HIST = 24
DEFAULT_START_DATE = months_ago()

app = typer.Typer(
    help="CLI for adding stocks to the data pull process and for refreshing stock/options pricing data"
)


@app.command(name="add")
def add(
    tickers: list[str] = typer.Argument(
        default=["SPY"], help="Underlying tickers to add to the data universe or to include in the pull"
    ),
    all_tickers: bool = typer.Option(False, "--all-tickers", "-A", help="Add all stock tickers to the pull"),
    partial: list[int] = typer.Option(
        None,
        "-p",
        help=(
            "Components to pull (import/refresh):"
            " 1: stock metadata,"
            " 2: stock prices,"
            " 3: options contracts,"
            " 4: options prices,"
            " 5: options snapshots,"
            # " 6: options quotes"
        ),
    ),
    start_date: datetime = typer.Option(
        DEFAULT_START_DATE,
        "--start-date",
        "-s",
        formats=["%Y-%m"],
        help="Start date of data pull (YYYY-MM)",
    ),
    end_date: datetime = typer.Option(
        datetime.now(), "--end-date", "-e", formats=["%Y-%m"], help="End date of data pull (YYYY-MM)"
    ),
    months_hist: int = typer.Option(
        DEFAULT_MONTHS_HIST,
        "-m",
        help="Months of historical options contracts to pull. **Only works if you DO NOT specify a start/end date**",
    ),
):
    if all_tickers:
        tickers = []
    if partial:
        asyncio.run(import_partial(partial, tickers, start_date, end_date, months_hist))
    else:
        asyncio.run(import_all(tickers, start_date, end_date, months_hist))


@app.command(name="refresh")
def refresh(
    tickers: list[str] = typer.Argument(
        default=["SPY"], help="Underlying tickers to add to the data universe or to include in the pull"
    ),
    all_tickers: bool = typer.Option(False, "--all-tickers", "-A", help="Add all stock tickers to the pull"),
    partial: list[int] = typer.Option(
        None,
        "-p",
        help=(
            "Components to pull (import/refresh):"
            " 1: stock metadata,"
            " 2: stock prices,"
            " 3: options contracts,"
            " 4: options prices,"
            " 5: options snapshots,"
            # " 6: options quotes"
        ),
    ),
    start_date: datetime = typer.Option(
        DEFAULT_START_DATE,
        "--start-date",
        "-s",
        formats=["%Y-%m"],
        help="Start date of data pull (YYYY-MM)",
    ),
    end_date: datetime = typer.Option(
        datetime.now(), "--end-date", "-e", formats=["%Y-%m"], help="End date of data pull (YYYY-MM)"
    ),
    months_hist: int = typer.Option(
        DEFAULT_MONTHS_HIST,
        "-m",
        help="Months of historical options contracts to pull. **Only works if you DO NOT specify a start/end date**",
    ),
):
    # TODO: a logic to look up the date of the most recent pull and set as start date
    pass


@app.command(name="remove")
def remove(
    tickers: list[str] = typer.Argument(
        default=["SPY"], help="Underlying tickers to remove from the data universe"
    ),
):
    typer.echo(f"Removing tickers: {tickers}")
    asyncio.run(remove_tickers(tickers))
    typer.echo(f"Completed removing tickers: {tickers}")


async def remove_tickers(tickers: list[str]):
    await remove_tickers_from_universe(tickers)


def main():
    """Run the Typer CLI"""
    app()


if __name__ == "__main__":
    main()
