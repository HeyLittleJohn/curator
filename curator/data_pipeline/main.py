import asyncio
from datetime import datetime
from typing import Annotated, Optional

import typer

from curator.data_pipeline.orchestrator import (
    import_all,
    import_partial,
    remove_tickers_from_universe,
)
from curator.data_pipeline.silver_futures_upload import silver_app
from curator.utils import months_ago

DEFAULT_MONTHS_HIST = 24
DEFAULT_START_DATE = months_ago(months=DEFAULT_MONTHS_HIST)
DEFAULT_END_DATE = datetime.now()
DEFAULT_REFRESH_PARTIAL: list[int] = [5]

app = typer.Typer(help="CLI for adding stocks to the data pull process and for refreshing stock/options pricing data")
app.add_typer(silver_app, name="silver")


def validate_partial(ctx, param, value: list[int]):
    if not all(1 <= i <= 6 for i in value):
        raise typer.BadParameter("All values must be between 1 and 6")
    return value


@app.command(name="add")
def add(
    tickers: Annotated[
        list[str], typer.Argument(help="Underlying tickers to add to the data universe or to include in the pull")
    ],
    all_tickers: Annotated[bool, typer.Option("--all-tickers", "-A", help="Add all stock tickers to the pull")] = False,
    partial: Annotated[
        Optional[list[int]],
        typer.Option(
            "--partial",
            "-p",
            callback=validate_partial,
            help=(
                "Components to pull (import/refresh):"
                " 1: stock metadata,"
                " 2: stock prices,"
                " 3: options contracts,"
                " 4: options prices,"
                " 5: options snapshots,"
                " 6: options quotes"
            ),
        ),
    ] = None,
    start_date: Annotated[
        Optional[datetime],
        typer.Option(
            "--start-date",
            "-s",
            formats=["%Y-%m"],
            help="Start date of data pull (YYYY-MM)",
        ),
    ] = None,
    end_date: Annotated[
        Optional[datetime],
        typer.Option("--end-date", "-e", formats=["%Y-%m"], help="End date of data pull (YYYY-MM)"),
    ] = None,
    months_hist: Annotated[
        Optional[int],
        typer.Option(
            "--months-hist",
            "-m",
            help="Months of historical options contracts to pull."
            " **Only works if you DO NOT specify a start/end date**",
        ),
    ] = None,
):
    if all_tickers:
        tickers = []

    if months_hist:
        if end_date and start_date:
            raise typer.BadParameter("You can't specify a start/end date and months_hist")
        elif end_date:
            start_date = months_ago(months=months_hist, end_date=end_date)
        else:
            start_date = months_ago(months=months_hist)
            end_date = datetime.now()
    else:
        months_hist = DEFAULT_MONTHS_HIST

    if not start_date:
        start_date = DEFAULT_START_DATE

    if not end_date:
        end_date = datetime.now()

    if partial:
        asyncio.run(import_partial(partial, tickers, start_date, end_date, months_hist))
    else:
        asyncio.run(import_all(tickers, start_date, end_date, months_hist))


@app.command(name="refresh")
def refresh(
    tickers: Annotated[
        list[str], typer.Argument(help="Underlying tickers to add to the data universe or to include in the pull")
    ],
    all_tickers: Annotated[bool, typer.Option("--all-tickers", "-A", help="Add all stock tickers to the pull")] = False,
    partial: Annotated[
        list[int],
        typer.Option(
            "--partial",
            "-p",
            callback=validate_partial,
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
    ] = DEFAULT_REFRESH_PARTIAL,
    start_date: Annotated[
        datetime,
        typer.Option(
            "--start-date",
            "-s",
            formats=["%Y-%m"],
            help="Start date of data pull (YYYY-MM)",
        ),
    ] = DEFAULT_START_DATE,
    end_date: Annotated[
        datetime, typer.Option("--end-date", "-e", formats=["%Y-%m"], help="End date of data pull (YYYY-MM)")
    ] = DEFAULT_END_DATE,
    months_hist: Annotated[
        int,
        typer.Option(
            "--months-hist",
            "-m",
            help="Months of historical options contracts to pull."
            " **Only works if you DO NOT specify a start/end date**",
        ),
    ] = DEFAULT_MONTHS_HIST,
):
    # TODO: a logic to look up the date of the most recent pull and set as start date
    pass


@app.command(name="remove")
def remove(
    tickers: Annotated[list[str], typer.Argument(help="Underlying tickers to remove from the data universe")],
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
