from datetime import datetime

from data_pipeline.download import (
    download_options_contracts,
    download_options_prices,
    download_options_quotes,
    download_options_snapshots,
    download_stock_metadata,
    download_stock_prices,
)
from data_pipeline.uploader import (
    upload_options_contracts,
    upload_options_prices,
    upload_options_quotes,
    upload_options_snapshots,
    upload_stock_metadata,
    upload_stock_prices,
)
from db_tools.queries import delete_stock_ticker, latest_date_per_ticker
from db_tools.utils import generate_o_ticker_lookup, pull_tickers_from_db
from pandas import DataFrame

from curator.proj_constants import log


async def import_all(tickers: list, start_date: datetime, end_date: datetime, months_hist: int):
    """this is THE trigger function. It will do the following:

    1. Download all metadata and prices for both stocks and options
    2. It will clean that data
    3. It will upload that data to the database

    The goal is for this to be able to create the process pools required for each step.

    May add toggle if "refreshing" the data or pulling everything fresh.
    As of now, refreshing won't change anything. Pull all history everytime.
    """
    all_ = True if len(tickers) == 0 else False
    # Download and upload metadata

    await download_stock_metadata(tickers=tickers, all_=all_)
    await upload_stock_metadata(tickers=tickers, all_=all_)

    # Get ticker_id_lookup from db
    ticker_lookup = await pull_tickers_from_db(tickers, all_=all_)

    # Download and upload options contract data
    await download_options_contracts(ticker_id_lookup=ticker_lookup, months_hist=months_hist)
    await upload_options_contracts(ticker_lookup, months_hist=months_hist)

    # Download and upload current snapshot of options contracts
    o_tickers = await generate_o_ticker_lookup(tickers, all_=all_, unexpired=True)

    await download_options_snapshots(list(o_tickers.values()))
    await upload_options_snapshots(o_tickers)

    # Download and upload options prices data
    o_tickers = await generate_o_ticker_lookup(tickers, all_=all_)

    await download_options_prices(o_tickers=list(o_tickers.values()), months_hist=months_hist)
    await upload_options_prices(o_tickers)

    # Download and upload quotes
    final_tickers = list(ticker_lookup.keys())
    failed_paths = []
    ticker_counter = 0
    for ticker in final_tickers:
        ticker_counter += 1
        log.info(f"downloading quotes for {ticker} ({ticker_counter}/{len(final_tickers)})")
        download_options_quotes(ticker=ticker, o_tickers=list(o_tickers.values()), months_hist=months_hist)
        temp_paths = upload_options_quotes(ticker)
        failed_paths.append(temp_paths)
    log.info(f"failed to parse these paths: {failed_paths}")
    log.info("-- Done Uploading Quote Data")

    # Download and upload underlying stock prices
    # NOTE: at the end due to it being the free api
    await download_stock_prices(ticker_lookup, start_date, end_date)
    await upload_stock_prices(ticker_lookup)


async def import_partial(
    partial: list[int], tickers: list, start_date: datetime, end_date: datetime, months_hist: int
):
    """This will download, clean, and upload data for the components specified in `partial`
    This is meant to be used on an adhoc basis to fill in data gaps or backfill changes
    """

    all_ = True if len(tickers) == 0 else False
    ticker_lookup = None
    o_tickers = None

    if 1 in partial:  # stock metadata
        await download_stock_metadata(tickers=tickers, all_=all_)
        await upload_stock_metadata(tickers=tickers, all_=all_)

    if 2 in partial:  # stock prices
        ticker_lookup = await pull_tickers_from_db(tickers, all_)
        await download_stock_prices(ticker_lookup, start_date, end_date)
        await upload_stock_prices(ticker_lookup)

    if 3 in partial:  # options contracts
        if not ticker_lookup:
            ticker_lookup = await pull_tickers_from_db(tickers, all_)
        await download_options_contracts(ticker_id_lookup=ticker_lookup, months_hist=months_hist)
        await upload_options_contracts(ticker_lookup, months_hist=months_hist)

    if 4 in partial:  # options prices
        o_tickers = await generate_o_ticker_lookup(tickers, all_=all_)
        await download_options_prices(o_tickers=list(o_tickers.values()), months_hist=months_hist)
        await upload_options_prices(o_tickers)

    if 5 in partial:  # snapshots
        if not o_tickers:
            o_tickers = await generate_o_ticker_lookup(tickers, all_=all_, unexpired=True)
        await download_options_snapshots(list(o_tickers.values()))
        await upload_options_snapshots(o_tickers)

    if 6 in partial:  # quotes
        if not ticker_lookup:
            ticker_lookup = await pull_tickers_from_db(tickers, all_)
        if not o_tickers:
            o_tickers = await generate_o_ticker_lookup(tickers, all_=all_)

        final_tickers = list(ticker_lookup.keys())
        failed_paths = []
        ticker_counter = 0
        for ticker in final_tickers:
            ticker_counter += 1
            log.info(f"downloading quotes for {ticker}  ({ticker_counter}/{len(final_tickers)})")
            await download_options_quotes(
                ticker=ticker, o_tickers=list(o_tickers.values()), months_hist=months_hist
            )
            temp_paths = await upload_options_quotes(ticker)
            failed_paths.append(temp_paths)
        log.info(f"failed to parse these paths: {failed_paths}")
        log.info("-- Done Uploading Quote Data")


async def remove_tickers_from_universe(tickers: list[str]):
    """Deletes the ticker from the stock ticker table which cascades and deletes all associated data."""
    for ticker in tickers:
        log.info(f"deleting ticker {ticker}")
        await delete_stock_ticker(ticker)
        log.info(f"ticker {ticker} successfully deleted")


async def refresh_import(
    tickers: list, start_date: datetime, end_date: datetime, months_hist: int, partial: list[int]
):
    """refreshes all or partial components for the given tickers
    start date and end date is set to today, start date from the most recent going back to 24 months at most"""
    all_ = True if len(tickers) == 0 else False

    await download_stock_metadata(tickers=tickers, all_=all_)
    await upload_stock_metadata(tickers=tickers, all_=all_)

    ticker_lookup = await pull_tickers_from_db(tickers, all_)

    if 2 in partial:
        dates = await latest_date_per_ticker(tickers=tickers, options=False)
        dates = DataFrame(dates)
        min_date = dates["latest_date"].min() if dates["latest_date"].min() >= start_date else start_date
        await download_stock_prices(ticker_lookup, min_date, end_date)
        await upload_stock_prices(ticker_lookup)

    if 3 in partial:
        await download_options_contracts(ticker_id_lookup=ticker_lookup, months_hist=months_hist)
        await upload_options_contracts(ticker_lookup, months_hist=months_hist)

    if 4 in partial:
        o_tickers = await generate_o_ticker_lookup(tickers, all_=all_)
        dates = await latest_date_per_ticker(tickers=tickers, options=True)


# if __name__ == "__main__":
#     o_tickers = asyncio.run(generate_o_ticker_lookup(tickers=[], all_=True))
#     asyncio.run(upload_options_prices(o_tickers))
