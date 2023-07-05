from argparse import Namespace

from data_pipeline.download import (
    download_options_contracts,
    download_options_prices,
    download_stock_metadata,
)
from data_pipeline.polygon_utils import PolygonPaginator
from data_pipeline.uploader import (
    upload_options_contracts,
    upload_options_prices,
    upload_stock_metadata,
)
from db_tools.queries import delete_stock_ticker
from db_tools.utils import generate_o_ticker_lookup, pull_tickers_from_db

from option_bot.proj_constants import log


async def import_all(args: Namespace):
    """this is THE trigger function. It will do the following:

    1. Download all metadata and prices for both stocks and options
    2. It will clean that data
    3. It will upload that data to the database

    The goal is for this to be able to create the process pools required for each step.

    May add toggle if "refreshing" the data or pulling everything fresh.
    As of now, refreshing won't change anything. Pull all history everytime.


    Args:
        tickers: list of tickers to import
        all_: bool (default=True) indicating whether to retrieve data for all tickers
    """
    # Download and upload metadata
    tickers = args.tickers if args.tickers else []
    all_ = True if args.add_all else False
    await download_stock_metadata(tickers=[], all_=True)
    await upload_stock_metadata(tickers=[], all_=True)

    # Get ticker_id_lookup from db
    ticker_lookup = await pull_tickers_from_db(tickers, all_)

    # Download and upload underlying stock prices
    # await download_stock_prices(tickers, args.startdate, args.enddate, all_)
    # await upload_stock_prices(tickers, all_)

    # Download and upload options contract data
    await download_options_contracts(ticker_id_lookup=ticker_lookup, months_hist=args.monthhist)
    await upload_options_contracts(ticker_lookup, months_hist=args.monthhist)

    # Get o_ticker_lookup from db
    o_tickers = await generate_o_ticker_lookup(tickers, all_=all_)

    # Download and upload options prices data
    await download_options_prices(o_tickers=list(o_tickers.values()), months_hist=args.monthhist)
    await upload_options_prices(o_tickers)

    # NOTE: the args from one download step can be returned and passed to the next step.
    # This can prevent the double pulling of ticker_ids from the db. Maybe not worth it


# NOTE: this may never need to be implemented
async def import_partial(components=list[PolygonPaginator], tickers: list[str] = [], all_: bool = True):
    """This will download, clean, and upload data for the components specified.
    This is meant to be used on an adhoc basis to fill in data gaps or backfill changes

    Args:
        components: list of PolygonPaginator objects
        tickers: list of tickers to import
        all_: bool (default=True) indicating whether to retrieve data for all tickers
    """
    pass


async def remove_tickers_from_universe(tickers: list[str]):
    """Deletes the ticker from the stock ticker table which cascades and deletes all associated data."""
    for ticker in tickers:
        log.info(f"deleting ticker {ticker}")
        await delete_stock_ticker(ticker)
        log.info(f"ticker {ticker} successfully deleted")
