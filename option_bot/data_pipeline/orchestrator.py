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

from option_bot.proj_constants import log


async def import_all(args: Namespace, tickers: list[str] = [], all_: bool = True):
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
    # lookup what used to be "import_all_ticker_metadata()" for options_contracts
    # ticker_lookup = await import_all_ticker_metadata()
    # ticker_lookup = {list(x.keys())[0]: list(x.values())[0] for x in ticker_lookup}

    await download_stock_metadata(tickers, all_)
    await upload_stock_metadata(tickers, all_)
    # await download_stock_prices(tickers, args.startdate, args.enddate, all_)
    # await upload_stock_prices(tickers, all_)
    # TODO: need to add an all_ arg to options_contracts
    await download_options_contracts(tickers, args.month_hist, all_)
    await upload_options_contracts(tickers, all_)
    await download_options_prices(tickers, args.month_hist, all_)
    await upload_options_prices(tickers, all_)

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
