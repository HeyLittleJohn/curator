from argparse import Namespace

# from option_bot.db_tools.uploader import Uploader
from data_pipeline.polygon_utils import PolygonPaginator
from db_tools.queries import delete_stock_ticker

from option_bot.proj_constants import log


async def import_all(args: Namespace, tickers: list[str] = [], refresh: bool = False):
    """this is THE trigger function. It will do the following:

    1. Download all metadata and prices for both stocks and options
    2. It will clean that data
    3. It will upload that data to the database

    The goal is for this to be able to create the process pools required for each step.

    It will toggle if "refreshing" the data or pulling everything fresh.

    Args:
        tickers: list of tickers to import, if empty, will import all
        refresh: bool (default=False)"""
    pass


async def import_partial(components=list[PolygonPaginator], tickers: list[str] = [], refresh: bool = False):
    """This will download, clean, and upload data for the components specified.
    This is meant to be used on an adhoc basis to fill in data gaps or backfill changes

    Args:
        components: list of PolygonPaginator objects
        tickers: list of tickers to import, if empty, will import all
        refresh: bool (default=False)
    """
    pass


async def remove_tickers_from_universe(tickers: list[str]):
    """Deletes the ticker from the stock ticker table which cascades and deletes all associated data."""
    for ticker in tickers:
        log.info(f"deleting ticker {ticker}")
        await delete_stock_ticker(ticker)
        log.info(f"ticker {ticker} successfully deleted")
