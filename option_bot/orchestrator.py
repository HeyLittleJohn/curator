import asyncio
from datetime import datetime
from math import floor
from multiprocessing import cpu_count  # , Lock, Pool

from db_manager import lookup_ticker_id, ticker_imported, update_stock_metadata, update_stock_prices
from polygon_utils import HistoricalStockPrices, OptionsContracts, StockMetaData


# from schemas import TickerModel


# from polygon import HistoricalOptionsPrices, , OptionsContracts


CPUS = cpu_count()
# NOTE Chain together the following process:
# Pull ticker data, pull ticker price data, save price data, analyze ticker info,\
# pass output to Options queries, pull options tickers, pull options prices, \
# save tickers and prices, calculate greeks


def add_tickers_to_universe(kwargs_list):

    cpus_per_stock = floor(CPUS / len(kwargs_list))
    remaining_cpus = CPUS - cpus_per_stock * len(kwargs_list)
    for i in kwargs_list:
        print(remaining_cpus)
        pass
    # TODO: Figure out the classes for the paginator, and how to run within multiple processes
    # NOTE: pass in the number of processes being used when this function is called (num of tickers)\
    #  so that the calls to get historical prices can be made with multiple processes as well


def remove_ticker_from_universe():
    pass


async def fetch_stock_metadata(ticker: str = "", all_: bool = True):
    meta = StockMetaData(ticker, all_)
    await meta.fetch()
    for batch in meta.clean_data_generator:
        await update_stock_metadata(batch)


async def fetch_stock_prices(ticker: str, start_date: str, end_date: str, all_: bool = False):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    ticker_id = await lookup_ticker_id(ticker, stock=True)
    prices = HistoricalStockPrices(ticker, ticker_id, start_date, end_date)
    await prices.fetch()
    for batch in prices.clean_data_generator:
        await update_stock_prices(batch)
    await ticker_imported(ticker_id)


async def test_query_query():
    results = await ticker_imported(9912)  # testing SPY id lookup
    print(results)


async def fetch_options_contracts(ticker: str, months_hist: int = 24, cpu_count: int = 1, all_=False):
    # NOTE: if refreshing, just pull the current month, months_hist = 1
    ticker_id = await lookup_ticker_id(ticker, stock=True)
    options = OptionsContracts(ticker, ticker_id, months_hist, cpu_count, all_)
    await options.fetch()


if __name__ == "__main__":

    asyncio.run(fetch_options_contracts("SPY", 24, 3))
