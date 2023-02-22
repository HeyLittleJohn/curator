import asyncio
from datetime import datetime
from math import floor
from multiprocessing import cpu_count  # , Lock, Pool

from db_manager import lookup_ticker_id, update_stock_metadata, update_stock_prices
from polygon_utils import HistoricalStockPrices, StockMetaData


# from schemas import TickerModel


# from polygon import HistoricalOptionsPrices, , OptionsContracts


CPUS = cpu_count()


def add_tickers_to_universe(kwargs_list):
    """Chain together the following process:
    Pull ticker data, pull ticker price data, save price data, analyze ticker info,\
    pass output to Options queries, pull options tickers, pull options prices, \
    save tickers and prices, calculate greeks
    """
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
    await meta.query_data()
    meta.clean_metadata()
    clean_data_batches = meta.clean_data_generator()
    for batch in clean_data_batches:
        await update_stock_metadata(batch)


async def fetch_stock_prices(ticker: str, start_date: str, end_date: str, all_: bool = False):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    ticker_id = await lookup_ticker_id(ticker, stock=True)
    prices = HistoricalStockPrices(ticker, ticker_id, start_date, end_date)
    await prices.query_data()
    prices.clean_data()
    await update_stock_prices(prices.clean_results)


if __name__ == "__main__":
    asyncio.run(fetch_stock_prices("SPY", "2021-03-01", "2023-02-21"))
