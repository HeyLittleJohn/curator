import asyncio
import json
from math import floor
from multiprocessing import cpu_count  # , Lock, Pool

from db_manager import update_stock_metadata
from polygon import StockMetaData


# from schemas import TickerModel


# from polygon import HistoricalOptionsPrices, HistoricalStockPrices, OptionsContracts


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
        pass
    # TODO: Figure out the classes for the paginator, and how to run within multiple processes
    # NOTE: pass in the number of processes being used when this function is called (num of tickers)\
    #  so that the calls to get historical prices can be made with multiple processes as well


def remove_ticker_from_universe():
    pass


async def fetch_stock_data(ticker: str = "", all_: bool = True):
    meta = StockMetaData(ticker, all_)
    await meta.get_data()
    meta.clean_metadata()
    await update_stock_metadata(meta.clean_results)


if __name__ == "__main__":
    with open("stocks_file", "r") as f:
        data = json.loads(f.read())
    meta = StockMetaData("", True)
    meta.results = [data]
    meta.clean_metadata()
    clean_data_batches = meta.clean_data_generator()
    for batch in clean_data_batches:
        asyncio.run(update_stock_metadata(batch))
