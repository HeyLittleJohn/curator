from math import floor
from multiprocessing import cpu_count  # , Lock, Pool

from polygon import StockMetaData


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


def fetch_stock_data(ticker: str, all_: bool = True):
    meta = StockMetaData(ticker, all_)
    meta.get_data()
    # TODO: write results to the DB
