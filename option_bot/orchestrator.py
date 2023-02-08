import asyncio
from multiprocessing import Lock, Pool

from polygon import HistoricalOptionsPrices, OptionsContracts


def add_tickers_to_universe():
    """Chain together the following process:
    Pull ticker data, pull ticker price data, save price data, analyze ticker info,\
    pass output to Options queries, pull options tickers, pull options prices, \
    save tickers and prices, calculate greeks
    """
    pass


def remove_ticker_from_universe():
    pass
