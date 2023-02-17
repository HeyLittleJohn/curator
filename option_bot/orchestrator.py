from multiprocessing import cpu_count  # , Lock, Pool


# from polygon import HistoricalOptionsPrices, HistoricalStockPrices, OptionsContracts


CPUS = cpu_count()


def add_tickers_to_universe(kwargs_list):
    """Chain together the following process:
    Pull ticker data, pull ticker price data, save price data, analyze ticker info,\
    pass output to Options queries, pull options tickers, pull options prices, \
    save tickers and prices, calculate greeks
    """
    for i in kwargs_list:
        pass
    # TODO: Figure out the classes for the paginator, and how to run within multiple processes
    # NOTE: pass in the number of processes being used when this function is called (num of tickers)\
    #  so that the calls to get historical prices can be made with multiple processes as well


def remove_ticker_from_universe():
    pass
