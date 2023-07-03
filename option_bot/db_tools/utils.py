from collections import namedtuple

from db_tools.queries import query_options_tickers, query_stock_tickers


OptionTicker = namedtuple("OptionTicker", ["o_ticker", "id", "expiration_date", "underlying_ticker"])


async def pull_tickers_from_db(tickers: list[str] = [], all_: bool = True) -> list[dict]:
    # TODO: rename this function
    """This function pulls all stock tickers from the db and returns a list of tickers.

    Returns:
        ticker_lookup: list[dict], each dict will be a {"ticker": "ticker_id"} pair.

    eg: [{'AAWW': 125}, {'ABGI': 138}, {'AA': 94}]
    """
    ticker_results = await query_stock_tickers(tickers=tickers, all_=all_)
    ticker_lookup = {x[1]: x[0] for x in ticker_results}
    return ticker_lookup


async def generate_o_ticker_lookup(tickers: list[str], all_: bool = False) -> dict[str, OptionTicker]:
    """Function to prepare a lookup of o_tickers to o_ticker info based on a list of underlying stock tickers
    Args:
        tickers: list of stock tickers
        all_: bool (default=False) indicating whether to retrieve all o_tickers

    Returns:
        o_ticker_lookup: dict[str, OptionTicker]
        A dict of o_tickers to OptionTicker tuple objects
    """
    if all_:
        o_tickers = await query_options_tickers(stock_tickers=["all_"], all_=True)
    else:
        o_tickers = await query_options_tickers(stock_tickers=tickers)
    return {x[0]: OptionTicker(*x) for x in o_tickers}
