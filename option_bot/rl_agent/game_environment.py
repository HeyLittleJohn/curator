import asyncio

from rl_agent.queries import extract_ticker_price, extract_options_contracts, extract_options_prices


class GameEnvironmnet(object):
    async def __init__(self, underlying_ticker: str, start_date: str, days_to_exp: int = 45):
        self.ticker = underlying_ticker
        self.start_date = start_date
        self.days_to_exp = days_to_exp
        self.end = False
        self.data = await self.pull_game_price_data()

    # NOTE: should this only pull for the current game and be re-called at "reset"?
    async def pull_game_price_data(self):
        return await asyncio.gather(
            extract_ticker_price(self.ticker, self.start_date),
            extract_options_contracts(self.ticker, self.start_date),
            extract_options_prices(o_contracts, self.start_date),
        )

    async def prepare_state_data(self):
        s_price, o_contracts, o_prices = await self.pull_game_price_data()
        pass

    def reset(self):
        pass

    def step():
        pass


# NOTE: this could be used for the logic of pulling the prices to be considered for each step
"""    Attributes:
        options_tickers: List[str]
            the options contract tickers

        exp_date: [datetime, datetime]
            the range of option expiration dates to be queried

        strike_price: decimal
            the strike price range want to include in our queries

    Note:
        exp_date and strike_price are inclusive ranges"""

"""
    Attributes:
        ticker: str
            the underlying stock ticker
        base_date: [datetime]
            the date that is the basis for current observations. \
            In other words: the date at which you are looking at the chain of options data
        current_price: decimal
            The current price of the underlying ticker
    """
