class GameEnvironmnet(object):
    async def __init__(self, underlying_ticker: str, start_date: str, days_to_exp: int = 45):
        self.ticker = underlying_ticker
        self.start_date = start_date
        self.days_to_exp = days_to_exp
        self.end = False
        self.data = await self.pull_game_price_data()

    async def pull_game_price_data(self):
        return

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
