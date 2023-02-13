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
