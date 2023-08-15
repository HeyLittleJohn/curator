import asyncio
from datetime import datetime
import random

import pandas as pd
from py_vollib_vectorized.api import price_dataframe

from rl_agent.queries import extract_game_market_data
from rl_agent.constants import DAYS_TIL_EXP, ANNUAL_TRADING_DAYS, RISK_FREE
from option_bot.utils import trading_days_in_range
from am_pm.port_tools.calc_funcs import calc_log_returns, calc_pct_returns, calc_hist_volatility


class GameEnvironment(object):
    """The game environment for the reinforcement learning agent.
    This env will support a single underlying ticker and up to 4 options positions
    Supported actions: close, hold position (2)
    The action space will be the number of combinations of actions per position. 2^n where n is [1,4]
    """

    positions = {
        1: ["call", "put"],
        2: ["call_credit_spread", "put_credit_spread", "staddle", "strangle"],
        3: ["double_call_credit_spread", "double_put_credit_spread", "strap", "strip"],
        4: ["iron_condor", "butterfly_spread"],
    }
    long_short = ["long", "short"]

    def __init__(
        self, underlying_ticker: str, start_date: str | datetime, days_to_exp: int = DAYS_TIL_EXP, num_positions=1
    ):
        self.ticker = underlying_ticker
        self.data_start_date = datetime.strptime(start_date, "%Y-%m-%d") if type(start_date) == str else start_date
        self.game_start_date: datetime = None
        self.start_days_to_exp = days_to_exp
        self.days_to_exp = days_to_exp
        self.num_positions = num_positions
        self.end: bool = False
        self.position: str = "short"
        self.state_data_df: pd.DataFrame = pd.DataFrame()
        self.underlying_price_df: pd.DataFrame = pd.DataFrame()

    # NOTE: should this only pull for the current game and be re-called at "reset"?
    async def _pull_game_price_data(self):
        # return await asyncio.gather(
        #     extract_ticker_price(self.ticker),
        #     extract_options_contracts(self.ticker, self.start_date),
        #     extract_options_prices(self.ticker, self.start_date),
        # )
        return await extract_game_market_data(self.ticker, self.data_start_date)

    # NOTE: may want to pull data from before the start date to calc hist volatility, etc

    async def prepare_state_data(self):
        # s_price, o_contracts, o_prices = await self.pull_game_price_data()
        df = await self._pull_game_price_data()
        df["flag"] = df["contract_type"].apply(lambda x: "c" if x.value == "call" else "p")
        # calc the time to expiration
        df["DTE"] = df.apply(
            lambda x: trading_days_in_range(x["as_of_date"], x["expiration_date"], "o_cal"),
            axis=1,
        )  # NOTE: THIS IS SLOW! Need to optimize with cuDF or np.vectorize
        # reference: https://shubhanshugupta.com/speed-up-apply-function-pandas-dataframe/#3-rapids-cudf-
        # the two .apply()s are both slow, especially together
        df["T"] = df["DTE"] / ANNUAL_TRADING_DAYS

        # add the risk free rate
        df = df.merge(RISK_FREE, on="as_of_date")
        # calc the div yield
        # NOTE: div yield is not currently in the db

        price_dataframe(
            df,
            flag_col="flag",
            underlying_price_col="stock_close_price",
            strike_col="strike_price",
            annualized_tte_col="T",
            riskfree_rate_col="risk_free_rate",
            price_col="opt_close_price",
            model="black_scholes",  # _merton when you add dividend yield
            inplace=True,
        )
        df["log_returns"] = calc_log_returns(df["stock_close_price"].to_numpy(dtype="float64"))
        df["pct_returns"] = calc_pct_returns(df["stock_close_price"].to_numpy(dtype="float64"))
        df["hist_90_vol"] = calc_hist_volatility(df["log_returns"].to_numpy(dtype="float64"), 90)
        df["hist_30_vol"] = calc_hist_volatility(df["log_returns"].to_numpy(dtype="float64"), 30)

        self.state_data_df = df
        self.underlying_price_df = (
            df[["as_of_date", "stock_close_price"]].drop_duplicates().sort_values("as_of_date").reset_index(drop=True)
        )

    def _impute_missing_data(self):
        # use this https://github.com/rsheftel/pandas_market_calendars to find missing days
        pass

    def reset(self):
        """self.days_to_exp is the counter within the game
        self.start_days_to_exp is the original value that self.days_to_exp is reset to. Set on class init()"""
        self.days_to_exp = self.start_days_to_exp
        self.game_start_date, self.under_start_price, self.opt_tkr = self._init_random_positions()
        self.game_state = (
            self.state_data_df.loc[
                (self.state_data_df["as_of_date"] >= self.game_start_date)
                & (self.state_data_df["options_ticker"] == self.opt_tkr)
            ]
            .sort_values("as_of_date", ascending=True)
            .reset_index(drop=True)
        )
        self.position = "short"
        # NOTE: may randomly set as long or short, but currently, we are just selling options
        self.end = False

    def step(self):
        """returns the next state, reward, and whether the game is over"""
        self.days_to_exp -= 1

    def _init_random_positions(self):
        """this function initializes the game with random positions.
        It chooses a row from the self.underlying_price_df that is atleast self.days_to_exp positions away from the last row.
        It takes the as_of_date value and the stock_close_price from that row.
        It then filters the self.state_data_df to only include rows with that as_of_date and chooses self.num_positions options contracts whose strike prices are +/- 8 contracts away from the stock_close_price.

        NOTE: may use under_start_price to decide if options should only be in the money or out of the money. But that can be done later.
        Or, to make sure that the strike price is within some range of the underlying price.

        TODO: remove the magic numbers
        """
        ix = random.randint(0, len(self.underlying_price_df) - self.days_to_exp)
        start_date, under_start_price = self.underlying_price_df.iloc[ix].values
        opt_tkrs_df = (
            self.state_data_df.loc[
                (self.state_data_df["as_of_date"] == start_date)
                & (self.state_data_df["DTE"] >= self.days_to_exp)
                & (self.state_data_df["DTE"] <= self.days_to_exp + 15)  # NOTE: this is a magic number
            ]
            .head(50)  # NOTE: this is a magic number
            .sort_values(by=["expiration_date", "opt_number_of_transactions"], ascending=[True, False])
            .reset_index(drop=True)
        )
        opt_tkr = opt_tkrs_df.iloc[random.randint(0, opt_tkrs_df.shape[0])]["options_ticker"]
        return start_date, under_start_price, opt_tkr

    def _calc_reward(self):
        pass

    def _determine_end(self):
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

if __name__ == "__main__":
    pass
