from datetime import datetime
from db_tools.schemas import (
    OptionsPricesRaw,
    OptionsTickers,
    StockPricesRaw,
    StockTickers,
    StockPriceModel,
    OptionPriceModel,
    OptionsTickerModel,
)
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd

from option_bot.utils import Session


@Session
async def extract_ticker_price(session: AsyncSession, ticker: str) -> list[tuple]:
    stmt = (
        select(
            StockPricesRaw.as_of_date,
            StockPricesRaw.close_price,
            StockPricesRaw.volume,
            StockPricesRaw.number_of_transactions,
        )
        .join(StockTickers)
        .where(StockTickers.ticker == ticker)
    )
    data = (await session.execute(stmt)).all()
    return pd.DataFrame.from_records(data, columns=["as_of_date", "close_price", "volume", "number_of_transactions"])


@Session
async def extract_options_contracts(session: AsyncSession, ticker: str, start_date: str) -> list[tuple]:
    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    stmt = (
        select(
            OptionsTickers.options_ticker,
            OptionsTickers.expiration_date,
            OptionsTickers.strike_price,
            OptionsTickers.contract_type,
            OptionsTickers.shares_per_contract,
        )
        .join(StockTickers)
        .where(StockTickers.ticker == ticker)
        .where(OptionsTickers.expiration_date >= start_date)
    )
    data = (await session.execute(stmt)).all()
    return pd.DataFrame.from_records(
        data, columns=["options_ticker", "expiration_date", "strike_price", "contract_type", "shares_per_contract"]
    )


@Session
async def extract_options_prices(session: AsyncSession, ticker: str, start_date: str) -> list[tuple]:
    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    stmt = (
        select(
            OptionsTickers.options_ticker,
            OptionsPricesRaw.as_of_date,
            OptionsPricesRaw.close_price,
            OptionsPricesRaw.volume,
            OptionsPricesRaw.number_of_transactions,
        )
        .join(OptionsTickers)
        .join(StockTickers)
        .where(StockTickers.ticker == ticker)
        .where(OptionsPricesRaw.as_of_date >= start_date)
    )
    data = (await session.execute(stmt)).all()
    return pd.DataFrame.from_records(
        data, columns=["options_ticker", "as_of_date", "opt_close_price", "opt_volume", "opt_number_of_transactions"]
    )


@Session
async def extract_game_market_data(session: AsyncSession, ticker: str, start_date: str) -> list[tuple]:
    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    stmt = (
        select(
            StockPricesRaw.as_of_date,
            StockPricesRaw.close_price.label("stock_close_price"),
            StockPricesRaw.volume.label("stock_volume"),
            StockPricesRaw.number_of_transactions.label("stock_number_of_transactions"),
            OptionsTickers.options_ticker,
            OptionsTickers.expiration_date,
            OptionsTickers.strike_price,
            OptionsTickers.contract_type,
            OptionsTickers.shares_per_contract,
            OptionsPricesRaw.close_price.label("opt_close_price"),
            OptionsPricesRaw.volume.label("opt_volume"),
            OptionsPricesRaw.number_of_transactions.label("opt_number_of_transactions"),
        )
        .select_from(StockTickers)
        .join(StockPricesRaw)
        .join(OptionsTickers)
        .join(OptionsPricesRaw)
        .where(StockTickers.ticker == ticker)
        .where(OptionsTickers.expiration_date >= start_date)
        .where(OptionsPricesRaw.as_of_date >= start_date)
        .filter(OptionsPricesRaw.as_of_date == StockPricesRaw.as_of_date)
    )
    print(stmt)
    data = (await session.execute(stmt)).all()
    return pd.DataFrame.from_records(
        data,
        columns=[
            "as_of_date",
            "stock_close_price",
            "stock_volume",
            "stock_number_of_transactions",
            "options_ticker",
            "expiration_date",
            "strike_price",
            "contract_type",
            "shares_per_contract",
            "opt_close_price",
            "opt_volume",
            "opt_number_of_transactions",
        ],
    )


# NOTE: didn't work for this: df = await extract_game_state("SPY", "2022-01-01")
# It was killed. Not sure the error
