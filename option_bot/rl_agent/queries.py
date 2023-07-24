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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.utils import Session


@Session
async def extract_ticker_price(session: AsyncSession, ticker: str) -> list[StockPriceModel]:
    stmt = (
        select(
            StockPricesRaw.close_price,
            StockPricesRaw.as_of_date,
            StockPricesRaw.volume,
            StockPricesRaw.number_of_transactions,
        )
        .join(StockTickers)
        .where(StockTickers.ticker == ticker)
    )
    return (await session.execute(stmt)).all()


@Session
async def extract_options_contracts(session: AsyncSession, ticker: str, start_date: str) -> list[OptionsTickerModel]:
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
    return (await session.execute(stmt)).all()


@Session
async def extract_options_prices(session: AsyncSession, ticker: str, start_date: str) -> list[OptionPriceModel]:
    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    stmt = (
        select(
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
    return (await session.execute(stmt)).all()
