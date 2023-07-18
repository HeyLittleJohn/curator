from db_tools.schemas import (
    OptionsPricesRaw,
    OptionsTickers,
    StockPricesRaw,
    StockTickers,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.utils import Session


@Session
async def lookup_ticker_id(session: AsyncSession, ticker_str: str, stock: bool = True) -> int:
    """Function to find the pk_id of a given ticker. Handles both Stock and Option ticker lookups

    Args:
        ticker_str: str
        This is the canonical ticker (like SPY or O:SPY251219C00650000)

        stock: bool (default=True)
        An indicator of whether this is a stock (default) or option ticker being looked up

    Returns:
        ticker_id: int
        The pk_id of the ticker on either the StockTickers or OptionsTickers tables"""
    table = StockTickers if stock else OptionsTickers
    column = StockTickers.ticker if stock else OptionsTickers.options_ticker
    return (await session.execute(select(table.id, column).where(column == ticker_str))).scalars().one()


@Session
async def extract_ticker_price(session: AsyncSession, ticker: str):
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
async def extract_options_contracts(session: AsyncSession, ticker: str, start_date):
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
async def extract_options_prices(session: AsyncSession, ticker: str, start_date):
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
