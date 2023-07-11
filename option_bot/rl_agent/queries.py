from db_tools.schemas import (
    OptionsPricesRaw,
    OptionsTickerModel,
    OptionsTickers,
    StockPricesRaw,
    StockTickers,
    TickerModel,
)
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.utils import Session, two_years_ago


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
async def extract_ticker_price(session: AsyncSession, ticker: str, start_date):
    pass


@Session
async def extract_options_contracts(session: AsyncSession, ticker: str, start_date):
    pass


@Session
async def extract_options_prices(session: AsyncSession, o_tickers: list[str], start_date):
    pass
