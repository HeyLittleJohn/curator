from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.exceptions import InvalidArgs
from option_bot.schemas import (
    OptionsPricesRaw,
    OptionsTickerModel,
    OptionsTickers,
    StockPricesRaw,
    StockTickers,
    TickerModel,
)
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
async def lookup_multi_ticker_ids(session: AsyncSession, ticker_list: list[str], stock: bool = True):
    """Function to find the pk_id of a given ticker. Handles both Stock and Option ticker lookups

    Args:
        ticker_list: list[str]
        This is the canonical ticker (like SPY or O:SPY251219C00650000)

        stock: bool (default=True)
        An indicator of whether this is a stock (default) or option ticker being looked up

    Returns:
        ticker: str
        ticker_ids: tuple(int)
        The pk_id of the ticker on either the StockTickers or OptionsTickers tables"""
    table = StockTickers if stock else OptionsTickers
    column = StockTickers.ticker if stock else OptionsTickers.options_ticker
    return (await session.execute(select(column, table.id).where(column.in_(ticker_list)))).all()


@Session
async def query_options_tickers(
    session: AsyncSession, stock_tickers: list[str], batch: list[dict] | None = None, all_=False
) -> list[OptionsTickerModel]:
    """
    This is to pull all options contracts for a given underlying ticker.
    The batch input is to only pull o_ticker_ids for given o_tickers
    """
    # NOTE: may need to adjust to not pull all columns from table
    if batch and all_:
        raise InvalidArgs("Can't have query all_ and a batch")

    stmt = (
        select(OptionsTickers.options_ticker, OptionsTickers.id, OptionsTickers.expiration_date, StockTickers.ticker)
        .join(StockTickers)
        .where(StockTickers.type.in_(["ADRC", "ETF", "CS"]))
        .where(OptionsTickers.expiration_date > two_years_ago())
    )
    if not all_:
        stmt = stmt.where(StockTickers.ticker.in_(stock_tickers))
    if batch:
        batch_tickers = [x["options_ticker"] for x in batch]
        stmt = stmt.where(OptionsTickers.options_ticker.in_(batch_tickers))

    return (await session.execute(stmt)).all()


@Session
async def query_all_stock_tickers(session: AsyncSession) -> list[TickerModel]:
    """only returns tickers likely to have options contracts"""
    return (
        await session.execute(
            select(StockTickers.id, StockTickers.ticker).where(StockTickers.type.in_(["CS", "ADRC", "ETF"]))
        )
    ).all()


@Session
async def ticker_imported(session: AsyncSession, ticker_id: int):
    """Function updates the StockTickers table to indicate a stock's prices have been imported

    Args:
        ticker_id: int
        The pk_id of the ticker that has been imported"""
    return await session.execute(
        update(StockTickers)
        .where(StockTickers.id == ticker_id)
        .values(imported=True)
        # .returning(StockTickers.ticker, StockTickers.imported)
    )  # .one()


@Session
async def update_stock_metadata(session: AsyncSession, data: list[TickerModel]):
    stmt = insert(StockTickers).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["ticker"],
        set_=dict(
            name=stmt.excluded.name,
            type=stmt.excluded.type,
            active=stmt.excluded.active,
            market=stmt.excluded.market,
            locale=stmt.excluded.locale,
            primary_exchange=stmt.excluded.primary_exchange,
            currency_name=stmt.excluded.currency_name,
            cik=stmt.excluded.cik,
        ),
    )
    return await session.execute(stmt)


@Session
async def update_stock_prices(
    session: AsyncSession,
    data: list[dict],
):
    # TODO: update list[dict] to list[StockPriceModel]
    stmt = insert(StockPricesRaw).values(data)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_stock_price",
        set_=dict(
            as_of_date=stmt.excluded.as_of_date,
            close_price=stmt.excluded.close_price,
            open_price=stmt.excluded.open_price,
            high_price=stmt.excluded.high_price,
            low_price=stmt.excluded.low_price,
            volume_weight_price=stmt.excluded.volume_weight_price,
            volume=stmt.excluded.volume,
            number_of_transactions=stmt.excluded.number_of_transactions,
            otc=stmt.excluded.otc,
            is_overwritten=True,
        ),
    )
    return await session.execute(stmt)


@Session
async def update_options_tickers(session: AsyncSession, data: list[dict]):
    stmt = insert(OptionsTickers).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["options_ticker"],
        set_=dict(
            underlying_ticker_id=stmt.excluded.underlying_ticker_id,
            expiration_date=stmt.excluded.expiration_date,
            strike_price=stmt.excluded.strike_price,
            contract_type=stmt.excluded.contract_type,
            shares_per_contract=stmt.excluded.shares_per_contract,
            cfi=stmt.excluded.cfi,
            exercise_style=stmt.excluded.exercise_style,
            primary_exchange=stmt.excluded.primary_exchange,
        ),
    )
    await session.execute(stmt)


@Session
async def update_options_prices(
    session: AsyncSession,
    data: list[dict],
):
    # TODO: update list[dict] to list[StockPriceModel]
    stmt = insert(OptionsPricesRaw).values(data)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_options_price",
        set_=dict(
            as_of_date=stmt.excluded.as_of_date,
            close_price=stmt.excluded.close_price,
            open_price=stmt.excluded.open_price,
            high_price=stmt.excluded.high_price,
            low_price=stmt.excluded.low_price,
            volume_weight_price=stmt.excluded.volume_weight_price,
            volume=stmt.excluded.volume,
            number_of_transactions=stmt.excluded.number_of_transactions,
            is_overwritten=True,
        ),
    )
    return await session.execute(stmt)


@Session
async def delete_stock_ticker(session: AsyncSession, ticker: str):
    session.execute(delete(StockTickers).where(StockTickers.ticker == ticker))
