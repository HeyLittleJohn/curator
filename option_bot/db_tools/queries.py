from datetime import datetime

import pandas as pd
from data_pipeline.exceptions import InvalidArgs
from db_tools.schemas import (
    OptionPriceModel,
    OptionsPricesRaw,
    OptionsQuotesRaw,
    OptionsSnapshot,
    OptionsSnapshotModel,
    OptionsTickerModel,
    OptionsTickers,
    QuoteModel,
    StockPriceModel,
    StockPricesRaw,
    StockTickers,
    TickerModel,
)
from sqlalchemy import case, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.utils import Session, months_ago


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
    session: AsyncSession,
    stock_tickers: list[str],
    batch: list[dict] | None = None,
    months_hist: int = 24,
    all_=False,
    unexpired=False,
) -> list[OptionsTickerModel]:
    """
    This is to pull all options contracts for a given underlying ticker.
    The batch input is to only pull o_ticker_ids for given o_tickers
    """
    # NOTE: may need to adjust to not pull all columns from table
    if batch and all_:
        raise InvalidArgs("Can't have query all_ and a batch")

    stmt = (
        select(
            OptionsTickers.options_ticker,
            OptionsTickers.id,
            OptionsTickers.expiration_date,
            StockTickers.ticker,
        )
        .join(StockTickers)
        .where(StockTickers.type.in_(["ADRC", "ETF", "CS"]))
    )
    if not all_:
        stmt = stmt.where(StockTickers.ticker.in_(stock_tickers))
    if batch:
        batch_tickers = [x["options_ticker"] for x in batch]
        stmt = stmt.where(OptionsTickers.options_ticker.in_(batch_tickers))
    if unexpired:
        stmt = stmt.where(OptionsTickers.expiration_date >= datetime.now().date())
    else:
        stmt = stmt.where(OptionsTickers.expiration_date > months_ago(months_hist))

    return (await session.execute(stmt)).all()


@Session
async def query_stock_tickers(
    session: AsyncSession, all_: bool = True, tickers: list[str] = []
) -> list[TickerModel]:
    """only returns tickers likely to have options contracts"""
    stmt = select(StockTickers.id, StockTickers.ticker).where(StockTickers.type.in_(["ADRC", "ETF", "CS"]))
    if not all_:
        stmt = stmt.where(StockTickers.ticker.in_(tickers))
    return (await session.execute(stmt)).all()


@Session
async def ticker_imported(session: AsyncSession, ticker_id: int):
    """Function updates the StockTickers table to indicate a stock's prices have been imported

    Args:
        ticker_id: int
        The pk_id of the ticker that has been imported"""
    return await session.execute(
        update(StockTickers).where(StockTickers.id == ticker_id).values(imported=True)
        # .returning(StockTickers.ticker, StockTickers.imported)
    )  # .one()


@Session
async def update_stock_metadata(session: AsyncSession, data: list[TickerModel]):
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=["ticker"], keep="last")
    data = df.to_dict(orient="records")
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
    data: list[StockPriceModel],
):
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
async def update_options_tickers(session: AsyncSession, data: list[OptionsTickerModel]):
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
    data: list[OptionPriceModel],
):
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
async def update_options_snapshot(session: AsyncSession, data: list[OptionsSnapshotModel]):
    stmt = insert(OptionsSnapshot).values(data)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_options_snapshot",
        set_=dict(
            implied_volatility=stmt.excluded.implied_volatility,
            delta=stmt.excluded.delta,
            gamma=stmt.excluded.gamma,
            theta=stmt.excluded.theta,
            vega=stmt.excluded.vega,
            open_interest=stmt.excluded.open_interest,
            is_overwritten=True,
        ),
    )
    return await session.execute(stmt)


@Session
async def update_options_quotes(session: AsyncSession, data: list[QuoteModel]):
    stmt = insert(OptionsQuotesRaw).values(data)
    return await session.execute(stmt)


@Session
async def delete_stock_ticker(session: AsyncSession, ticker: str):
    await session.execute(delete(StockTickers).where(StockTickers.ticker == ticker))


@Session
async def latest_date_per_ticker(session: AsyncSession, tickers: list[str] = [], options=False):
    """query to retrieve the most recent date of record for ticker data (prices/quotes for stock/options).
    If tickers is [] empty, return all
    """

    if options:
        # Query for options data
        subquery = (
            select(
                OptionsTickers.id.label("ticker_id"),
                func.max(
                    case(
                        (
                            OptionsQuotesRaw.as_of_date < OptionsTickers.expiration_date,
                            OptionsQuotesRaw.as_of_date,
                        ),
                        else_=None,
                    )
                ).label("latest_quote_date"),
                func.max(
                    case(
                        (
                            OptionsPricesRaw.as_of_date < OptionsTickers.expiration_date,
                            OptionsPricesRaw.as_of_date,
                        ),
                        else_=None,
                    )
                ).label("latest_price_date"),
            )
            .join(StockTickers, StockTickers.id == OptionsTickers.underlying_ticker_id)
            .outerjoin(OptionsQuotesRaw, OptionsQuotesRaw.options_ticker_id == OptionsTickers.id)
            .outerjoin(OptionsPricesRaw, OptionsPricesRaw.options_ticker_id == OptionsTickers.id)
            .where(or_(StockTickers.ticker.in_(tickers), len(tickers) == 0))
            .group_by(OptionsTickers.id)
            .subquery()
        )

        query = (
            select(
                StockTickers.ticker,
                OptionsTickers.options_ticker,
                subquery.c.latest_price_date,
                subquery.c.latest_quote_date,
            )
            .join(subquery, subquery.c.ticker_id == OptionsTickers.id)
            .join(StockTickers, StockTickers.id == OptionsTickers.underlying_ticker_id)
        )

    else:
        # Query for stock data
        subquery = (
            select(StockPricesRaw.ticker_id, func.max(StockPricesRaw.as_of_date).label("latest_date"))
            .join(StockTickers, StockTickers.id == StockPricesRaw.ticker_id)
            .where(or_(StockTickers.ticker.in_(tickers), len(tickers) == 0))
            .group_by(StockPricesRaw.ticker_id)
            .subquery()
        )

        query = select(StockTickers.ticker, subquery.c.latest_date).join(
            subquery, subquery.c.ticker_id == StockTickers.id
        )

    return (await session.execute(query)).all()
