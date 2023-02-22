from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.schemas import OptionsTickers, StockPricesRaw, StockTickers, TickerModel
from option_bot.utils import Session


@Session
async def lookup_ticker_id(session: AsyncSession, ticker_str: str, stock: bool = True):
    table = StockTickers if stock else OptionsTickers
    column = StockTickers.ticker if stock else OptionsTickers.option_ticker
    return (await session.execute(select(table.id).where(column == ticker_str))).scalars().one()


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
