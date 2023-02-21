from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncSession

from option_bot.schemas import StockTickers, TickerModel
from option_bot.utils import Session


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
