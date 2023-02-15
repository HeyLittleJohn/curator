import os

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context
from option_bot.schemas import Base


config = context.config

target_metadata = Base.metadata


def _get_url() -> str:
    with open(os.environ.get("PGPASSFILE"), "r") as f:
        host, port, dbname, user, password = f.read().split(":")
        database_uri = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
    return database_uri


def create_engine_(async_engine: bool = False):
    database_uri = _get_url()
    return create_async_engine(database_uri) if async_engine else create_engine(database_uri)
