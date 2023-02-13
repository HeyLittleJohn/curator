import base64
import json
import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


ENVIRONMENT = os.environ.get("ENVIRONMENT")


# NOTE: use this function if pass variables to env via docker .env file. Otherwise use .pgpass
def decode_env_var(env_var_name: str) -> dict:
    if ENVIRONMENT == "LOCAL":
        env_var_dict = {env_var_name: os.environ.get(env_var_name)}
    else:
        env_var_dict = json.loads(base64.b64decode(os.environ.get(env_var_name, "")).decode("utf-8"))
    return env_var_dict


POSTGRES_DATABASE_URL = "postgresql://{username}:{password}@{host}/{database}".format(**decode_env_var("postgres"))

if "asyncpg" not in POSTGRES_DATABASE_URL:
    POSTGRES_DATABASE_URL = POSTGRES_DATABASE_URL.replace("://", "+asyncpg://")

async_engine = create_async_engine(
    POSTGRES_DATABASE_URL,
    future=True,
    echo=True,
    echo_pool=True,
    max_overflow=15,
    pool_pre_ping=True,
    pool_size=5,
    pool_recycle=180,
    pool_timeout=30,
)

async_session_maker = sessionmaker(
    async_engine,
    autoflush=False,
    autocommit=False,
    class_=AsyncSession,
    expire_on_commit=False,
    future=True,
)
