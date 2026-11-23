"""Alembic environment for schema ``theta``.

This history is not the repository-root chain. Versions are stored in
``theta.alembic_version_theta``. v3 timestamps are zone-less America/New_York
wall times; see ``curator.thetadata.time.parse_thetadata_timestamp``.
"""

import logging
import sys
from pathlib import Path

from sqlalchemy import pool
from sqlalchemy.engine import Connection, create_engine

from alembic import context

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from curator.thetadata.config import ALEMBIC_VERSION_TABLE, THETA_SCHEMA  # noqa: E402
from curator.thetadata.models.theta import ThetaBase  # noqa: E402
from curator.thetadata.partitions import quote_ident  # noqa: E402

config = context.config
target_metadata = ThetaBase.metadata

for _logger_name in ("sqlalchemy", "sqlalchemy.engine", "sqlalchemy.pool"):
    logging.getLogger(_logger_name).setLevel(logging.CRITICAL)

_SCHEMA_DDL = f"CREATE SCHEMA IF NOT EXISTS {quote_ident(THETA_SCHEMA)}"


def _configure(connection: Connection | None, url: str | None) -> None:
    """Point Alembic at the theta version table.

    Args:
        connection: Live connection for online mode.
        url: Offline URL. Unused when ``connection`` is set.

    Returns:
        None.
    """
    kwargs: dict[str, object] = {
        "target_metadata": target_metadata,
        "version_table": ALEMBIC_VERSION_TABLE,
        "version_table_schema": THETA_SCHEMA,
        "include_schemas": True,
        "compare_type": True,
    }
    if connection is not None:
        kwargs["connection"] = connection
    else:
        kwargs["url"] = url
        kwargs["literal_binds"] = True
        kwargs["dialect_opts"] = {"paramstyle": "named"}
    context.configure(**kwargs)


def run_migrations_offline() -> None:
    """Emit SQL without connecting.

    Returns:
        None.
    """
    url = config.get_main_option("sqlalchemy.url")
    _configure(None, url)
    with context.begin_transaction():
        context.execute(_SCHEMA_DDL)
        context.run_migrations()


def run_migrations_online() -> None:
    """Apply migrations with a synchronous connection.

    Returns:
        None.

    Raises:
        RuntimeError: If the config has no database URL. The message has no URL.
    """
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("Database URL is not set.")
    connectable = create_engine(url, poolclass=pool.NullPool, echo=False, hide_parameters=True)
    with connectable.connect() as connection:
        # Configure before any statement so Alembic owns the transaction.
        # A statement issued first would autobegin, and SQLAlchemy 2 rolls
        # that transaction back when the connection closes.
        _configure(connection, None)
        with context.begin_transaction():
            context.execute(_SCHEMA_DDL)
            context.run_migrations()
        if connection.in_transaction():
            connection.commit()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
