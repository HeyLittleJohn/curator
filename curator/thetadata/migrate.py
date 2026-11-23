"""Run the theta Alembic chain.

The chain lives under ``curator/thetadata/migrations`` and records its version
in ``theta.alembic_version_theta``. It does not use the repository-root Alembic
history. Offline rendering uses a credential-free placeholder and does not connect.
"""

import io
import logging
import re
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from alembic import command
from curator.thetadata.secrets import scrub_secrets

SCRIPT_LOCATION = Path(__file__).resolve().parent / "migrations"
# Not a database. Alembic needs a postgresql URL to render postgres SQL offline.
OFFLINE_PLACEHOLDER_URL = "postgresql+psycopg://offline@127.0.0.1/markets"
_REVISION = re.compile(r"^[A-Za-z0-9_:+-]+$")
_DIRECTIONS = frozenset({"upgrade", "downgrade", "current"})


class MigrationError(RuntimeError):
    """A theta migration failed. The message has no database URL."""


def theta_script_location() -> Path:
    """Return the theta Alembic script directory.

    Returns:
        Absolute path of the theta migration environment.
    """
    return SCRIPT_LOCATION


def theta_head() -> str:
    """Return the theta chain head revision.

    Returns:
        The current head revision id.

    Raises:
        MigrationError: If the chain has no head.
    """
    script = ScriptDirectory.from_config(_config(OFFLINE_PLACEHOLDER_URL))
    head = script.get_current_head()
    if head is None:
        raise MigrationError("theta Alembic chain has no head")
    return head


def render_migration_sql(direction: str, revision: str) -> str:
    """Render migration SQL without opening a connection.

    Args:
        direction: ``upgrade`` or ``downgrade``.
        revision: Alembic target such as ``head``, ``base``, or ``-1``.

    Returns:
        The SQL text Alembic would run.

    Raises:
        MigrationError: If the direction or revision is rejected, or rendering fails.
    """
    if direction == "current":
        raise MigrationError("current does not render SQL")
    buffer = io.StringIO()
    config = _config(OFFLINE_PLACEHOLDER_URL)
    config.output_buffer = buffer
    _invoke(config, direction, revision, sql=True)
    return buffer.getvalue()


def run_migration(direction: str, revision: str, database_url: str | None) -> str:
    """Apply or inspect the theta chain.

    Args:
        direction: ``upgrade``, ``downgrade``, or ``current``.
        revision: Target revision. Ignored for ``current``.
        database_url: Postgres URL from the CLI or the environment. Not logged.

    Returns:
        Text Alembic wrote for the command, such as the current revision.
        The database URL is not included.

    Raises:
        MigrationError: If the URL is missing or Alembic fails. The message
            does not include the URL.
    """
    if direction not in _DIRECTIONS:
        raise MigrationError("direction must be upgrade, downgrade, or current")
    if not database_url or not database_url.strip():
        raise MigrationError("Pass --database-url or set THETADATA_DATABASE_URL.")
    buffer = io.StringIO()
    config = _config(to_sync_database_url(database_url.strip()))
    config.stdout = buffer
    _invoke(config, direction, revision, sql=False)
    return buffer.getvalue()


def _invoke(config: Config, direction: str, revision: str, *, sql: bool) -> None:
    """Call Alembic for one direction.

    Args:
        config: Theta Alembic config. Its URL is not logged.
        direction: ``upgrade``, ``downgrade``, or ``current``.
        revision: Target revision for upgrade and downgrade.
        sql: When true, render SQL instead of connecting.

    Returns:
        None.

    Raises:
        MigrationError: On an unsupported direction or revision, or any Alembic failure.
    """
    if direction not in _DIRECTIONS:
        raise MigrationError("direction must be upgrade, downgrade, or current")
    if direction != "current":
        _validate_revision(revision)
    if sql and direction == "downgrade" and ":" not in revision:
        revision = f"{theta_head()}:{revision}"
    _quiet_database_logs()
    try:
        if direction == "upgrade":
            command.upgrade(config, revision, sql=sql)
        elif direction == "downgrade":
            command.downgrade(config, revision, sql=sql)
        else:
            command.current(config)
    except MigrationError:
        raise
    except Exception as exc:
        raise MigrationError(scrub_secrets(f"{type(exc).__name__}: {exc}")) from None


def _config(url: str) -> Config:
    """Build an in-memory Alembic config for the theta chain.

    Args:
        url: Driver URL stored on the config and not logged.

    Returns:
        A config whose script location is the theta migration directory.
    """
    cfg = Config()
    cfg.set_main_option("script_location", str(SCRIPT_LOCATION))
    cfg.set_main_option("sqlalchemy.url", url)
    cfg.set_main_option("path_separator", "os")
    cfg.set_main_option("prepend_sys_path", str(SCRIPT_LOCATION.parents[2]))
    return cfg


def to_sync_database_url(url: str) -> str:
    """Rewrite a URL so Alembic uses the psycopg driver.

    Args:
        url: A postgresql URL, with or without a driver suffix.

    Returns:
        The URL using the ``postgresql+psycopg`` scheme.

    Raises:
        MigrationError: If the scheme is not postgresql. The URL is not included.
    """
    if "://" not in url:
        raise MigrationError("Database URL must include a scheme.")
    scheme, rest = url.split("://", 1)
    base = scheme.split("+", 1)[0]
    if base not in {"postgresql", "postgres"}:
        raise MigrationError("Database URL must use the postgresql scheme.")
    return f"postgresql+psycopg://{rest}"


def _validate_revision(revision: str) -> None:
    """Reject revision strings that are not Alembic tokens.

    Args:
        revision: Requested target.

    Returns:
        None.

    Raises:
        MigrationError: If ``revision`` contains unsupported characters.
            The value is not included.
    """
    if _REVISION.fullmatch(revision) is None:
        raise MigrationError("revision contains unsupported characters")


def _quiet_database_logs() -> None:
    """Keep SQLAlchemy from logging a connection URL.

    Returns:
        None.
    """
    for name in ("sqlalchemy", "sqlalchemy.engine", "sqlalchemy.pool"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
