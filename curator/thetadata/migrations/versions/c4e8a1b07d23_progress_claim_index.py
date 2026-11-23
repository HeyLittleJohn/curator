"""Progress interval and claim indexes.

``theta.progress`` already stores dataset, date, expiration, status, row_count,
and updated_at. This revision adds the per-job intraday interval and the
indexes the planner uses to claim pending rows and to find stale running rows.
It does not change the main Alembic chain.

Revision ID: c4e8a1b07d23
Revises: b7e4d1a09c52
Create Date: 2026-11-23

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from curator.thetadata.config import THETA_SCHEMA
from curator.thetadata.models.theta import PROGRESS_INTERVAL_COMMENT, PROGRESS_INTERVAL_SQL, PROGRESS_STATUS_SQL
from curator.thetadata.partitions import quote_ident

revision: str = "c4e8a1b07d23"
down_revision: str | None = "b7e4d1a09c52"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCHEMA = quote_ident(THETA_SCHEMA)


def upgrade() -> None:
    """Add ``interval_s``, status checks, and the claim indexes.

    Returns:
        None.
    """
    op.add_column(
        "progress",
        sa.Column("interval_s", sa.SmallInteger(), nullable=True, comment=PROGRESS_INTERVAL_COMMENT),
        schema=_SCHEMA,
    )
    op.create_check_constraint("ck_progress_status", "progress", PROGRESS_STATUS_SQL, schema=_SCHEMA)
    op.create_check_constraint("ck_progress_interval_s", "progress", PROGRESS_INTERVAL_SQL, schema=_SCHEMA)
    op.execute(
        f"CREATE INDEX ix_progress_claim ON {_SCHEMA}.progress (date, expiration, dataset) WHERE status = 'pending'"
    )
    op.execute(f"CREATE INDEX ix_progress_running ON {_SCHEMA}.progress (updated_at) WHERE status = 'running'")


def downgrade() -> None:
    """Remove the claim indexes, checks, and ``interval_s``.

    Returns:
        None.
    """
    op.execute(f"DROP INDEX IF EXISTS {_SCHEMA}.ix_progress_running")
    op.execute(f"DROP INDEX IF EXISTS {_SCHEMA}.ix_progress_claim")
    op.drop_constraint("ck_progress_interval_s", "progress", schema=_SCHEMA, type_="check")
    op.drop_constraint("ck_progress_status", "progress", schema=_SCHEMA, type_="check")
    op.drop_column("progress", "interval_s", schema=_SCHEMA)
