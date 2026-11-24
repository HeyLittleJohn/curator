"""Sanitized failure text on theta.progress.

A failed job records the scrubbed error. Done, empty, and pending rows leave
the column null. This revision does not change the main Alembic chain.

Revision ID: d7a1e4c09b56
Revises: c4e8a1b07d23
Create Date: 2026-11-24

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from curator.thetadata.config import THETA_SCHEMA
from curator.thetadata.partitions import quote_ident

revision: str = "d7a1e4c09b56"
down_revision: str | None = "c4e8a1b07d23"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCHEMA = quote_ident(THETA_SCHEMA)


def upgrade() -> None:
    """Add the nullable ``error`` column.

    Returns:
        None.
    """
    op.add_column("progress", sa.Column("error", sa.Text(), nullable=True), schema=_SCHEMA)


def downgrade() -> None:
    """Remove the ``error`` column.

    Returns:
        None.
    """
    op.drop_column("progress", "error", schema=_SCHEMA)
