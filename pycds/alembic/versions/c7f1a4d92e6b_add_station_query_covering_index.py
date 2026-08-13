"""Add a covering index for station observation queries.

``query_one_station`` filters observations by history, pivots variables, and
orders the result by observation time. The previous indexes either led with
``obs_time`` or ordered the remaining columns as ``history_id, vars_id,
obs_time``. PostgreSQL therefore had to read and sort a large intermediate row
set for long stations.

Leading with ``history_id, obs_time`` lets the common single-history query use
an equality condition while reading rows in time order. Including ``vars_id``
supports variable filtering, and including ``datum`` allows an index-only scan
when PostgreSQL's visibility map permits it. Multi-history stations still use
the same index for each history before combining their rows safely.

The index was tested manually on production-like data before this migration
was added, so some databases may already contain it. ``IF NOT EXISTS`` makes
the migration safe for those databases while retaining normal creation for
databases that do not yet have the index.

Revision ID: c7f1a4d92e6b
Revises: f6d5a4c2e901
Create Date: 2026-08-12
"""

from alembic import op

from pycds import get_schema_name


revision = "c7f1a4d92e6b"
down_revision = "f6d5a4c2e901"
branch_labels = None
depends_on = None


INDEX_NAME = "obs_raw_history_time_variable_covering_idx"


def upgrade():
    op.create_index(
        INDEX_NAME,
        "obs_raw",
        ["history_id", "obs_time", "vars_id"],
        unique=False,
        if_not_exists=True,
        schema=get_schema_name(),
        postgresql_include=["datum"],
    )


def downgrade():
    op.drop_index(
        INDEX_NAME,
        table_name="obs_raw",
        schema=get_schema_name(),
    )
