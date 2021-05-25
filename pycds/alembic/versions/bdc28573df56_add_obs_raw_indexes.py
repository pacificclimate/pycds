"""Add obs_raw indexes

Revision ID: bdc28573df56
Revises: 7a3b247c577b
Create Date: 2021-05-06 10:41:36.554301

This migration conditionally creates/deletes the indexes. It is robust to
their pre-existence on upgrade, and their non-existence on downgrade. The
former is expected in some databases. It's hard to imagine why the latter
would be true, but it's easy enough to provide for.
"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.database import get_schema_item_names


# revision identifiers, used by Alembic.
revision = "bdc28573df56"
down_revision = "7a3b247c577b"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


indexes = (
    ("mod_time_idx", "obs_raw", ["mod_time"]),
    ("obs_raw_comp_idx", "obs_raw", ["obs_time", "vars_id", "history_id"]),
    ("obs_raw_history_id_idx", "obs_raw", ["history_id"]),
    ("obs_raw_id_idx", "obs_raw", ["obs_raw_id"]),
)


def upgrade():
    engine = op.get_bind().engine
    for index_name, table_name, columns in indexes:
        existing_index_names = get_schema_item_names(
            engine, "indexes", table_name=table_name, schema_name=schema_name
        )
        if index_name not in existing_index_names:
            logger.debug(f"Creating index {index_name}")
            op.create_index(
                index_name,
                table_name,
                columns,
                unique=False,
                schema=schema_name,
            )
        else:
            logger.debug(f"Index {index_name} already exists; skipping create")


def downgrade():
    engine = op.get_bind().engine
    for index_name, table_name, _ in indexes:
        existing_index_names = get_schema_item_names(
            engine, "indexes", table_name=table_name, schema_name=schema_name
        )
        if index_name in existing_index_names:
            logger.debug(f"Dropping index {index_name}")
            op.drop_index(index_name, table_name=table_name, schema=schema_name)
        else:
            logger.info(f"Index {index_name} does not exist; skipping drop")
