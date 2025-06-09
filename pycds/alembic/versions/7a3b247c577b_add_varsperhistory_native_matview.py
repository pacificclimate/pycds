"""Add VarsPerHistory native matview

Revision ID: 7a3b247c577b
Revises: 8fd8f556c548
Create Date: 2021-01-18 17:06:14.064487

"""

import logging
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import (
    grant_standard_table_privileges,
    create_matview,
    create_view,
    drop_matview,
    drop_view
)
from pycds import get_schema_name
from pycds.orm.native_matviews.version_22819129a609 import CollapsedVariables
from pycds.orm.native_matviews.version_7a3b247c577b import VarsPerHistory
from pycds.database import db_supports_matviews

# revision identifiers, used by Alembic.
revision = "7a3b247c577b"
down_revision = "8fd8f556c548"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic")

schema_name = get_schema_name()

def drop_dependent_objects():
    """
    Drop dependent objects that may exist in the database.
    This is necessary to ensure that the upgrade can proceed without conflicts.
    """
    op.drop_table_if_exists("collapsed_vars_mv", schema=schema_name)

def create_dependent_objects():
    """
    Create dependent objects that are required for the upgrade.
    This is necessary to ensure that the upgrade can proceed without conflicts.
    """
    create_matview(CollapsedVariables, schema=schema_name)


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    logger.debug(f"tables: {inspector.get_table_names(schema=schema_name)}")
    if db_supports_matviews(conn):
        logger.debug("This database supports native materialized views")
        op.drop_table_if_exists("vars_per_history_mv", schema=schema_name)
        logger.debug(f"tables: {inspector.get_table_names(schema=schema_name)}")
        create_matview(VarsPerHistory, schema=schema_name)

    else:
        logger.info(
            "This database does not support native materialized views: "
            "skipping upgrade"
        )


def downgrade():
    conn = op.get_bind()
    if db_supports_matviews(conn):
        logger.debug("This database supports matviews")
        drop_dependent_objects()
        drop_matview(VarsPerHistory, schema=schema_name)
        # Note: This will create the table in the database even if it didn't
        # exist before. At the time of writing, this is the desired behaviour.
        op.create_table(
            "vars_per_history_mv",
            sa.Column("history_id", sa.Integer(), nullable=False),
            sa.Column("vars_id", sa.Integer(), nullable=False),
            sa.ForeignKeyConstraint(
                ["history_id"], [f"{schema_name}.meta_history.history_id"]
            ),
            sa.ForeignKeyConstraint(["vars_id"], [f"{schema_name}.meta_vars.vars_id"]),
            sa.PrimaryKeyConstraint("history_id", "vars_id"),
            schema=schema_name,
        )
        grant_standard_table_privileges("vars_per_history_mv", schema=schema_name)
    else:
        logger.info(
            "This database does not support native materialized views: "
            "skipping downgrade"
        )
