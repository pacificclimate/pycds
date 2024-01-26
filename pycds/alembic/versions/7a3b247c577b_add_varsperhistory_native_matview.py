"""Add VarsPerHistory native matview

Revision ID: 7a3b247c577b
Revises: 8fd8f556c548
Create Date: 2021-01-18 17:06:14.064487

"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import (
    create_view_or_matview, drop_view_or_matview,
    grant_standard_table_privileges,
)
from pycds import get_schema_name
from pycds.orm.native_matviews.version_7a3b247c577b import VarsPerHistory
from pycds.database import db_supports_matviews

# revision identifiers, used by Alembic.
revision = "7a3b247c577b"
down_revision = "8fd8f556c548"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic")

schema_name = get_schema_name()


def upgrade():
    engine = op.get_bind().engine
    inspector = sa.inspect(engine)
    logger.debug(f"tables: {inspector.get_table_names(schema=schema_name)}")
    if db_supports_matviews(engine):
        logger.debug("This database supports native materialized views")
        op.drop_table_if_exists("vars_per_history_mv", schema=schema_name)
        create_view_or_matview(VarsPerHistory, schema=schema_name)
    else:
        logger.info(
            "This database does not support native materialized views: "
            "skipping upgrade"
        )


def downgrade():
    engine = op.get_bind().engine
    if db_supports_matviews(engine):
        logger.debug("This database supports matviews")
        drop_view_or_matview(VarsPerHistory, schema=schema_name)
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
