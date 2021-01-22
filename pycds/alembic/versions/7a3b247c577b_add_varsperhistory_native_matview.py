"""Add VarsPerHistory native matview

Revision ID: 7a3b247c577b
Revises: 8fd8f556c548
Create Date: 2021-01-18 17:06:14.064487

"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.materialized_views import VarsPerHistory
from pycds.alembic.helpers import db_supports_matviews

# revision identifiers, used by Alembic.
revision = "7a3b247c577b"
down_revision = "8fd8f556c548"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic")

engine = op.get_bind().engine
schema_name = get_schema_name()


def upgrade():
    if db_supports_matviews(engine):
        logger.debug("This database supports matviews")
        op.drop_table("vars_per_history_mv", schema=schema_name)
        op.create_native_materialized_view(VarsPerHistory, schema=schema_name)
        for index in VarsPerHistory.__table__.indexes:
            op.create_index(
                index_name=index.name,
                table_name=index.table.name,
                columns=[col.name for col in index.columns],
                unique=index.unique,
                schema=schema_name,
            )
    else:
        logger.info("This database does not support matviews: skipping upgrade")


def downgrade():
    if db_supports_matviews(engine):
        logger.debug("This database supports matviews")
        for index in VarsPerHistory.__table__.indexes:
            op.drop_index(
                index_name=index.name,
                table_name=index.table.name,
                schema=schema_name,
            )
        op.drop_native_materialized_view(VarsPerHistory, schema=schema_name)
        op.create_table(
            "vars_per_history_mv",
            sa.Column("history_id", sa.Integer(), nullable=False),
            sa.Column("vars_id", sa.Integer(), nullable=False),
            sa.ForeignKeyConstraint(
                ["history_id"], [f"{schema_name}.meta_history.history_id"]
            ),
            sa.ForeignKeyConstraint(
                ["vars_id"], [f"{schema_name}.meta_vars.vars_id"]
            ),
            sa.PrimaryKeyConstraint("history_id", "vars_id"),
            schema=schema_name,
        )
    else:
        logger.info(
            "This database does not support matviews: skipping downgrade"
        )
