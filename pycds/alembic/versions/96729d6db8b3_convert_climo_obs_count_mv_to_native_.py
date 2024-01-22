"""Convert climo_obs_count_mv to native matview

Revision ID: 96729d6db8b3
Revises: 5c841d2c01d1
Create Date: 2023-07-14 15:35:03.045034

"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.orm.native_matviews.version_96729d6db8b3 import (
    ClimoObsCount as ClimoObsCountMatview,
)
from pycds.orm.views.version_96729d6db8b3 import ClimoObsCount as ClimoObsCountView

# revision identifiers, used by Alembic.
revision = "96729d6db8b3"
down_revision = "5c841d2c01d1"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")

schema_name = get_schema_name()


def upgrade():
    op.drop_replaceable_object(ClimoObsCountView)
    op.drop_table_if_exists(ClimoObsCountMatview.__tablename__, schema=schema_name)
    op.create_replaceable_object(ClimoObsCountMatview, schema=schema_name)
    for index in ClimoObsCountMatview.__table__.indexes:
        op.create_index(
            index_name=index.name,
            table_name=index.table.name,
            columns=[col.name for col in index.columns],
            unique=index.unique,
            schema=schema_name,
        )


def downgrade():
    for index in ClimoObsCountMatview.__table__.indexes:
        op.drop_index(
            index_name=index.name,
            table_name=index.table.name,
            schema=schema_name,
        )
    op.drop_replaceable_object(ClimoObsCountMatview)
    op.create_table(
        "climo_obs_count_mv",
        sa.Column("count", sa.BigInteger(), nullable=True),
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.PrimaryKeyConstraint("history_id"),
        schema=schema_name,
    )
    op.create_replaceable_object(ClimoObsCountView)
