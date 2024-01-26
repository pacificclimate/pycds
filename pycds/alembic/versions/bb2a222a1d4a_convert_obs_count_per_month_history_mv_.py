"""Convert obs_count_per_month_history_mv to native matview

Revision ID: bb2a222a1d4a
Revises: 22819129a609
Create Date: 2023-08-24 12:48:18.851655

"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import (
    drop_view_or_matview, create_view_or_matview,
    grant_standard_table_privileges,
)
from pycds import get_schema_name
from pycds.orm.native_matviews.version_bb2a222a1d4a import (
    ObsCountPerMonthHistory as ObsCountPerMonthHistoryMatview,
)
from pycds.orm.views.version_bb2a222a1d4a import (
    ObsCountPerMonthHistory as ObsCountPerMonthHistoryView,
)


# revision identifiers, used by Alembic.
revision = "bb2a222a1d4a"
down_revision = "22819129a609"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def upgrade():
    # Drop fake matview table and its associated view
    op.drop_table_if_exists(
        ObsCountPerMonthHistoryMatview.__tablename__, schema=schema_name
    )
    op.drop_replaceable_object(ObsCountPerMonthHistoryView)

    # Replace them with the native matview
    create_view_or_matview(ObsCountPerMonthHistoryMatview, schema=schema_name)


def downgrade():
    # Drop real native matview
    drop_view_or_matview(ObsCountPerMonthHistoryMatview, schema=schema_name)

    # Replace with fake matview table and associated view
    op.create_table(
        "obs_count_per_month_history_mv",
        sa.Column("count", sa.Integer(), nullable=True),
        sa.Column("date_trunc", sa.DateTime(), nullable=False),
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.PrimaryKeyConstraint("date_trunc", "history_id"),
        schema=schema_name,
    )
    grant_standard_table_privileges("obs_count_per_month_history_mv", schema=schema_name)
    create_view_or_matview(ObsCountPerMonthHistoryView, schema=schema_name)
