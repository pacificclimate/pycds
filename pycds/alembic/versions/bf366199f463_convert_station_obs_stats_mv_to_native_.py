"""Convert station_obs_stats_mv to native matview

Revision ID: bf366199f463
Revises: 96729d6db8b3
Create Date: 2023-08-18 15:55:38.242505

"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.orm.native_matviews.version_bf366199f463 import (
    StationObservationStats as StationObservationStatsMatview,
)
from pycds.orm.views.version_bf366199f463 import (
    StationObservationStats as StationObservationStatsView,
)
# Important: We must obtain replaceable database objects from the appropriate revision
# of the database. Otherwise, later migrations will cause errors in earlier migrations
# due to a mismatch between the expected version and the latest (head) version.
from pycds.orm.views.version_84b7fc2596d5 import CrmpNetworkGeoserver

# revision identifiers, used by Alembic.
revision = "bf366199f463"
down_revision = "96729d6db8b3"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


# TODO: Use CrmpNetworkGeoserver taken from appropriate revision
def drop_dependent_objects():
    op.drop_replaceable_object(CrmpNetworkGeoserver)


def create_dependent_objects():
    op.create_replaceable_object(CrmpNetworkGeoserver)


def upgrade():
    #  We could do this with a DROP ... CASCADE, but I like to be sure exactly what
    #  I am dropping.
    drop_dependent_objects()

    # Drop fake matview table and its associated view, and replace them with the
    # native matview
    op.drop_replaceable_object(StationObservationStatsView)
    op.drop_table_if_exists(
        StationObservationStatsMatview.__tablename__, schema=schema_name
    )
    op.create_replaceable_object(StationObservationStatsMatview, schema=schema_name)
    for index in StationObservationStatsMatview.__table__.indexes:
        op.create_index(
            index_name=index.name,
            table_name=index.table.name,
            columns=[col.name for col in index.columns],
            unique=index.unique,
            schema=schema_name,
        )

    # Restore dependent objects
    create_dependent_objects()


def downgrade():
    #  We could do this with a DROP ... CASCADE, but I like to be sure exactly what
    #  I am dropping.
    drop_dependent_objects()

    # Drop real native matview and replace with fake matview table
    for index in StationObservationStatsMatview.__table__.indexes:
        op.drop_index(
            index_name=index.name,
            table_name=index.table.name,
            schema=schema_name,
        )
    op.drop_replaceable_object(StationObservationStatsMatview)
    op.create_table(
        "station_obs_stats_mv",
        sa.Column("station_id", sa.Integer(), nullable=False),
        sa.Column("history_id", sa.Integer(), nullable=True),
        sa.Column("min_obs_time", sa.DateTime(), nullable=True),
        sa.Column("max_obs_time", sa.DateTime(), nullable=True),
        sa.Column("obs_count", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.ForeignKeyConstraint(
            ["station_id"], [f"{schema_name}.meta_station.station_id"]
        ),
        schema=schema_name,
    )
    op.create_replaceable_object(StationObservationStatsView)

    # Restore dependent objects
    create_dependent_objects()
