"""Convert station_obs_stats_mv to native matview

Revision ID: bf366199f463
Revises: 96729d6db8b3
Create Date: 2023-08-18 15:55:38.242505

"""

import logging
from alembic import op
import sqlalchemy as sa
from pycds.alembic.util import create_view, create_matview, drop_matview, drop_view
from pycds import get_schema_name
from pycds.database import matview_exists
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


def drop_dependent_objects():
    drop_view(CrmpNetworkGeoserver, schema=schema_name)


def create_dependent_objects():
    create_view(CrmpNetworkGeoserver, schema=schema_name)


def upgrade():
    conn = op.get_bind()
    if matview_exists(
        conn, StationObservationStatsMatview.__tablename__, schema=schema_name
    ):
        logger.info(
            f"A native materialized view '{StationObservationStatsMatview.__tablename__}' "
            f"already exists in the database; skipping upgrade"
        )
    else:
        #  We could do this with a DROP ... CASCADE, but I like to be sure exactly what
        #  I am dropping.
        drop_dependent_objects()

        # Drop fake matview table and its associated view, and replace them with the
        # native matview
        drop_view(StationObservationStatsView, schema=schema_name)
        op.drop_table_if_exists(
            StationObservationStatsMatview.__tablename__, schema=schema_name
        )
        create_matview(StationObservationStatsMatview, schema=schema_name)

        # Restore dependent objects
        create_dependent_objects()


def downgrade():
    #  We could do this with a DROP ... CASCADE, but I like to be sure exactly what
    #  I am dropping.
    drop_dependent_objects()

    # Drop real native matview and replace with fake matview table
    drop_matview(StationObservationStatsMatview, schema=schema_name)
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
    create_view(StationObservationStatsView, schema=schema_name)

    # Restore dependent objects
    create_dependent_objects()
