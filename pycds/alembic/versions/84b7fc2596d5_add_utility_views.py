"""Add utility views

Revision ID: 84b7fc2596d5
Revises: 4a2f1879293a
Create Date: 2020-01-31 16:54:35.636097

"""

from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.orm.views.version_84b7fc2596d5 import (
    CrmpNetworkGeoserver,
    HistoryStationNetwork,
    ObsCountPerDayHistory,
    ObsWithFlags,
)
import pycds.alembic.extensions.operation_plugins


# revision identifiers, used by Alembic.
revision = "84b7fc2596d5"
down_revision = "4a2f1879293a"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


views = (
    CrmpNetworkGeoserver,
    HistoryStationNetwork,
    ObsCountPerDayHistory,
    ObsWithFlags,
)


def upgrade():
    for view in views:
        op.create_replaceable_object(view)


def downgrade():
    for view in reversed(views):
        op.drop_replaceable_object(view)
