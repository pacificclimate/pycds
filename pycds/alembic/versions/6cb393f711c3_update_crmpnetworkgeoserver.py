"""Add columns vars_ids and unique_variable_tags to crmp_network_geoserver

Revision ID: 6cb393f711c3
Revises: fecff1a73d7e
Create Date: 2024-01-05 13:06:02.811787

"""
import logging
from alembic import op
from pycds import get_schema_name
from pycds.orm.views.version_84b7fc2596d5 import (
    CrmpNetworkGeoserver as OldCrmpNetworkGeoserver,
)
from pycds.orm.views.version_6cb393f711c3 import (
    CrmpNetworkGeoserver as NewCrmpNetworkGeoserver,
)


# revision identifiers, used by Alembic.
revision = "6cb393f711c3"
down_revision = "fecff1a73d7e"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def drop_view(view):
    op.drop_replaceable_object(view, schema=schema_name)


def create_view(view):
    op.create_replaceable_object(view, schema=schema_name)


def upgrade():
    drop_view(OldCrmpNetworkGeoserver)
    create_view(NewCrmpNetworkGeoserver)


def downgrade():
    drop_view(NewCrmpNetworkGeoserver)
    create_view(OldCrmpNetworkGeoserver)
