"""Add start/stop times to vars_per_history_mv

Revision ID: 3505750d3416
Revises: e4d17c4a1a0c
Create Date: 2024-02-27 12:30:37.975526

"""

from alembic import op
import sqlalchemy as sa

from pycds.alembic.util import create_view, create_matview, drop_matview, drop_view
from pycds.context import get_su_role_name, get_schema_name

# matview being upgraded
from pycds.orm.native_matviews.version_7a3b247c577b import (
    VarsPerHistory as old_varsperhistory,
)
from pycds.orm.native_matviews.version_3505750d3416 import (
    VarsPerHistory as new_varsperhistory,
)

# view and matview that depend on the one being upgraded or downgraded,
# and need to be replaced as well
from pycds.orm.views.version_6cb393f711c3 import CrmpNetworkGeoserver
from pycds.orm.native_matviews.version_fecff1a73d7e import CollapsedVariables

# revision identifiers, used by Alembic.
revision = "3505750d3416"
down_revision = "efde19ea4f52"
branch_labels = None
depends_on = None

schema_name = get_schema_name()


# drop other objects that depend on the view being upgraded
def drop_dependent_objects():
    drop_view(CrmpNetworkGeoserver, schema=schema_name)
    drop_matview(CollapsedVariables, schema=schema_name)


# recreate other objects that depend on the view being upgraded
def create_dependent_objects():
    create_matview(CollapsedVariables, schema=schema_name)
    create_view(CrmpNetworkGeoserver, schema=schema_name)


def upgrade():
    op.set_role(get_su_role_name())

    drop_dependent_objects()
    drop_matview(old_varsperhistory, schema=schema_name)
    create_matview(new_varsperhistory, schema=schema_name)

    create_dependent_objects()

    op.reset_role()


def downgrade():
    op.set_role(get_su_role_name())

    drop_dependent_objects()

    drop_matview(new_varsperhistory, schema=schema_name)
    create_matview(old_varsperhistory, schema=schema_name)

    create_dependent_objects()

    op.reset_role()
