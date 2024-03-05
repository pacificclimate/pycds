"""Add start/stop times to vars_per_history_mv

Revision ID: 3505750d3416
Revises: e4d17c4a1a0c
Create Date: 2024-02-27 12:30:37.975526

"""
from alembic import op
import sqlalchemy as sa

from pycds.context import get_su_role_name

#matview being upgraded
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
    
    
def upgrade():
    op.set_role(get_su_role_name())
    
    #drop dependent objects
    op.drop_replaceable_object(CrmpNetworkGeoserver)
    op.drop_replaceable_object(CollapsedVariables)
    
    #upgrade VarsPerHistory
    op.drop_replaceable_object(old_varsperhistory)
    op.create_replaceable_object(new_varsperhistory)
    
    #restore dependent objects
    op.create_replaceable_object(CollapsedVariables)
    op.create_replaceable_object(CrmpNetworkGeoserver)

    op.reset_role()


def downgrade():
    op.set_role(get_su_role_name())
    
    #drop dependent objects
    op.drop_replaceable_object(CrmpNetworkGeoserver)
    op.drop_replaceable_object(CollapsedVariables)
    
    #downgrade VarsPerHistory
    op.drop_replaceable_object(new_varsperhistory)
    op.create_replaceable_object(old_varsperhistory)
    
    #restore dependent objects
    op.create_replaceable_object(CollapsedVariables)
    op.create_replaceable_object(CrmpNetworkGeoserver)

    op.reset_role()

