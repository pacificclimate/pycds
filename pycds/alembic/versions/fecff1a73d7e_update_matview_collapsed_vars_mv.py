"""Add columns vars_ids and unique_variable_tags to matview collapsed_vars_mv

Revision ID: fecff1a73d7e
Revises: bb2a222a1d4a
Create Date: 2023-12-19 14:36:43.362862

"""
import logging
from alembic import op
from pycds.alembic.util import (
    grant_standard_table_privileges,
    drop_view_or_matview,
    create_view_or_matview,
)
from pycds.orm.native_matviews.version_22819129a609 import (
    CollapsedVariables as OldCollapsedVariables,
)
from pycds.orm.native_matviews.version_fecff1a73d7e import (
    CollapsedVariables as NewCollapsedVariables,
)
from pycds import (
    get_schema_name,
    CrmpNetworkGeoserver,
)

# revision identifiers, used by Alembic.
revision = "fecff1a73d7e"
down_revision = "bb2a222a1d4a"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def drop_dependent_objects():
    drop_view_or_matview(CrmpNetworkGeoserver, schema=schema_name)


def create_dependent_objects():
    create_view_or_matview(CrmpNetworkGeoserver, schema=schema_name)


def upgrade():
    drop_dependent_objects()
    drop_view_or_matview(OldCollapsedVariables)
    create_view_or_matview(NewCollapsedVariables)
    create_dependent_objects()


def downgrade():
    drop_dependent_objects()
    drop_view_or_matview(NewCollapsedVariables)
    create_view_or_matview(OldCollapsedVariables)
    create_dependent_objects()
