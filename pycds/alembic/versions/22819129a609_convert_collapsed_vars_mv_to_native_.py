"""Convert collapsed_vars_mv to native matview

Revision ID: 22819129a609
Revises: bf366199f463
Create Date: 2023-08-22 16:15:55.404917

"""
import logging
from alembic import op
import sqlalchemy as sa
from pycds import get_schema_name
from pycds.orm.native_matviews.version_22819129a609 import (
    CollapsedVariables as CollapsedVariablesMatview,
)
from pycds.orm.views.version_22819129a609 import (
    CollapsedVariables as CollapsedVariablesView,
)
from pycds.orm.views.version_84b7fc2596d5 import CrmpNetworkGeoserver
from pycds.alembic.util import (
    grant_standard_table_privileges,
    create_view_or_matview,
    drop_view_or_matview,
)

# revision identifiers, used by Alembic.
revision = "22819129a609"
down_revision = "bf366199f463"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def drop_dependent_objects():
    drop_view_or_matview(CrmpNetworkGeoserver, schema=schema_name, drop_indexes=False)


def create_dependent_objects():
    create_view_or_matview(CrmpNetworkGeoserver, schema=schema_name, create_indexes=False)


def upgrade():
    #  We could do this with a DROP ... CASCADE, but that invites disasters.
    drop_dependent_objects()

    # Drop fake matview table and its associated view
    op.drop_table_if_exists(CollapsedVariablesMatview.__tablename__, schema=schema_name)
    op.drop_replaceable_object(CollapsedVariablesView, schema=schema_name)

    # Replace them with the native matview
    create_view_or_matview(CollapsedVariablesMatview, schema=schema_name)

    # Restore dependent objects
    create_dependent_objects()


def downgrade():
    #  We could do this with a DROP ... CASCADE, but that invites disasters.
    drop_dependent_objects()

    # Drop real native matview
    drop_view_or_matview(CollapsedVariablesMatview, schema=schema_name)

    # Replace with fake matview table
    op.create_table(
        "collapsed_vars_mv",
        sa.Column("history_id", sa.Integer(), nullable=False),
        sa.Column("vars", sa.String(), nullable=True),
        sa.Column("display_names", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["history_id"], [f"{schema_name}.meta_history.history_id"]
        ),
        sa.PrimaryKeyConstraint("history_id"),
        schema=schema_name,
    )
    grant_standard_table_privileges("collapsed_vars_mv", schema=schema_name)
    create_view_or_matview(CollapsedVariablesView, schema=schema_name)

    # Restore dependent objects
    create_dependent_objects()
