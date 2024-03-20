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
    create_view,
    drop_view,
    create_matview,
    drop_matview,
)
from pycds.database import matview_exists

# revision identifiers, used by Alembic.
revision = "22819129a609"
down_revision = "bf366199f463"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def drop_dependent_objects():
    drop_view(CrmpNetworkGeoserver, schema=schema_name)


def create_dependent_objects():
    create_view(CrmpNetworkGeoserver, schema=schema_name)


def upgrade():
    engine = op.get_bind().engine
    if matview_exists(
        engine, CollapsedVariablesMatview.__tablename__, schema=schema_name
    ):
        logger.info(
            f"A native materialized view '{CollapsedVariablesMatview.__tablename__}' "
            f"already exists in the database; skipping upgrade"
        )
    else:
        #  We could do this with a DROP ... CASCADE, but that invites disasters.
        drop_dependent_objects()

        # Drop fake matview table and its associated view
        op.drop_table_if_exists(
            CollapsedVariablesMatview.__tablename__, schema=schema_name
        )
        drop_view(CollapsedVariablesView, schema=schema_name)

        # Replace them with the native matview
        create_matview(CollapsedVariablesMatview, schema=schema_name)

        # Restore dependent objects
        create_dependent_objects()


def downgrade():
    #  We could do this with a DROP ... CASCADE, but that invites disasters.
    drop_dependent_objects()

    # Drop real native matview
    drop_matview(CollapsedVariablesMatview, schema=schema_name)

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
    create_view(CollapsedVariablesView, schema=schema_name)

    # Restore dependent objects
    create_dependent_objects()
