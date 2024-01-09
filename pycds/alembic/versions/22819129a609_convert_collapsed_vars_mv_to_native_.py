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


# revision identifiers, used by Alembic.
revision = "22819129a609"
down_revision = "bf366199f463"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def drop_dependent_objects():
    op.drop_replaceable_object(CrmpNetworkGeoserver)


def create_dependent_objects():
    op.create_replaceable_object(CrmpNetworkGeoserver)


def upgrade():
    #  We could do this with a DROP ... CASCADE, but that invites disasters.
    drop_dependent_objects()

    # Drop fake matview table and its associated view
    op.drop_table_if_exists(CollapsedVariablesMatview.__tablename__, schema=schema_name)
    op.drop_replaceable_object(CollapsedVariablesView)

    # Replace them with the native matview
    op.create_replaceable_object(CollapsedVariablesMatview, schema=schema_name)
    for index in CollapsedVariablesMatview.__table__.indexes:
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
    #  We could do this with a DROP ... CASCADE, but that invites disasters.
    drop_dependent_objects()

    # Drop real native matview
    for index in CollapsedVariablesMatview.__table__.indexes:
        op.drop_index(
            index_name=index.name,
            table_name=index.table.name,
            schema=schema_name,
        )
    op.drop_replaceable_object(CollapsedVariablesMatview)

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
    op.create_replaceable_object(CollapsedVariablesView)

    # Restore dependent objects
    create_dependent_objects()
