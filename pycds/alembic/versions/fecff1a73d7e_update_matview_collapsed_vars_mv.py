"""Update matview collapsed_vars_mv

Revision ID: fecff1a73d7e
Revises: bb2a222a1d4a
Create Date: 2023-12-19 14:36:43.362862

"""
import logging
from alembic import op
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
down_revision = "78260d36e42b"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")
schema_name = get_schema_name()


def drop_dependent_objects():
    op.drop_replaceable_object(CrmpNetworkGeoserver, schema=schema_name)


def create_dependent_objects():
    op.create_replaceable_object(CrmpNetworkGeoserver, schema=schema_name)


def drop_matview(matview, schema=schema_name):
    # Drop any indices on the matview
    for index in matview.__table__.indexes:
        op.drop_index(
            index_name=index.name,
            table_name=index.table.name,
            schema=schema,
        )
    # Drop the matview
    op.drop_replaceable_object(matview, schema=schema)


def create_matview(matview, schema=schema_name):
    # Create the matview
    op.create_replaceable_object(matview, schema=schema)
    # Create any indices on the matview
    for index in matview.__table__.indexes:
        op.create_index(
            index_name=index.name,
            table_name=index.table.name,
            columns=[col.name for col in index.columns],
            unique=index.unique,
            schema=schema,
        )


def upgrade():
    drop_dependent_objects()
    drop_matview(OldCollapsedVariables)
    create_matview(NewCollapsedVariables)
    create_dependent_objects()


def downgrade():
    drop_dependent_objects()
    drop_matview(NewCollapsedVariables)
    create_matview(OldCollapsedVariables)
    create_dependent_objects()
