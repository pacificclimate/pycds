"""Add metadata change history

Revision ID: 434b4a868241
Revises: 081f17262852
Create Date: 2024-08-16 15:47:28.844217

"""
from datetime import datetime

from alembic import op
import sqlalchemy as sa
from sqlalchemy import (
    Column,
    Integer,
    Boolean,
    DateTime,
    String,
    ForeignKey,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)

from pycds.alembic.util import create_view, drop_view
from pycds.context import get_su_role_name, get_schema_name
from pycds.orm.trigger_functions.version_434b4a868241 import update_metadata
from pycds.orm.views.version_434b4a868241 import Network as NetworkView


# revision identifiers, used by Alembic.
revision = "434b4a868241"
down_revision = "081f17262852"
branch_labels = None
depends_on = None

schema_name = get_schema_name()


metadata_descriptors = [
    # Order matters. Dependent objects (e.g., objects using other objects' foreign keys)
    # must come after dependencies.
    (
        NetworkView,
        "network_id",
        (
            Column("network_id", Integer),
            Column("network_name", String),
            Column("description", String),
            Column("virtual", String(255)),
            Column("publish", Boolean),
            Column("col_hex", String),
            Column("contact_id", Integer),
            # TODO: Change to meta_contact_hx.contact_id. For now this is for a single-table test.
            ForeignKeyConstraint(
                ["contact_id"],
                [f"meta_contact.contact_id"],
            ),
        ),
    ),
]


def upgrade():
    # Add trigger function metadata that handles view operations
    op.set_role(get_su_role_name())
    op.create_replaceable_object(update_metadata)
    op.reset_role()

    # Convert each existing metadata table to history table plus view with trigger.
    for view, metadata_id_name, metadata_specific_cols in metadata_descriptors:
        view_name = orig_metadata_table_name = view.__tablename__
        hx_id_name = f"{view_name}_hx_id"  # This can be any valid column name.
        # history_table_name must match what update_metadata computes from view name
        history_table_name = f"{view_name}_hx"

        # Create history table
        op.create_table(
            history_table_name,
            # History maintenance columns
            Column(hx_id_name, Integer, nullable=False),
            PrimaryKeyConstraint(hx_id_name),
            Column("deleted", Boolean),
            Column("create_time", DateTime, nullable=False),
            Column("creator", String, nullable=False),
            # Metadata specific columns
            *metadata_specific_cols,
            schema=schema_name,
        )

        # Migrate data from original table to history table
        op.execute(
            f"INSERT INTO {schema_name}.{history_table_name} VALUES"
            f"   SELECT {metadata_id_name}, FALSE, now(), 'UNKNOWN', *"
            f"   FROM {schema_name}.{orig_metadata_table_name}"
        )

        # Drop original table
        op.drop_table(orig_metadata_table_name, schema=schema_name)

        # Create view
        create_view(view, schema=schema_name)

        # Add trigger to view
        op.execute(
            f"CREATE TRIGGER update_{view_name}"
            f"   INSTEAD OF INSERT OR UPDATE OR DELETE ON {schema_name}.{view_name}"
            f"   FOR EACH ROW EXECUTE FUNCTION "
            f"   {schema_name}.update_metadata('{view_name}', '{metadata_id_name}')"
        )


def downgrade():
    for view, metadata_id_name, metadata_specific_cols in reversed(metadata_descriptors):
        view_name = orig_metadata_table_name = view.__tablename__
        history_table_name = f"{view_name}_hx"

        # Create original table
        op.create_table(
            orig_metadata_table_name,
            *metadata_specific_cols,
            PrimaryKeyConstraint(metadata_id_name),
        )

        # Migrate data from history table into original table.
        # Oh my this is nasty. We need all but the first 2 columns of the view to
        # repopulate the original metadata table. That's not a trivial ask in PG.
        # This appears to be the best we can do.
        op.execute(
            f"DO $$"
            f"DECLARE"
            f"  columns text;"
            f"BEGIN"
            f"  -- Get all column names, in order, except the first 2"
            f"  SELECT 'SELECT ' || string_agg(column_name, ', ') || ' FROM {view_name}' "
            f"  FROM information_schema.columns "
            f"  WHERE table_name = {view_name} "
            f"  AND table_schema = {schema_name} "
            f"  AND ordinal_position > 2 "
            f"  ORDER BY ordinal_position"
            f"  INTO columns;"
            f"  -- Select those columns into the existing metadata table"
            f"  EXECUTE "
            f"  'INSERT INTO {orig_metadata_table_name} SELECT $1 FROM {view_name}'"
            f"  USING columns;" 
            f"END $$"
        )

        # Drop view
        drop_view(view, schema=schema_name)

        # Drop history table
        op.drop_table(history_table_name)

    # Drop trigger function metadata that handles view operations
    op.drop_replaceable_object(update_metadata)
