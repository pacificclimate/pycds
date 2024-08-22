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
            ForeignKeyConstraint(
                ["contact_id"],
                [f"{schema_name}.meta_contact.contact_id"],
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
        # History table name must match what update_metadata computes from view name
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
            f"INSERT INTO {schema_name}.{history_table_name} "
            f"   SELECT {metadata_id_name}, FALSE, now(), 'UNKNOWN', * "
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
        temp_metadata_table_name = f"{orig_metadata_table_name}_temp"
        history_table_name = f"{view_name}_hx"

        # Create original table
        op.create_table(
            orig_metadata_table_name,
            *metadata_specific_cols,
            PrimaryKeyConstraint(metadata_id_name),
        )

        # Migrate data from history table into original table.
        # Create table duplicate of view
        op.create_table(
            temp_metadata_table_name,
            # Extra columns in view - we will drop these later
            Column("create_time", DateTime),
            Column("creator", String),
            # Original columns
            *metadata_specific_cols,
        )
        # Copy data from view to table
        op.execute(
            f"INSERT INTO {temp_metadata_table_name}"
            f"SELECT * FROM {view_name}"
        )
        # Drop first two columns from table to make it match original
        op.drop_column(temp_metadata_table_name, "create_time")
        op.drop_column(temp_metadata_table_name, "creator")
        # Later we will rename this table to the original name, but first...

        # Drop view
        drop_view(view, schema=schema_name)

        # Rename temp metadata table to original name
        op.rename_table(temp_metadata_table_name, orig_metadata_table_name)

        # Drop history table
        op.drop_table(history_table_name)

    # Drop trigger function metadata that handles view operations
    op.drop_replaceable_object(update_metadata)
