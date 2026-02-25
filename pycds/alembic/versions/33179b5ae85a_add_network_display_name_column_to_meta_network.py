"""add network_display_name column to meta_network

Revision ID: 33179b5ae85a
Revises: 8c05da87cb79
Create Date: 2026-01-07 20:25:34.314026

Notes: This process was made more complicated by some assumptions made by the history tracking code.
In particular it assumes that the primary and history table have the same column order with the
exception that history tables have additional columns at the end. When adding a new column it is added
at the end and therefore breaks the assumption. To work around this, we have to recreate the history table
with the correct column order. This involves renaming the existing history table, creating a new one with
the correct structure, copying the data over, and then dropping the old table.

This is needed because at the current time neither postgres nor alembic support adding a column at a specific
position in the table.

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from pycds.context import get_schema_name
from pycds.alembic.change_history_utils import (
    disable_primary_table_triggers,
    create_history_table,
    create_history_table_triggers,
    create_history_table_indexes,
    enable_primary_table_triggers,
)
from pycds.alembic.util import grant_standard_table_privileges


# revision identifiers, used by Alembic.
revision = "33179b5ae85a"
down_revision = "8c05da87cb79"
branch_labels = None
depends_on = None

schema_name = get_schema_name()


def upgrade():
    # Disable existing triggers before modifying table structure so that we don't accidentally track
    # the intermediate states
    disable_primary_table_triggers("meta_network")

    # Rename the existing history table to preserve existing history data
    # We'll copy data from this into the new table with the correct column order
    op.execute(
        text(f"ALTER TABLE {schema_name}.meta_network_hx RENAME TO meta_network_hx_old")
    )

    # Drop the identity property, which also drops the associated sequence
    op.execute(
        text(
            f"ALTER TABLE {schema_name}.meta_network_hx_old ALTER COLUMN meta_network_hx_id DROP IDENTITY"
        )
    )

    op.add_column(
        "meta_network",
        sa.Column(
            "network_display_name",
            sa.String(),
            nullable=True,
        ),
        schema=schema_name,
    )

    # Copy existing network names into the new display name column.
    op.execute(
        text(
            f"""
            UPDATE {schema_name}.meta_network
            SET network_display_name = network_name
            """
        )
    )

    op.create_unique_constraint(
        "uq_meta_network_network_display_name",
        "meta_network",
        ["network_display_name"],
        schema=schema_name,
    )

    # Create a trigger function to auto-populate network_display_name on INSERT. Must be a trigger as
    # Default values can't access other columns.
    op.execute(
        text(
            f"""
            CREATE OR REPLACE FUNCTION {schema_name}.set_network_display_name_default()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF NEW.network_display_name IS NULL THEN
                    NEW.network_display_name := NEW.network_name;
                END IF;
                RETURN NEW;
            END;
            $$
            """
        )
    )

    # Create trigger to run before INSERT
    op.execute(
        text(
            f"""
            CREATE TRIGGER set_network_display_name_default_trigger
            BEFORE INSERT ON {schema_name}.meta_network
            FOR EACH ROW
            EXECUTE FUNCTION {schema_name}.set_network_display_name_default()
            """
        )
    )

    # Create a trigger function to prevent modification of network_name column
    op.execute(
        text(
            f"""
            CREATE OR REPLACE FUNCTION {schema_name}.prevent_network_name_modification()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF TG_OP = 'UPDATE' AND OLD.network_name IS DISTINCT FROM NEW.network_name THEN
                    RAISE EXCEPTION 'Modification of network_name column is not allowed';
                END IF;
                RETURN NEW;
            END;
            $$
            """
        )
    )

    # Create trigger to prevent network_name modification on UPDATE
    op.execute(
        text(
            f"""
            CREATE TRIGGER prevent_network_name_modification_trigger
            BEFORE UPDATE ON {schema_name}.meta_network
            FOR EACH ROW
            EXECUTE FUNCTION {schema_name}.prevent_network_name_modification()
            """
        )
    )

    # Recreate the history table with the new column structure
    create_history_table("meta_network", foreign_tables=None)
    grant_standard_table_privileges(f"{schema_name}.meta_network_hx")

    # Copy existing history data from the old table to the new one
    op.execute(
        text(
            f"""
            INSERT INTO {schema_name}.meta_network_hx 
                (network_id, network_name, description, virtual, 
                 publish, col_hex, mod_time, mod_user, 
                 network_display_name, deleted, meta_network_hx_id)
            SELECT 
                network_id, network_name, description, virtual,
                publish, col_hex, mod_time, mod_user,
                network_name,
                deleted, meta_network_hx_id
            FROM {schema_name}.meta_network_hx_old
            ORDER BY meta_network_hx_id
            """
        )
    )

    # Update foreign key references in dependent tables to point to the new history table
    # meta_station_hx and meta_vars_hx have foreign keys to meta_network_hx

    # Drop the foreign key constraints from dependent tables
    op.execute(
        text(
            f"ALTER TABLE {schema_name}.meta_station_hx DROP CONSTRAINT meta_station_hx_meta_network_hx_id_fkey"
        )
    )
    op.execute(
        text(
            f"ALTER TABLE {schema_name}.meta_vars_hx DROP CONSTRAINT meta_vars_hx_meta_network_hx_id_fkey"
        )
    )

    # Drop the old history table now that data has been copied and FKs removed
    op.execute(text(f"DROP TABLE {schema_name}.meta_network_hx_old"))

    # Set the sequence to continue from the next ID after the last copied record
    op.execute(
        text(
            f"""
            SELECT setval(
                '{schema_name}.meta_network_hx_meta_network_hx_id_seq',
                (SELECT COALESCE(MAX(meta_network_hx_id), 1) FROM {schema_name}.meta_network_hx),
                true
            )
            """
        )
    )

    # Recreate the foreign key constraints pointing to the new history table
    op.execute(
        text(
            f"""
            ALTER TABLE {schema_name}.meta_station_hx 
            ADD CONSTRAINT meta_station_hx_meta_network_hx_id_fkey 
            FOREIGN KEY (meta_network_hx_id) 
            REFERENCES {schema_name}.meta_network_hx(meta_network_hx_id)
            """
        )
    )
    op.execute(
        text(
            f"""
            ALTER TABLE {schema_name}.meta_vars_hx 
            ADD CONSTRAINT meta_vars_hx_meta_network_hx_id_fkey 
            FOREIGN KEY (meta_network_hx_id) 
            REFERENCES {schema_name}.meta_network_hx(meta_network_hx_id)
            """
        )
    )

    # Recreate the history tracking triggers
    enable_primary_table_triggers("meta_network")
    create_history_table_triggers("meta_network", foreign_tables=None)

    # Create indexes on the history table
    create_history_table_indexes(
        "meta_network", "network_id", foreign_tables=None, extras=None
    )


def downgrade():
    # Drop the triggers and trigger functions
    op.execute(
        text(
            f"""
            DROP TRIGGER IF EXISTS prevent_network_name_modification_trigger ON {schema_name}.meta_network;
            DROP TRIGGER IF EXISTS set_network_display_name_default_trigger ON {schema_name}.meta_network;
            DROP FUNCTION IF EXISTS {schema_name}.set_network_display_name_default();
            DROP FUNCTION IF EXISTS {schema_name}.prevent_network_name_modification();
            """
        )
    )

    # Drop the constraint and column from primary table
    op.drop_constraint(
        "uq_meta_network_network_display_name",
        "meta_network",
        type_="unique",
        schema=schema_name,
    )

    # When dropping we don't have the same issues with column order so we can safely just drop the
    # column to return to the pre-migration state
    op.drop_column("meta_network", "network_display_name", schema=schema_name)

    # Drop the column from history table
    op.drop_column("meta_network_hx", "network_display_name", schema=schema_name)
