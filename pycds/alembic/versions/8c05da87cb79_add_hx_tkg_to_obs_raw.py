"""Add history tracking to obs_raw

Revision ID: 8c05da87cb79
Revises: a59d64cf16ca
Create Date: 2025-01-07 13:04:10.515777

"""
from alembic import op
import sqlalchemy as sa

from pycds import get_schema_name
from pycds.alembic.change_history_utils import (
    add_history_cols_to_primary,
    create_history_table,
    populate_history_table,
    drop_history_triggers,
    drop_history_table,
    drop_history_cols_from_primary,
    create_history_table_triggers,
    create_primary_table_triggers,
    create_history_table_indexes,
)
from pycds.alembic.util import grant_standard_table_privileges

# revision identifiers, used by Alembic.
revision = "8c05da87cb79"
down_revision = "a59d64cf16ca"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


table_name = "obs_raw"
primary_key_name = "obs_raw_id"
foreign_keys = [("meta_history", "history_id"), ("meta_vars", "vars_id")]


def upgrade():
    # Set the search_path so that when the history table is populated, the trigger
    # functions fired can find the functions that they call.
    op.get_bind().execute(f"SET search_path TO {schema_name}, public")

    # Primary table
    add_history_cols_to_primary(
        table_name,
        columns=(
            'mod_user character varying(64) COLLATE pg_catalog."default" '
            "   NOT NULL DEFAULT CURRENT_USER",
        ),
    )
    create_primary_table_triggers(table_name)

    # History table
    create_history_table(table_name, foreign_keys)
    create_history_table_indexes(table_name, primary_key_name)
    # History table triggers must be created before the table is populated.
    create_history_table_triggers(table_name, foreign_keys)
    populate_history_table(table_name, primary_key_name)
    grant_standard_table_privileges(table_name, schema=schema_name)


def downgrade():
    drop_history_triggers(table_name)
    drop_history_table(table_name)
    drop_history_cols_from_primary(table_name, columns=("mod_user",))
