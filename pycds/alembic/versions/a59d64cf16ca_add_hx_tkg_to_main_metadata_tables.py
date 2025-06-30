"""Add history tracking to 4 main metadata tables

Revision ID: a59d64cf16ca
Revises: 7ab87f8fbcf4
Create Date: 2024-12-16 14:54:41.332001

"""

from alembic import op
from sqlalchemy import text

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
    hx_table_name,
)
from pycds.alembic.util import grant_standard_table_privileges

# revision identifiers, used by Alembic.
revision = "a59d64cf16ca"
down_revision = "7ab87f8fbcf4"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


table_info = (
    # table_name, primary_key_name, foreign_keys, extra_indexes
    ("meta_network", "network_id", None, None),
    ("meta_station", "station_id", [("meta_network", "network_id")], None),
    ("meta_history", "history_id", [("meta_station", "station_id")], None),
    ("meta_vars", "vars_id", [("meta_network", "network_id")], None),
)


def upgrade():
    # We have to set the search_path so that the trigger functions fired when
    # the history table is populated can find the functions that they call.
    op.get_bind().execute(text(f"SET search_path TO {schema_name}, public"))

    for table_name, primary_key_name, foreign_tables, extra_indexes in table_info:
        # Primary table
        add_history_cols_to_primary(table_name)
        create_primary_table_triggers(table_name)

        # History table
        create_history_table(table_name, foreign_tables)
        grant_standard_table_privileges(hx_table_name(table_name, schema=schema_name))
        populate_history_table(table_name, primary_key_name, foreign_tables)
        # History table triggers must be created after the table is populated.
        create_history_table_triggers(table_name, foreign_tables)
        create_history_table_indexes(
            table_name, primary_key_name, foreign_tables, extra_indexes
        )


def downgrade():
    for table_name, _, _, _ in reversed(table_info):
        drop_history_triggers(table_name)
        drop_history_table(table_name)
        drop_history_cols_from_primary(table_name)
