"""Add history tracking to 4 main metadata tables

Revision ID: a59d64cf16ca
Revises: 7ab87f8fbcf4
Create Date: 2024-12-16 14:54:41.332001

"""
from alembic import op

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
)

# revision identifiers, used by Alembic.
revision = "a59d64cf16ca"
down_revision = "7ab87f8fbcf4"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


table_info = (
    ("meta_network", "network_id", None),
    ("meta_station", "station_id", [("meta_network", "network_id")]),
    ("meta_history", "history_id", [("meta_station", "station_id")]),
    ("meta_vars", "vars_id", [("meta_network", "network_id")]),
)


def upgrade():
    # We have to set the search_path so that the trigger functions fired when
    # the history table is populated can find the functions that they call.
    op.get_bind().execute(f"SET search_path TO {schema_name}, public")
    for table_name, primary_key_name, foreign_keys in table_info:
        add_history_cols_to_primary(table_name)
        create_primary_table_triggers(table_name)

        create_history_table(table_name, foreign_keys)
        # History table triggers must be created before the table is populated.
        create_history_table_triggers(table_name, foreign_keys)
        populate_history_table(table_name, primary_key_name)


def downgrade():
    for table_name, _, _ in reversed(table_info):
        drop_history_triggers(table_name)
        drop_history_table(table_name)
        drop_history_cols_from_primary(table_name)
