"""apply hx tracking to multi climo normals

Revision ID: 7244176be9fa
Revises: 758be4f4ce0f
Create Date: 2025-09-23 16:15:58.236278

"""

from alembic import op
import sqlalchemy as sa

from pycds.alembic.util import grant_standard_table_privileges
from pycds.context import get_schema_name
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


# revision identifiers, used by Alembic.
revision = "7244176be9fa"
down_revision = "758be4f4ce0f"
branch_labels = None
depends_on = None


schema_name = get_schema_name()


table_info = (
    # table_name, primary_key_name, foreign_keys, extra_indexes
    ("climo_period", "climo_period_id", None, None),
    ("climo_station", "climo_station_id", [("climo_period", "climo_period_id"), ], None),
    ("climo_stn_x_hist", ["climo_station_id", "history_id"], [("climo_station", "climo_station_id"), ("meta_history", "history_id")], None),
    ("climo_variable", "climo_variable_id", None, None),
    ("climo_value", "climo_value_id", [("climo_variable", "climo_variable_id"), ("climo_station", "climo_station_id")], None),
)

def upgrade():
    
    # We have to set the search_path so that the trigger functions fired when
    # the history table is populated can find the functions that they call.
    op.get_bind().execute(sa.text(f"SET search_path TO {schema_name}, public"))

    for table_name, primary_key_name, foreign_tables, extra_indexes in table_info:
        # Primary table
        add_history_cols_to_primary(table_name)
        create_primary_table_triggers(table_name)

        # History table
        create_history_table(table_name, foreign_tables)
        populate_history_table(table_name, primary_key_name, foreign_tables)
        # History table triggers must be created after the table is populated.
        create_history_table_triggers(table_name, foreign_tables)
        create_history_table_indexes(
            table_name, primary_key_name, foreign_tables, extra_indexes
        )
        grant_standard_table_privileges(table_name, schema=schema_name)


def downgrade():
    for table_name, _, _, _ in reversed(table_info):
        drop_history_triggers(table_name)
        drop_history_table(table_name)
        drop_history_cols_from_primary(table_name)
