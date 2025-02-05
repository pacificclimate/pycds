"""Add history tracking to obs_raw, part 2 (update FKs)

Revision ID: 976071a4f6e8
Revises: 8c05da87cb79
Create Date: 2025-02-04 10:09:18.094153

"""
from alembic import op
import sqlalchemy as sa

from pycds import get_schema_name
from pycds.alembic.change_history_utils import update_obs_raw_history_FKs, create_history_table_triggers, \
    create_history_table_indexes, drop_history_triggers

# revision identifiers, used by Alembic.
revision = "976071a4f6e8"
down_revision = "8c05da87cb79"
branch_labels = None
depends_on = None


schema_name = get_schema_name()

# TODO: These are the same as in the previous migration, but can't be accessed due
#   to module naming. Use __import__()?
table_name = "obs_raw"
primary_key_name = "obs_raw_id"
foreign_keys = [("meta_history", "history_id"), ("meta_vars", "vars_id")]


def upgrade():
    # Create indexes before updating, so that scans are faster.
    create_history_table_indexes(table_name, primary_key_name, foreign_keys)
    # If we let the FK trigger update FKs, fired row-by-row on ~1e9 records,
    # it requires an unfeasible amount of time, so we do it in bulk.
    update_obs_raw_history_FKs()
    # History table triggers must be created after the table is populated.
    create_history_table_triggers(table_name, foreign_keys)


def downgrade():
    drop_history_triggers(table_name)
    # We could set the hx FKs to NULL here, but there's not much point given this
    # migration is paired with the previous one, whose downgrade immediately drops
    # the entire table.
