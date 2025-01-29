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
    hx_table_name,
    update_obs_raw_history_FKs,
    pri_table_name,
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
    # Note: This is no longer relied upon, but it may be again in future, so leave
    # this here.
    op.get_bind().execute(f"SET search_path TO {schema_name}, public")

    # Primary table
    ####

    add_history_cols_to_primary(
        table_name,
        columns=(
            'mod_user character varying(64) COLLATE pg_catalog."default" '
            "   NOT NULL DEFAULT CURRENT_USER",
        ),
    )
    # Existing trigger on obs_raw is superseded by the hx tracking trigger.
    op.execute(
        f"DROP TRIGGER IF EXISTS update_mod_time ON {pri_table_name(table_name)}"
    )
    create_primary_table_triggers(table_name)

    # History table
    ####

    create_history_table(table_name, foreign_keys)

    # Populate the history table, then update its history FKs in bulk.
    # If we let the FK trigger do this work, fired row-by-row on ~1e9 records,
    # it requires an unfeasible amount of time, so we do it in bulk.
    populate_history_table(table_name, primary_key_name)
    update_obs_raw_history_FKs()

    # History table triggers must be created after the table is populated.
    create_history_table_triggers(table_name, foreign_keys)

    create_history_table_indexes(table_name, primary_key_name, foreign_keys)
    grant_standard_table_privileges(table_name, schema=schema_name)


def downgrade():
    drop_history_triggers(table_name)
    drop_history_table(table_name)
    drop_history_cols_from_primary(table_name, columns=("mod_user",))
    op.execute(
        f"CREATE TRIGGER update_mod_time"
        f"  BEFORE UPDATE"
        f"  ON {pri_table_name(table_name)}"
        f"  FOR EACH ROW"
        f"  EXECUTE FUNCTION public.moddatetime('mod_time')"
    )
