"""Refactor flags association tables

Revision ID: e688e520d265
Revises: bdc28573df56
Create Date: 2021-05-14 12:07:09.107616

"""
import logging
from alembic import op, context
from pycds import get_schema_name
from pycds.alembic.helpers import create_primary_key_if_not_exists

# revision identifiers, used by Alembic.
revision = "e688e520d265"
down_revision = "bdc28573df56"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")

schema_name = get_schema_name()


def upgrade():
    # Table obs_raw_native_flags
    table_name = "obs_raw_native_flags"
    op.drop_constraint_if_exists(
        table_name=table_name,
        constraint_name="obs_raw_native_flag_unique",
        type_="unique",
        schema=schema_name,
    )
    create_primary_key_if_not_exists(
        op,
        table_name=table_name,
        constraint_name="obs_raw_native_flags_pkey",
        columns=["obs_raw_id", "native_flag_id"],
        schema=schema_name,
    )

    # Table obs_raw_pcic_flags
    table_name = "obs_raw_pcic_flags"
    op.drop_constraint_if_exists(
        table_name=table_name,
        constraint_name="obs_raw_pcic_flag_unique",
        type_="unique",
        schema=schema_name,
    )
    create_primary_key_if_not_exists(
        op,
        table_name=table_name,
        constraint_name="obs_raw_pcic_flags_pkey",
        columns=["obs_raw_id", "pcic_flag_id"],
        schema=schema_name,
    )


def downgrade():
    pass
