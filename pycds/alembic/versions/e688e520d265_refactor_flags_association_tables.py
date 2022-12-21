"""Refactor flags association tables

Revision ID: e688e520d265
Revises: bdc28573df56
Create Date: 2021-05-14 12:07:09.107616

"""
import logging
from alembic import op
from pycds import get_schema_name
from pycds.alembic.extensions.special_operations import create_primary_key_if_not_exists

# revision identifiers, used by Alembic.
revision = "e688e520d265"
down_revision = "bdc28573df56"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")

schema_name = get_schema_name()


items = (
    {
        "table_name": "obs_raw_native_flags",
        "unique_constraint_name": "obs_raw_native_flag_unique",
        "primary_key_name": "obs_raw_native_flags_pkey",
        "columns": ["obs_raw_id", "native_flag_id"],
    },
    {
        "table_name": "obs_raw_pcic_flags",
        "unique_constraint_name": "obs_raw_pcic_flag_unique",
        "primary_key_name": "obs_raw_pcic_flags_pkey",
        "columns": ["obs_raw_id", "pcic_flag_id"],
    },
)


def upgrade():
    for item in items:
        op.drop_constraint_if_exists(
            table_name=item["table_name"],
            constraint_name=item["unique_constraint_name"],
            type_="unique",
            schema=schema_name,
        )
        create_primary_key_if_not_exists(
            op,
            table_name=item["table_name"],
            constraint_name=item["primary_key_name"],
            columns=item["columns"],
            schema=schema_name,
        )


def downgrade():
    for item in items:
        op.drop_constraint(
            table_name=item["table_name"],
            constraint_name=item["primary_key_name"],
            schema=schema_name,
        )
        op.create_unique_constraint(
            table_name=item["table_name"],
            constraint_name=item["unique_constraint_name"],
            columns=item["columns"],
            schema=schema_name,
        )
