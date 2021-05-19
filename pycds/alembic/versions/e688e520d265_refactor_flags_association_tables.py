"""Refactor flags association tables

Revision ID: e688e520d265
Revises: bdc28573df56
Create Date: 2021-05-14 12:07:09.107616

"""
import logging
from alembic import op, context
import sqlalchemy as sa
from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import reflection
from pycds import get_schema_name
from pycds.alembic.helpers import get_schema_item_names, log_context_info

# revision identifiers, used by Alembic.
revision = "e688e520d265"
down_revision = "bdc28573df56"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")

schema_name = get_schema_name()


def drop_constraint_if_exists(
    op, table_name, constraint_name, constraint_type, schema
):
    bind = op.get_bind()
    existing_constraint_names = get_schema_item_names(
        bind,
        "constraints",
        table_name=table_name,
        constraint_type=constraint_type,
        schema_name=schema,
    )
    logger.debug(f"### drop_constraint_if_exists {existing_constraint_names}")
    if constraint_name in existing_constraint_names:
        logger.info(
            f"Dropping {constraint_type} constraint '{constraint_name}' "
            f"from table '{table_name}'."
        )
        op.drop_constraint(
            constraint_name=constraint_name,
            table_name=table_name,
            schema=schema,
        )
    else:
        logger.info(
            f"Table '{table_name}' does not have {constraint_type} constraint "
            f"'{constraint_name}': skipping drop."
        )


def create_primary_key_if_not_exists(
    op, table_name, constraint_name, columns, schema,
):
    bind = op.get_bind()
    pkey_constraint_names = get_schema_item_names(
        bind,
        "constraints",
        table_name=table_name,
        constraint_type="primary",
        schema_name=schema,
    )
    if constraint_name in pkey_constraint_names:
        logger.info(
            f"Primary key '{constraint_name}' already exists in "
            f"table '{table_name}': skipping create."
        )
    else:
        logger.info(
            f"Creating primary key '{constraint_name}' in table '{table_name}'."
        )
        op.create_primary_key(
            constraint_name=constraint_name,
            table_name=table_name,
            columns=columns,
            schema=schema,
        )


def upgrade():
    # Table obs_raw_native_flags
    table_name = "obs_raw_native_flags"
    op.drop_constraint_if_exists(
        table_name=table_name,
        constraint_name="obs_raw_native_flag_unique",
        type_="unique",
        schema=schema_name,
    )
    # drop_constraint_if_exists(
    #     op,
    #     table_name=table_name,
    #     constraint_name="obs_raw_native_flag_unique",
    #     constraint_type="unique",
    #     schema=schema_name,
    # )
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
    # drop_constraint_if_exists(
    #     op,
    #     table_name=table_name,
    #     constraint_name="obs_raw_pcic_flag_unique",
    #     constraint_type="unique",
    #     schema=schema_name,
    # )
    create_primary_key_if_not_exists(
        op,
        table_name=table_name,
        constraint_name="obs_raw_pcic_flags_pkey",
        columns=["obs_raw_id", "pcic_flag_id"],
        schema=schema_name,
    )


def downgrade():
    pass
