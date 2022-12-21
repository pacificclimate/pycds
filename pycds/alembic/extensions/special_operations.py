import logging
from pycds.database import get_schema_item_names


logger = logging.getLogger("alembic")


def create_primary_key_if_not_exists(op, constraint_name, table_name, columns, schema):
    """
    Create a primary key in a table if it does not already exist.
    SQL (and PostgreSQL) does not support an IF NOT EXISTS option for this
    operation, so we have to do this the hard way.
    """
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
