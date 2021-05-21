"""Smoke tests:
- Upgrade
- Downgrade
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from ....helpers import get_schema_item_names
import pycds.alembic.helpers


logger = logging.getLogger("tests")
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

test_items = [
    (
        "obs_raw_native_flags",
        "obs_raw_native_flag_unique",
        "obs_raw_native_flags_pkey",
        ("obs_raw_id", "native_flag_id"),
    ),
    (
        "obs_raw_pcic_flags",
        "obs_raw_pcic_flag_unique",
        "obs_raw_pcic_flags_pkey",
        ("obs_raw_id", "pcic_flag_id"),
    ),
]


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", ("bdc28573df56",), indirect=True
)
@pytest.mark.parametrize(
    "table_name, unique_constraint_name, primary_key_name, pkey_columns",
    test_items,
)
@pytest.mark.parametrize("unique_exists", [True, False])
@pytest.mark.parametrize("primary_key_exists", [True, False])
def test_upgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    table_name,
    unique_constraint_name,
    unique_exists,
    primary_key_name,
    pkey_columns,
    primary_key_exists,
    schema_name,
):
    """Test the schema migration from bdc28573df56 to e688e520d265. """

    # Set up database to revision bdc28573df56
    engine, script = prepared_schema_from_migrations_left

    # Prep the database according to test conditions
    if not unique_exists:
        logger.debug(f"Dropping {unique_constraint_name}")
        engine.execute(
            f"ALTER TABLE {schema_name}.{table_name} "
            f"DROP CONSTRAINT {unique_constraint_name}"
        )

    if primary_key_exists:
        logger.debug(f"Adding {primary_key_name}")
        engine.execute(
            f"ALTER TABLE {schema_name}.{table_name} "
            f"ADD CONSTRAINT {primary_key_name} "
            f"PRIMARY KEY ({', '.join(pkey_columns)})"
        )

    # Upgrade to revision e688e520d265
    command.upgrade(alembic_config_left, "+1")

    # Check that unique constraint does not exist
    unique_constraint_names = get_schema_item_names(
        engine,
        "constraints",
        table_name=table_name,
        constraint_type="unique",
        schema_name=schema_name,
    )
    assert unique_constraint_name not in unique_constraint_names

    # Check that primary key exists
    pkey_constraint_names = get_schema_item_names(
        engine,
        "constraints",
        table_name=table_name,
        constraint_type="primary",
        schema_name=schema_name,
    )
    assert primary_key_name in pkey_constraint_names


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "table_name, unique_constraint_name, primary_key_name, pkey_columns",
    test_items,
)
def test_downgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    table_name,
    unique_constraint_name,
    primary_key_name,
    pkey_columns,
    schema_name,
):
    """Test the schema migration from e688e520d265 to bdc28573df56 . """

    # Set up database to revision e688e520d265
    engine, script = prepared_schema_from_migrations_left

    # Downgrade to revision bdc28573df56
    command.downgrade(alembic_config_left, "-1")

    # Check that the primary key does not exist
    pkey_constraint_names = get_schema_item_names(
        engine,
        "constraints",
        table_name=table_name,
        constraint_type="primary",
        schema_name=schema_name,
    )
    assert primary_key_name not in pkey_constraint_names

    # Check that the unique constraint exists
    unique_constraint_names = get_schema_item_names(
        engine,
        "constraints",
        table_name=table_name,
        constraint_type="unique",
        schema_name=schema_name,
    )
    assert unique_constraint_name in unique_constraint_names
