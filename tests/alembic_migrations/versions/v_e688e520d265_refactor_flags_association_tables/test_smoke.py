"""Smoke tests:
- Upgrade
- Downgrade
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from pycds.database import get_schema_item_names
from sqlalchemy import text


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


@pytest.mark.update20
@pytest.mark.parametrize(
    "table_name, unique_constraint_name, primary_key_name, pkey_columns",
    test_items,
)
@pytest.mark.parametrize("unique_exists", [True, False])
@pytest.mark.parametrize("primary_key_exists", [True, False])
def test_upgrade(
    alembic_engine,
    alembic_runner,
    table_name,
    unique_constraint_name,
    unique_exists,
    primary_key_name,
    pkey_columns,
    primary_key_exists,
    schema_name,
):
    """Test the schema migration from bdc28573df56 to e688e520d265."""
    alembic_runner.migrate_up_before("e688e520d265")

    # Prep the database according to test conditions
    if not unique_exists:
        logger.debug(f"Dropping {unique_constraint_name}")
        alembic_engine.execute(text(
            f"ALTER TABLE {schema_name}.{table_name} "
            f"DROP CONSTRAINT {unique_constraint_name}"
        ))

    if primary_key_exists:
        logger.debug(f"Adding {primary_key_name}")
        alembic_engine.execute(text(
            f"ALTER TABLE {schema_name}.{table_name} "
            f"ADD CONSTRAINT {primary_key_name} "
            f"PRIMARY KEY ({', '.join(pkey_columns)})"
        ))

    # Upgrade to revision e688e520d265
    alembic_runner.migrate_up_one()

    # Check that unique constraint does not exist
    unique_constraint_names = get_schema_item_names(
        alembic_engine,
        "constraints",
        table_name=table_name,
        constraint_type="unique",
        schema_name=schema_name,
    )
    assert unique_constraint_name not in unique_constraint_names

    # Check that primary key exists
    pkey_constraint_names = get_schema_item_names(
        alembic_engine,
        "constraints",
        table_name=table_name,
        constraint_type="primary",
        schema_name=schema_name,
    )
    assert primary_key_name in pkey_constraint_names


@pytest.mark.update20
@pytest.mark.parametrize(
    "table_name, unique_constraint_name, primary_key_name, pkey_columns",
    test_items,
)
def test_downgrade(
    alembic_engine,
    alembic_runner,
    table_name,
    unique_constraint_name,
    primary_key_name,
    pkey_columns,
    schema_name,
):
    """Test the schema migration from e688e520d265 to bdc28573df56 ."""
    alembic_runner.migrate_up_to("e688e520d265")

    # Downgrade to revision bdc28573df56
    alembic_runner.migrate_down_one()

    # Check that the primary key does not exist
    pkey_constraint_names = get_schema_item_names(
        alembic_engine,
        "constraints",
        table_name=table_name,
        constraint_type="primary",
        schema_name=schema_name,
    )
    assert primary_key_name not in pkey_constraint_names

    # Check that the unique constraint exists
    unique_constraint_names = get_schema_item_names(
        alembic_engine,
        "constraints",
        table_name=table_name,
        constraint_type="unique",
        schema_name=schema_name,
    )
    assert unique_constraint_name in unique_constraint_names
