"""Smoke tests:
- Upgrade adds publish attribute to meta station table
- Downgrade drops publish attirbute from meta station table
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from sqlalchemy import inspect, null
from alembic import command


logger = logging.getLogger("tests")

column_name = "publish"
table_name = "meta_station"

def check_if_column_exists(col_name, sch_cols):
    for col in sch_cols:
        if col["name"] == col_name:
            return col
    return null

@pytest.mark.usefixtures("new_db_left")
def test_upgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    schema_name,
):
    """Test the schema migration from 3d50ec832e47 to 879f0efa125f."""

    # Set up database to revision  879f0efa125f
    engine, script = prepared_schema_from_migrations_left

    # Check that column has been added to meta_station
    meta_station_table = inspect(engine).get_columns(table_name, schema=schema_name)
    col = check_if_column_exists(column_name, meta_station_table)

    assert (col["nullable"] == False) and (col["name"] == column_name)


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name):
    """Test the schema migration from 879f0efa125f to 3d50ec832e47."""

    # Set up database to revision 879f0efa125f
    engine, script = prepared_schema_from_migrations_left

    # Downgrade to revision 3d50ec832e47
    command.downgrade(alembic_config_left, "-1")

    # Check that cloumn has been removed from meta_station
    meta_station_table = inspect(engine).get_columns(table_name, schema=schema_name)
    col = check_if_column_exists(column_name, meta_station_table)

    assert (col == null)
    