"""Smoke tests:
- Upgrade adds publish attribute to meta station table
- Downgrade drops publish attirbute from meta station table
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from sqlalchemy import inspect, null


logger = logging.getLogger("tests")

column_name = "publish"
table_name = "meta_station"


def check_if_column_exists(col_name, sch_cols):
    for col in sch_cols:
        if col["name"] == col_name:
            return col
    return null


@pytest.mark.update20
def test_upgrade(
    alembic_engine,
    alembic_runner,
    schema_name,
):
    """Test the schema migration from 7b139906ac46 to 879f0efa125f."""
    alembic_runner.migrate_up_to("879f0efa125f")

    # Check that column has been added to meta_station
    meta_station_table = inspect(alembic_engine).get_columns(
        table_name, schema=schema_name
    )
    col = check_if_column_exists(column_name, meta_station_table)

    assert (col["nullable"] == False) and (col["name"] == column_name)


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 879f0efa125f to 7b139906ac46."""
    alembic_runner.migrate_up_to("879f0efa125f")

    # Downgrade to revision 7b139906ac46
    alembic_runner.migrate_down_one()

    # Check that cloumn has been removed from meta_station
    meta_station_table = inspect(alembic_engine).get_columns(
        table_name, schema=schema_name
    )
    col = check_if_column_exists(column_name, meta_station_table)

    assert col == null
