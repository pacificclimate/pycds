"""Smoke tests:
- Upgrade adds network_key column to meta_network table and meta_network_hx table
- Downgrade drops network_key column from meta_network table and meta_network_hx table
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from sqlalchemy import inspect, null


logger = logging.getLogger("tests")

column_name = "network_key"
table_name = "meta_network"
history_table_name = "meta_network_hx"


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
    """Test the schema migration from 8c05da87cb79 to 33179b5ae85a."""
    alembic_runner.migrate_up_to("33179b5ae85a")

    # Check that column has been added to meta_network
    meta_network_table = inspect(alembic_engine).get_columns(
        table_name, schema=schema_name
    )
    col = check_if_column_exists(column_name, meta_network_table)
    assert (col["nullable"] == True) and (col["name"] == column_name)

    # Check that column has been added to meta_network_hx
    meta_network_hx_table = inspect(alembic_engine).get_columns(
        history_table_name, schema=schema_name
    )
    col_hx = check_if_column_exists(column_name, meta_network_hx_table)
    assert (col_hx["nullable"] == True) and (col_hx["name"] == column_name)


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 33179b5ae85a to 8c05da87cb79."""
    alembic_runner.migrate_up_to("33179b5ae85a")

    # Downgrade to revision 8c05da87cb79
    alembic_runner.migrate_down_one()

    # Check that column has been removed from meta_network
    meta_network_table = inspect(alembic_engine).get_columns(
        table_name, schema=schema_name
    )
    col = check_if_column_exists(column_name, meta_network_table)
    assert col == null

    # Check that column has been removed from meta_network_hx
    meta_network_hx_table = inspect(alembic_engine).get_columns(
        history_table_name, schema=schema_name
    )
    col_hx = check_if_column_exists(column_name, meta_network_hx_table)
    assert col_hx == null
