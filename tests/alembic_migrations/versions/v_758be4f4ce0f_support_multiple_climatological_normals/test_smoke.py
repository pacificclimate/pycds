"""Smoke tests"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import Table, MetaData, text
from sqlalchemy.types import TIMESTAMP, VARCHAR, BOOLEAN, INTEGER

from pycds import (
    ClimatologicalStation,
    ClimatologicalStationXHistory,
    ClimatologicalVariable,
    ClimatologicalValue,
)
from pycds.alembic.change_history_utils import pri_table_name, hx_table_name, hx_id_name
from pycds.database import get_schema_item_names
from tests.alembic_migrations.helpers import check_orm_actual_tables_match

logger = logging.getLogger("tests")

orm_tables = (
    ClimatologicalStation,
    ClimatologicalStationXHistory,
    ClimatologicalVariable,
    ClimatologicalValue,
)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize("orm_table", orm_tables)
def test_upgrade(
    orm_table, prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration to 758be4f4ce0f."""

    # Set up database to target version (758be4f4ce0f)
    engine, script = prepared_schema_from_migrations_left

    # Check that table has been created as expected.
    check_orm_actual_tables_match(engine, orm_table, schema_name=schema_name)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize("table", orm_tables)
def test_downgrade(
    table, prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from a59d64cf16ca to previous rev."""

    # Set up database to current version
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # Check that tables have been dropped as expected.
    names = set(get_schema_item_names(engine, "tables", schema_name=schema_name))
    assert table.__tablename__ not in names
