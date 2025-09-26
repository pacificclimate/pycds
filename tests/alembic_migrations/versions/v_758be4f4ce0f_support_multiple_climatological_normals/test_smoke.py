"""Smoke tests"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import Table, MetaData, text
from sqlalchemy.types import TIMESTAMP, VARCHAR, BOOLEAN, INTEGER

from pycds import (
    ClimatologicalPeriod,
    ClimatologicalStation,
    ClimatologicalStationXHistory,
    ClimatologicalVariable,
    ClimatologicalValue,
)
from pycds.alembic.change_history_utils import hx_table_name, hx_id_name
from pycds.database import get_schema_item_names
from tests.alembic_migrations.helpers import check_orm_actual_tables_match

logger = logging.getLogger("tests")

orm_tables = (
    ClimatologicalPeriod,
    ClimatologicalStation,
    ClimatologicalStationXHistory,
    ClimatologicalVariable,
    ClimatologicalValue,
)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize("orm_table", orm_tables)
def test_upgrade(orm_table, alembic_engine, alembic_runner, schema_name):
    """Test the schema migration to 758be4f4ce0f."""

    alembic_runner.migrate_up_to("758be4f4ce0f")

    with alembic_engine.connect() as conn:
        # Check that table has been created as expected.
        check_orm_actual_tables_match(conn, orm_table, schema_name=schema_name)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize("table", orm_tables)
def test_downgrade(table, alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from a59d64cf16ca to previous rev."""

    # Set up database to current version, then back off one
    alembic_runner.migrate_up_to("758be4f4ce0f")
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        # Check that columns have been dropped as expected.
        names = set(get_schema_item_names(conn, "tables", schema_name=schema_name))
        assert table.__tablename__ not in names
