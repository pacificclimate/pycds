"""Smoke tests:
- Upgrade creates functions
- Downgrade drops functions
- Created functions are executable
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from pycds.database import get_schema_item_names


logger = logging.getLogger("tests")


# Matviews are implemented as tables.
table_names = {
    "daily_max_temperature_mv",
    "daily_min_temperature_mv",
    "monthly_average_of_daily_max_temperature_mv",
    "monthly_average_of_daily_min_temperature_mv",
    "monthly_total_precipitation_mv",
}


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 84b7fc2596d5 to 8fd8f556c548."""
    alembic_runner.migrate_up_to("8fd8f556c548")

    # Check that views have been added
    names = get_schema_item_names(alembic_engine, "tables", schema_name=schema_name)
    assert names >= table_names


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 8fd8f556c548 to 84b7fc2596d5."""
    alembic_runner.migrate_up_to("8fd8f556c548")

    alembic_runner.migrate_down_one()

    # Check that views have been removed
    names = get_schema_item_names(alembic_engine, "tables", schema_name=schema_name)
    assert names & table_names == set()
