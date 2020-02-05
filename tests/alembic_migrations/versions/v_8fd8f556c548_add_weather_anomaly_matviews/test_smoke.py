"""Smoke tests:
- Upgrade creates functions
- Downgrade drops functions
- Created functions are executable
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from ....helpers import get_schema_item_names


logger = logging.getLogger("tests")


# Matviews are implemented as tables.
table_names = {
    "daily_max_temperature_mv",
    "daily_min_temperature_mv",
    "monthly_average_of_daily_max_temperature_mv",
    "monthly_average_of_daily_min_temperature_mv",
    "monthly_total_precipitation_mv",
}


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration from 4a2f1879293a to 84b7fc2596d5. """

    # Set up database to version 84b7fc2596d5
    engine, script = prepared_schema_from_migrations_left

    # Check that views have been added
    names = get_schema_item_names(engine, "tables", schema_name=schema_name)
    assert names >= table_names


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 84b7fc2596d5 to 4a2f1879293a. """

    # Set up database to version 84b7fc2596d5
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # Check that views have been removed
    names = get_schema_item_names(engine, "tables", schema_name=schema_name)
    assert names & table_names == set()
