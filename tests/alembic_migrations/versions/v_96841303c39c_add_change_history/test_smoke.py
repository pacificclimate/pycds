"""Smoke tests:
- Upgrade creates functions
- Downgrade drops functions
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from pycds.database import get_schema_item_names


logger = logging.getLogger("tests")


table_names = { "obs_raw_cxhx" }


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration from 522eed334c85 to 4a2f1879293a. """

    # Set up database to version 4a2f1879293a
    engine, script = prepared_schema_from_migrations_left

    # Check that functions have been added
    names = get_schema_item_names(engine, "tables", schema_name=schema_name)
    assert names >= table_names


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 4a2f1879293a to 522eed334c85. """

    # Set up database to version 4a2f1879293a
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # Check that functions have been removed
    names = get_schema_item_names(engine, "tables", schema_name=schema_name)
    assert names & table_names == set()
