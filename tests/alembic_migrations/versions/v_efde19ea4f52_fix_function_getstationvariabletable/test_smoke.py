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


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration to version efde19ea4f52."""

    # Set up database to version 4a2f1879293a
    engine, script = prepared_schema_from_migrations_left

    # Check that function is there (new version)
    names = get_schema_item_names(engine, "routines", schema_name=schema_name)
    assert "getstationvariabletable" in names


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 4a2f1879293a to 522eed334c85."""

    # Set up database to version efde19ea4f52
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # Check that function is still there (old version)
    names = get_schema_item_names(engine, "routines", schema_name=schema_name)
    assert "getstationvariabletable" in names
