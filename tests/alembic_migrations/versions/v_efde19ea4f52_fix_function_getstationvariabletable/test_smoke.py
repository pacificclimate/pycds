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


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration to version efde19ea4f52."""

    # Set up database to version efde19ea4f52
    alembic_runner.migrate_up_to("efde19ea4f52")

    with alembic_engine.begin() as conn:
        # Check that function is there (new version)
        names = get_schema_item_names(conn, "routines", schema_name=schema_name)
    assert "getstationvariabletable" in names


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from efde19ea4f52 to 6cb393f711c3."""

    # Set up database to version 4a2f1879293a
    alembic_runner.migrate_up_to("efde19ea4f52")

    # Run downgrade migration
    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as conn:
        # Check that function is still there (old version)
        names = get_schema_item_names(conn, "routines", schema_name=schema_name)
    assert "getstationvariabletable" in names
