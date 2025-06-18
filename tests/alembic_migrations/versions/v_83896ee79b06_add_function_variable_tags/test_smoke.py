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


function_name = "variable_tags"


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration to 83896ee79b06."""
    # Set up database to target version (83896ee79b06)
    alembic_runner.migrate_up_to("83896ee79b06")

    with alembic_engine.begin() as conn:
        # Check that function has been added
        names = get_schema_item_names(conn, "routines", schema_name=schema_name)
    assert function_name in names


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 83896ee79b06 to 879f0efa125f."""

    alembic_runner.migrate_up_before("879f0efa125f")

    # Run downgrade migration
    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as conn:
        # Check that functions have been removed
        names = get_schema_item_names(conn, "routines", schema_name=schema_name)
    assert function_name not in names
