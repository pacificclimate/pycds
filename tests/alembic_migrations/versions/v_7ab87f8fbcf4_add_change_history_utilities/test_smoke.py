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


function_names = {
    # Ordinary functions
    "hxtk_collection_name_from_hx",
    "hxtk_hx_table_name",
    "hxtk_hx_id_name",
    # Trigger functions
    "hxtk_primary_control_hx_cols",
    "hxtk_primary_ops_to_hx",
    "hxtk_add_foreign_hx_keys",
}


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration to 7ab87f8fbcf4."""

    # Set up database to target version (7ab87f8fbcf4)
    engine, script = prepared_schema_from_migrations_left

    # Check that function has been added
    names = set(get_schema_item_names(engine, "routines", schema_name=schema_name))
    assert function_names <= names


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 7ab87f8fbcf4 to previous rev."""

    # Set up database to version 4a2f1879293a
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # Check that functions have been removed
    names = get_schema_item_names(engine, "routines", schema_name=schema_name)
    assert function_names & names == set()
