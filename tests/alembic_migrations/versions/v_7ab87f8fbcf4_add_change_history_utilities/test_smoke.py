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


def test_upgrade(
    alembic_engine, alembic_runner, schema_name
):
    """Test the schema migration to 7ab87f8fbcf4."""
    alembic_runner.migrate_up_to("7ab87f8fbcf4")

    # Check that function has been added
    names = set(get_schema_item_names(alembic_engine, "routines", schema_name=schema_name))
    assert function_names <= names


def test_downgrade(
    alembic_engine, alembic_runner, schema_name
):
    """Test the schema migration from 7ab87f8fbcf4 to previous rev."""
    alembic_runner.migrate_up_to("7ab87f8fbcf4")
    alembic_runner.migrate_down_one()

    # Check that functions have been removed
    names = get_schema_item_names(alembic_engine, "routines", schema_name=schema_name)
    assert function_names & names == set()
