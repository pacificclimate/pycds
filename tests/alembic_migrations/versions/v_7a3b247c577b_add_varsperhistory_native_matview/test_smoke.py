"""Smoke tests:
- Upgrade drops table and creates matview
- Downgrade drops matview and creates table
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from ....helpers import get_schema_item_names


logger = logging.getLogger("tests")


matview_names = {"vars_per_history_mv"}


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration from 84b7fc2596d5 to 7a3b247c577b. """

    # Set up database to revision 7a3b247c577b
    engine, script = prepared_schema_from_migrations_left

    # Check that table has been replaced with matview
    names = get_schema_item_names(engine, "tables", schema_name=schema_name)
    assert names & matview_names == set()
    names = get_schema_item_names(engine, "matviews", schema_name=schema_name)
    assert names >= matview_names


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 7a3b247c577b to 84b7fc2596d5. """

    # Set up database to revision 7a3b247c577b
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration to revision 
    command.downgrade(alembic_config_left, "-1")

    # Check that matview has been replaced with table
    names = get_schema_item_names(engine, "matviews", schema_name=schema_name)
    assert names & matview_names == set()
    names = get_schema_item_names(engine, "tables", schema_name=schema_name)
    assert names >= matview_names
