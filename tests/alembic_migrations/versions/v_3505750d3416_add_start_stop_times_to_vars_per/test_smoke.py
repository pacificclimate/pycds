"""Smoke tests:
- Upgrade adds columns to matview
- Downgrade drops columns from matview
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import inspect
from pycds.database import get_schema_item_names

logger = logging.getLogger("tests")


from .. import check_matviews

matview_defns = {"vars_per_history_mv": {"indexes": {"var_hist_idx"}}}

@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration to version 3505750d3416."""

    # Set up database to version 3505750d3416
    engine, script = prepared_schema_from_migrations_left

    
    # this check only confirms that the matview and its index exist;
    # it's hard to directly check the columns via sqlalchemy. 
    # Behavioural tests address this elsewhere.
    check_matviews(engine, matview_defns, schema_name, matviews_present=True)


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 3505750d3416 to efde19ea4f52."""

    # Set up database to version efde19ea4f52
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # this check only confirms that the matview and its index exist;
    # it's hard to directly check the columns via sqlalchemy. 
    # Behavioural tests address this elsewhere.
    check_matviews(engine, matview_defns, schema_name, matviews_present=True)
