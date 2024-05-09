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

matview_defns = {"monthly_total_precipitation_mv": {"indexes": {}}}


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration to version 3505750d3416."""

    # Set up database to version 3505750d3416
    engine, script = prepared_schema_from_migrations_left

    # confirm native matview exists
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

    # confirm matview has been removed
    check_matviews(engine, matview_defns, schema_name, matviews_present=False)