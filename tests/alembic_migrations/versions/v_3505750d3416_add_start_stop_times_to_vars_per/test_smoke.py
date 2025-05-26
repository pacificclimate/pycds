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

@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration to version 3505750d3416."""

    # Set up database to version 3505750d3416
    alembic_runner.migrate_up_to("3505750d3416")

    # this check only confirms that the matview and its index exist;
    # it's hard to directly check the columns via sqlalchemy.
    # Behavioural tests address this elsewhere.
    check_matviews(alembic_engine, matview_defns, schema_name, matviews_present=True)

@pytest.mark.update20
def test_downgrade(
    alembic_engine, alembic_runner, schema_name
):
    """Test the schema migration from 3505750d3416 down one"""

    # Set up database to version efde19ea4f52
    alembic_runner.migrate_up_to("3505750d3416")

    # Run downgrade migration
    alembic_runner.migrate_down_one()

    # this check only confirms that the matview and its index exist;
    # it's hard to directly check the columns via sqlalchemy.
    # Behavioural tests address this elsewhere.
    check_matviews(alembic_engine, matview_defns, schema_name, matviews_present=True)
