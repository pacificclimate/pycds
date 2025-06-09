"""Smoke tests:
- Upgrade creates functions
- Downgrade drops functions
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from pycds.database import get_schema_item_names

import pycds.alembic.extensions.operation_plugins

logger = logging.getLogger("tests")


function_names = {
    "closest_stns_within_threshold",
    "daily_ts",
    "daysinmonth",
    "do_query_one_station",
    "effective_day",
    "getstationvariabletable",
    "lastdateofmonth",
    "monthly_ts",
    "query_one_station",
    "query_one_station_climo",
    "season",
    "updatesdateedate",
}


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 522eed334c85 to 4a2f1879293a."""
    alembic_runner.migrate_up_to("4a2f1879293a")

    with alembic_engine.connect() as conn:
        # Check that functions have been added
        names = get_schema_item_names(conn, "routines", schema_name=schema_name)
        assert function_names <= names


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 4a2f1879293a to 522eed334c85."""
    alembic_runner.migrate_up_to("4a2f1879293a")

    # Run downgrade migration
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        # Check that functions have been removed
        names = get_schema_item_names(conn, "routines", schema_name=schema_name)
        assert names & function_names == set()
