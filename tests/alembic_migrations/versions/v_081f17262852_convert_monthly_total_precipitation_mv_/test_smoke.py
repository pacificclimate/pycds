"""Smoke tests:
- Upgrade drops matview-style tables and adds matviews
- Downgrade drops matviews and restores matview-style tables
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import inspect
from pycds.database import get_schema_item_names

logger = logging.getLogger("tests")


from .. import check_matviews

matview_defns = {
    "monthly_total_precipitation_mv": {
        "indexes": {"monthly_total_precipitation_mv_idx"}
    },
    "daily_max_temperature_mv": {
        "indexes": {
            "daily_max_temperature_mv_idx",
        }
    },
    "daily_min_temperature_mv": {"indexes": {"daily_min_temperature_mv_idx"}},
    "monthly_average_of_daily_max_temperature_mv": {
        "indexes": {"monthly_average_of_daily_max_temperature_mv_idx"}
    },
    "monthly_average_of_daily_min_temperature_mv": {
        "indexes": {
            "monthly_average_of_daily_min_temperature_mv_idx",
        }
    },
}

@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration to version 081f17262852."""
    
    alembic_runner.migrate_up_to("081f17262852")

    # confirm native matview exists
    check_matviews(alembic_engine, matview_defns, schema_name, matviews_present=True)

@pytest.mark.update20
def test_downgrade(
    alembic_engine, alembic_runner, schema_name
):
    """Test the schema migration from 081f17262852 to 3505750d3416."""

    alembic_runner.migrate_up_to("081f17262852")
    alembic_runner.migrate_down_one()

    # confirm matview has been removed
    check_matviews(alembic_engine, matview_defns, schema_name, matviews_present=False)
