"""Smoke tests:
- Upgrade creates views
- Downgrade drops views
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from pycds.database import get_schema_item_names


logger = logging.getLogger("tests")


view_names = {
    "crmp_network_geoserver",
    "history_join_station_network",
    "obs_count_per_day_history_v",
    "obs_with_flags",
}


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema up migration from 4a2f1879293a to 84b7fc2596d5."""
    alembic_runner.migrate_up_to("84b7fc2596d5")

    with alembic_engine.connect() as conn:
        names = get_schema_item_names(conn, "views", schema_name=schema_name)

    assert names >= view_names


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema down migration from 84b7fc2596d5 to 4a2f1879293a."""
    alembic_runner.migrate_up_to("84b7fc2596d5")

    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        # Check that views have been removed
        names = get_schema_item_names(conn, "views", schema_name=schema_name)
        assert names & view_names == set()
