"""Smoke tests:
- Upgrade creates views
- Downgrade drops views
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from pycds.database import get_schema_item_names


logger = logging.getLogger('tests')


view_names = {
    "crmp_network_geoserver",
    "history_join_station_network",
    "obs_count_per_day_history_v",
    "obs_with_flags",
}


@pytest.mark.usefixtures('new_db_left')
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration from 4a2f1879293a to 84b7fc2596d5. """

    # Set up database to version 84b7fc2596d5
    engine, script = prepared_schema_from_migrations_left

    # Check that views have been added
    names = get_schema_item_names(engine, 'views', schema_name=schema_name)
    assert names >= view_names


@pytest.mark.usefixtures('new_db_left')
def test_downgrade(prepared_schema_from_migrations_left, alembic_config_left, schema_name):
    """Test the schema migration from 84b7fc2596d5 to 4a2f1879293a. """

    # Set up database to version 84b7fc2596d5
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, '-1')

    # Check that views have been removed
    names = get_schema_item_names(engine, 'views', schema_name=schema_name)
    assert names & view_names == set()
