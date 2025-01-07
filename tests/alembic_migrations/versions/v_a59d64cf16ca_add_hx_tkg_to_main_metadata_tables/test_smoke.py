"""Smoke tests"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import Table, MetaData, text
from sqlalchemy.types import TIMESTAMP, VARCHAR, BOOLEAN, INTEGER

from pycds.alembic.change_history_utils import pri_table_name, hx_table_name, hx_id_name
from pycds.database import get_schema_item_names
from tests.alembic_migrations.helpers import (
    check_history_tracking_upgrade,
    check_history_tracking_downgrade,
)

logger = logging.getLogger("tests")


table_info = (
    ("meta_network", "network_id", []),
    ("meta_station", "station_id", [("meta_network", "network_id")]),
    ("meta_history", "history_id", [("meta_station", "station_id")]),
    ("meta_vars", "vars_id", [("meta_network", "network_id")]),
)


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration to a59d64cf16ca."""

    # Set up database to target version (a59d64cf16ca)
    engine, script = prepared_schema_from_migrations_left

    # Check that tables have been altered or created as expected.
    for table_name, pri_key_name, foreign_keys in table_info:
        check_history_tracking_upgrade(
            engine, table_name, pri_key_name, foreign_keys, schema_name
        )


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from a59d64cf16ca to previous rev."""

    # Set up database to current version
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # Check that tables have been altered or dropped as expected.
    for table_name, _, _ in table_info:
        check_history_tracking_downgrade(engine, table_name, schema_name)
