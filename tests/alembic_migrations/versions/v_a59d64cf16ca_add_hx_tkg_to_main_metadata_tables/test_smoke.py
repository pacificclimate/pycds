"""Smoke tests"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import Table, MetaData, text
from sqlalchemy.types import TIMESTAMP, VARCHAR, BOOLEAN, INTEGER

from pycds.alembic.change_history_utils import (
    main_table_name,
    hx_table_name,
    hx_id_name,
)
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

@pytest.mark.update20
def test_upgrade(
    alembic_engine, alembic_runner, schema_name
):
    """Test the schema migration to a59d64cf16ca."""
    alembic_runner.migrate_up_to("a59d64cf16ca")

    # Check that tables have been altered or created as expected.
    for table_name, pri_key_name, foreign_tables in table_info:
        check_history_tracking_upgrade(
            alembic_engine, table_name, pri_key_name, foreign_tables, schema_name
        )

@pytest.mark.update20
def test_downgrade(
    alembic_engine, alembic_runner, schema_name
):
    """Test the schema migration from a59d64cf16ca to previous rev."""

    alembic_runner.migrate_up_to("a59d64cf16ca")
    alembic_runner.migrate_down_one()

    # Check that tables have been altered or dropped as expected.
    for table_name, _, _ in table_info:
        check_history_tracking_downgrade(alembic_engine, table_name, schema_name)
