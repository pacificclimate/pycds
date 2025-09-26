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
    # table_name, primary_key_name, foreign_keys, extra_indexes
    ("climo_period", "climo_period_id", None),
    (
        "climo_station",
        "climo_station_id",
        [
            ("climo_period", "climo_period_id"),
        ],
    ),
    # TODO: weird one
    # ("climo_station_x_meta_history", [("climo_station_id", "history_id")], [("climo_station", "climo_period_id"), ("meta_history", "history_id")], None),
    ("climo_variable", "climo_variable_id", None),
    (
        "climo_value",
        "climo_value_id",
        [
            ("climo_variable", "climo_variable_id"),
            ("climo_station", "climo_station_id"),
        ],
    ),
)


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration to 7244176be9fa."""
    alembic_runner.migrate_up_to("7244176be9fa")

    with alembic_engine.connect() as conn:
        # Check that tables have been altered or created as expected.
        for table_name, pri_key_name, foreign_tables in table_info:
            check_history_tracking_upgrade(
                conn, table_name, pri_key_name, foreign_tables, schema_name
            )


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 7244176be9fa to previous rev."""

    alembic_runner.migrate_up_to("7244176be9fa")
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        # Check that tables have been altered or dropped as expected.
        for table_name, _, _ in table_info:
            check_history_tracking_downgrade(conn, table_name, schema_name)
