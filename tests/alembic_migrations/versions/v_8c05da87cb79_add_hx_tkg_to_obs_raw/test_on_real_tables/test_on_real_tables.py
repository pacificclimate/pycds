"""
These tests simulate applying history tracking to metadata tables containing
data, as we would find in a real migration. We do this by populating the tables
with data, applying the migration, and then examining the resulting history
tables.
"""
import datetime
import logging

import pytest
from alembic import command

from pycds import (
    History,
    Variable,
    Obs,
)
from pycds.database import check_migration_version, get_schema_item_names
from pycds.orm.tables import ObsHistory
from tests.alembic_migrations.helpers import (
    check_history_table_initial_contents,
    check_history_tracking,
)

logging.getLogger("sqlalchemy.engine").setLevel(logging.CRITICAL)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "primary, history, primary_id, columns, foreign_tables, insert_info, update_info, delete_info",
    [
        (
            Obs,
            ObsHistory,
            "obs_raw_id",
            ("time", "datum", "vars_id", "history_id"),
            (History, Variable),
            {
                "values": {
                    "time": datetime.datetime(2100, 1, 1),
                    "vars_id": 517,
                    "history_id": 13216,
                    "datum": 999,
                },
                "check": {
                    "time": datetime.datetime(2100, 1, 1),
                    "vars_id": 517,
                    "history_id": 13216,
                    "datum": 999,
                    "deleted": False,
                },
            },
            {
                "where": {
                    "time": datetime.datetime(2100, 1, 1),
                },
                "values": {
                    "datum": 1000,
                },
                "check": {
                    "time": datetime.datetime(2100, 1, 1),
                    "vars_id": 517,
                    "history_id": 13216,
                    "datum": 1000,
                    "deleted": False,
                },
            },
            {
                "where": {
                    "time": datetime.datetime(2100, 1, 1),
                },
                "check": {
                    "time": datetime.datetime(2100, 1, 1),
                    "vars_id": 517,
                    "history_id": 13216,
                    "datum": 1000,
                    "deleted": True,
                },
            },
        ),
    ],
)
def test_table_contents(
    primary,
    history,
    primary_id,
    columns,
    foreign_tables,
    insert_info,
    update_info,
    delete_info,
    prepared_schema_from_migrations_left,
    alembic_config_left,
    schema_name,
    sesh_with_large_data,
    env_config,
):
    """
    Test that contents of history tables are as expected.

    The local conftest sets us up to revision a59d64cf16ca = 8c05da87cb79 - 1.
    The database contains data loaded by `sesh_with_large_data` before we migrate to
    8c05da87cb79. Then we migrate and check the results by comparing a specified
    subset of the columns shared by the two tables.
    """
    sesh = sesh_with_large_data

    check_migration_version(sesh, version="a59d64cf16ca")
    # assert table_count(pri_table) > 0  # This blocks upgrade that follows. Sigh

    # Now upgrade to a59d64cf16ca
    command.upgrade(alembic_config_left, "+1")
    check_migration_version(sesh, version="8c05da87cb79")

    # Check the resulting tables
    check_history_table_initial_contents(
        sesh,
        primary,
        history,
        primary_id,
        columns,
        foreign_tables,
        schema_name,
    )
    check_history_tracking(
        sesh,
        primary,
        history,
        insert_info,
        update_info,
        delete_info,
        schema_name,
    )
