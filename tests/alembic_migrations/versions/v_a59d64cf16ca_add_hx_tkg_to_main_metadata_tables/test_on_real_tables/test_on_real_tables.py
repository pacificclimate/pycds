"""
These tests simulate applying history tracking to metadata tables containing
data, as we would find in a real migration. We do this by populating the tables
with data, applying the migration, and then examining the resulting history
tables.

The behaviour of the history-tracking features proper (trigger functions, etc.)
is tested in detail in the migration that installs them.

These tests test that this migration correctly modifies the base tables and creates
the history tables, and attaches the trigger functions to them. Then we do some cursory
tests to verify they are actually being called and recording history records.
"""

import logging

import pytest
from alembic import command
from sqlalchemy import text

from pycds import (
    Network,
    NetworkHistory,
    Station,
    StationHistory,
    History,
    HistoryHistory,
    Variable,
    VariableHistory,
    get_schema_name,
)
from pycds.database import check_migration_version, get_schema_item_names
from tests.alembic_migrations.helpers import (
    check_history_table_initial_contents,
    check_history_tracking,
)

logging.getLogger("sqlalchemy.engine").setLevel(logging.CRITICAL)

schema_name = get_schema_name()


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "primary, history, primary_id, columns, foreign_tables, insert_info, update_info, delete_info",
    [
        (
            Network,
            NetworkHistory,
            "network_id",
            ("name", "long_name", "publish"),
            None,
            {
                "values": {
                    "name": "test network",
                    "long_name": "test network description",
                    "publish": True,
                },
                "check": {
                    "name": "test network",
                    "long_name": "test network description",
                    "publish": True,
                    "deleted": False,
                },
            },
            {
                "where": {"name": "test network"},
                "values": {"name": "test network 2", "long_name": "foo"},
                "check": {
                    "name": "test network 2",
                    "long_name": "foo",
                    "publish": True,
                    "deleted": False,
                },
            },
            {
                "where": {"name": "test network 2"},
                "check": {
                    "name": "test network 2",
                    "long_name": "foo",
                    "publish": True,
                    "deleted": True,
                },
            },
        ),
        (
            Station,
            StationHistory,
            "station_id",
            ("native_id", "network_id", "publish"),
            ((Network, NetworkHistory),),
            {
                "values": {
                    "native_id": "WOWZA",
                    "network_id": 1,
                    "publish": True,
                },
                "check": {
                    "native_id": "WOWZA",
                    "network_id": 1,
                    "publish": True,
                    "deleted": False,
                },
            },
            {
                "where": {
                    "native_id": "WOWZA",
                },
                "values": {
                    "native_id": "WOWZA SECRET",
                    "publish": False,
                },
                "check": {
                    "deleted": False,
                },
            },
            {
                "where": {
                    "native_id": "WOWZA SECRET",
                },
                "check": {
                    "native_id": "WOWZA SECRET",
                    "publish": False,
                    "deleted": True,
                },
            },
        ),
        (
            History,
            HistoryHistory,
            "history_id",
            ("station_id", "station_name", "lon", "lat", "elevation"),
            ((Station, StationHistory),),
            {
                "values": {
                    "station_id": 4137,
                    "station_name": "MY GOODNESS",
                    "lon": -123,
                    "lat": 50,
                    "elevation": 999,
                },
                "check": {
                    "station_id": 4137,
                    "station_name": "MY GOODNESS",
                    "lon": -123,
                    "lat": 50,
                    "elevation": 999,
                    "deleted": False,
                },
            },
            {
                "where": {
                    "station_name": "MY GOODNESS",
                },
                "values": {
                    "lon": -122,
                    "lat": 51,
                    "elevation": 998,
                },
                "check": {
                    "station_id": 4137,
                    "station_name": "MY GOODNESS",
                    "lon": -122,
                    "lat": 51,
                    "elevation": 998,
                    "deleted": False,
                },
            },
            {
                "where": {
                    "station_name": "MY GOODNESS",
                },
                "check": {
                    "station_id": 4137,
                    "station_name": "MY GOODNESS",
                    "lon": -122,
                    "lat": 51,
                    "elevation": 998,
                    "deleted": True,
                },
            },
        ),
        (
            Variable,
            VariableHistory,
            "vars_id",
            ("network_id", "name", "unit", "standard_name", "cell_method"),
            ((Network, NetworkHistory),),
            {
                "values": {
                    "network_id": 1,
                    "name": "inductance",
                    "unit": "mH",
                    "standard_name": "inductance",
                    "cell_method": "boo",
                    "display_name": "Tendency to oppose change in current flow",
                },
                "check": {
                    "network_id": 1,
                    "name": "inductance",
                    "unit": "mH",
                    "standard_name": "inductance",
                    "cell_method": "boo",
                    "display_name": "Tendency to oppose change in current flow",
                    "deleted": False,
                },
            },
            {
                "where": {
                    "name": "inductance",
                },
                "values": {
                    "unit": "milliHenries",
                    "standard_name": "indooktance",
                },
                "check": {
                    "network_id": 1,
                    "name": "inductance",
                    "unit": "milliHenries",
                    "standard_name": "indooktance",
                    "cell_method": "boo",
                    "display_name": "Tendency to oppose change in current flow",
                    "deleted": False,
                },
            },
            {
                "where": {
                    "standard_name": "indooktance",
                },
                "check": {
                    "network_id": 1,
                    "name": "inductance",
                    "unit": "milliHenries",
                    "standard_name": "indooktance",
                    "cell_method": "boo",
                    "display_name": "Tendency to oppose change in current flow",
                    "deleted": True,
                },
            },
        ),
    ],
)
def test_migration_results(
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
    Test that contents of history tables are as expected, and that history tracking is
    working.

    The local conftest sets us up to revision 7ab87f8fbcf4 = a59d64cf16ca - 1.
    The database contains data loaded by `sesh_with_large_data` before we migrate to
    a59d64cf16ca. Then we migrate and check the results by comparing a specified
    subset of the columns shared by the two tables.
    """
    sesh = sesh_with_large_data

    check_migration_version(sesh, version="7ab87f8fbcf4")
    # assert table_count(pri_table) > 0  # This blocks upgrade that follows. Sigh

    # Now upgrade to a59d64cf16ca
    command.upgrade(alembic_config_left, "+1")
    check_migration_version(sesh, version="a59d64cf16ca")

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
