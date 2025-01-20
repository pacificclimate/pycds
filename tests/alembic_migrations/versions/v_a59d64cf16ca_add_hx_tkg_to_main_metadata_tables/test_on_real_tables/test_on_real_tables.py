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
    check_history_function,
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
            {"name": "test network"},
            {"where": {"name": "test network"}, "values": {"name": "test network 2"}},
            {"where": {"name": "test network 2"}},
        ),
        # (
        #     Station,
        #     StationHistory,
        #     "station_id",
        #     ("native_id", "network_id", "publish"),
        #     (Network,),
        # ),
        # (
        #     History,
        #     HistoryHistory,
        #     "history_id",
        #     ("station_id", "station_name", "lon", "lat", "elevation"),
        #     (Station,),
        # ),
        # (
        #     Variable,
        #     VariableHistory,
        #     "vars_id",
        #     ("network_id", "name", "unit", "standard_name", "cell_method"),
        #     (Network,),
        # ),
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

    print("SEQUENCES")
    result = sesh.execute(
        text(f"SELECT * FROM pg_sequences WHERE schemaname = '{schema_name}'")
    )
    for row in result:
        print(row)
    print("END SEQUENCES")

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
    check_history_function(
        sesh,
        primary,
        history,
        primary_id,
        insert_info,
        update_info,
        delete_info,
        schema_name,
    )
