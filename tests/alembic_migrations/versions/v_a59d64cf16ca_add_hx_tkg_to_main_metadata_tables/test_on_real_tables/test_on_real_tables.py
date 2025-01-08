"""
These tests simulate applying history tracking to metadata tables containing
data, as we would find in a real migration. We do this by populating the tables
with data, applying the migration, and then examining the resulting history
tables.
"""
import logging

import pytest
from alembic import command

from pycds import (
    Network,
    NetworkHistory,
    Station,
    StationHistory,
    History,
    HistoryHistory,
    Variable,
    VariableHistory,
)
from pycds.database import check_migration_version, get_schema_item_names
from tests.alembic_migrations.helpers import check_history_table_contents

logging.getLogger("sqlalchemy.engine").setLevel(logging.CRITICAL)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "primary, history, primary_id, columns, foreign_tables",
    [
        (Network, NetworkHistory, "network_id", ("name", "long_name", "publish"), None),
        (
            Station,
            StationHistory,
            "station_id",
            ("native_id", "network_id", "publish"),
            (Network,),
        ),
        (
            History,
            HistoryHistory,
            "history_id",
            ("station_id", "station_name", "lon", "lat", "elevation"),
            (Station,),
        ),
        (
            Variable,
            VariableHistory,
            "vars_id",
            ("network_id", "name", "unit", "standard_name", "cell_method"),
            (Network,),
        ),
    ],
)
def test_table_contents(
    primary,
    history,
    primary_id,
    columns,
    foreign_tables,
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

    # Check the resulting tables
    check_history_table_contents(
        sesh,
        primary,
        history,
        primary_id,
        columns,
        foreign_tables,
        schema_name,
    )
