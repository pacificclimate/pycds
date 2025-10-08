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

import datetime
import logging

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.orm import Session

from pycds import (
    ClimatologicalPeriod,
    ClimatologicalPeriodHistory,
    get_schema_name,
)
from pycds.database import check_migration_version, get_schema_item_names
from pycds.orm.tables import (
    ClimatologicalStation,
    ClimatologicalStationHistory,
    ClimatologicalStationXHistory,
    ClimatologicalStationXHistoryHistory,
    ClimatologicalValue,
    ClimatologicalValueHistory,
    ClimatologicalVariable,
    ClimatologicalVariableHistory,
)
from tests.alembic_migrations.helpers import (
    check_history_table_initial_contents,
    check_history_tracking,
)

logging.getLogger("sqlalchemy.engine").setLevel(logging.CRITICAL)

schema_name = get_schema_name()


@pytest.mark.parametrize(
    "primary, history, primary_id, columns, foreign_tables, insert_info, update_info, delete_info",
    [
        (
            ClimatologicalPeriod,
            ClimatologicalPeriodHistory,
            "climo_period_id",
            ("start_date", "end_date"),
            None,
            # insert tests
            {
                "values": {
                    "start_date": "2000-01-01 00:00:00",
                    "end_date": "2010-12-31 00:00:00",
                },
                "check": {
                    "start_date": datetime.datetime(2000, 1, 1, 0, 0, 0),
                    "end_date": datetime.datetime(2010, 12, 31, 0, 0, 0),
                    "deleted": False,
                },
            },
            # update tests
            {
                "where": {"start_date": "2000-01-01 00:00:00"},
                "values": {
                    "start_date": "2000-01-02 00:00:00",
                    "end_date": "2012-12-31 00:00:00",
                },
                "check": {
                    "start_date": datetime.datetime(2000, 1, 2, 0, 0, 0),
                    "end_date": datetime.datetime(2012, 12, 31, 0, 0, 0),
                    "deleted": False,
                },
            },
            # delete tests
            {
                "where": {"start_date": "2000-01-02 00:00:00"},
                "check": {
                    "start_date": datetime.datetime(2000, 1, 2, 0, 0, 0),
                    "end_date": datetime.datetime(2012, 12, 31, 0, 0, 0),
                    "deleted": True,
                },
            },
        ),
        (
            ClimatologicalVariable,
            ClimatologicalVariableHistory,
            "climo_variable_id",
            (
                "duration",
                "unit",
                "standard_name",
                "display_name",
                "short_name",
                "cell_methods",
                "net_var_name",
            ),
            None,
            # insert tests
            {
                "values": {
                    "duration": "annual",
                    "unit": "mm",
                    "standard_name": "test_variable_new",
                    "display_name": "Test Variable New",
                    "short_name": "test_var_new",
                    "cell_methods": "time: sum",
                    "net_var_name": "test_var_new",
                },
                "check": {
                    "duration": "annual",
                    "unit": "mm",
                    "standard_name": "test_variable_new",
                    "display_name": "Test Variable New",
                    "short_name": "test_var_new",
                    "cell_methods": "time: sum",
                    "net_var_name": "test_var_new",
                    "deleted": False,
                },
            },
            # update tests
            {
                "where": {"standard_name": "test_variable_new"},
                "values": {
                    "duration": "monthly",
                    "unit": "cm",
                    "standard_name": "test_variable_updated",
                    "display_name": "Test Variable Updated",
                    "short_name": "test_var_updated",
                    "cell_methods": "time: mean",
                    "net_var_name": "test_var_updated",
                },
                "check": {
                    "duration": "monthly",
                    "unit": "cm",
                    "standard_name": "test_variable_updated",
                    "display_name": "Test Variable Updated",
                    "short_name": "test_var_updated",
                    "cell_methods": "time: mean",
                    "net_var_name": "test_var_updated",
                    "deleted": False,
                },
            },
            # delete tests - use the same updated record
            {
                "where": {"standard_name": "test_variable_updated"},
                "check": {
                    "duration": "monthly",
                    "unit": "cm",
                    "standard_name": "test_variable_updated",
                    "display_name": "Test Variable Updated",
                    "short_name": "test_var_updated",
                    "cell_methods": "time: mean",
                    "net_var_name": "test_var_updated",
                    "deleted": True,
                },
            },
        ),
        (
            ClimatologicalStation,
            ClimatologicalStationHistory,
            "climo_station_id",
            ("type", "basin_id", "comments", "climo_period_id"),
            [(ClimatologicalPeriod, "climo_period_id")],
            # insert tests
            {
                "values": {
                    "type": "long-record",
                    "basin_id": "9999",
                    "comments": "Test station for climatological data - new",
                    "climo_period_id": "1",
                },
                "check": {
                    "type": "long-record",
                    "basin_id": 9999,
                    "comments": "Test station for climatological data - new",
                    "climo_period_id": 1,
                    "deleted": False,
                },
            },
            # update tests
            {
                "where": {"comments": "Test station for climatological data - new"},
                "values": {
                    "type": "composite",
                    "basin_id": "8888",
                    "comments": "Updated test station for climatological data",
                    "climo_period_id": "1",
                },
                "check": {
                    "type": "composite",
                    "basin_id": 8888,
                    "comments": "Updated test station for climatological data",
                    "climo_period_id": 1,
                    "deleted": False,
                },
            },
            # delete tests
            {
                "where": {"comments": "Updated test station for climatological data"},
                "check": {
                    "type": "composite",
                    "basin_id": 8888,
                    "comments": "Updated test station for climatological data",
                    "climo_period_id": 1,
                    "deleted": True,
                },
            },
        ),
        (
            ClimatologicalValue,
            ClimatologicalValueHistory,
            "climo_value_id",
            (
                "value_time",
                "value",
                "num_contributing_years",
                "climo_variable_id",
                "climo_station_id",
            ),
            [
                (ClimatologicalVariable, "climo_variable_id"),
                (ClimatologicalStation, "climo_station_id"),
            ],
            # insert tests
            {
                "values": {
                    "value_time": "2020-06-15 00:00:00",
                    "value": "125.5",
                    "num_contributing_years": "30",
                    "climo_variable_id": "1",
                    "climo_station_id": "1",
                },
                "check": {
                    "value_time": datetime.datetime(2020, 6, 15, 0, 0, 0),
                    "value": 125.5,
                    "num_contributing_years": 30,
                    "climo_variable_id": 1,
                    "climo_station_id": 1,
                    "deleted": False,
                },
            },
            # update tests
            {
                "where": {"value_time": "2020-06-15 00:00:00"},
                "values": {
                    "value_time": "2020-07-15 00:00:00",
                    "value": "98.2",
                    "num_contributing_years": "25",
                    "climo_variable_id": "1",
                    "climo_station_id": "1",
                },
                "check": {
                    "value_time": datetime.datetime(2020, 7, 15, 0, 0, 0),
                    "value": 98.2,
                    "num_contributing_years": 25,
                    "climo_variable_id": 1,
                    "climo_station_id": 1,
                    "deleted": False,
                },
            },
            # delete tests
            {
                "where": {"value_time": "2020-07-15 00:00:00"},
                "check": {
                    "value_time": datetime.datetime(2020, 7, 15, 0, 0, 0),
                    "value": 98.2,
                    "num_contributing_years": 25,
                    "climo_variable_id": 1,
                    "climo_station_id": 1,
                    "deleted": True,
                },
            },
        ),
        # TODO: Fix helper functions to support composite primary keys
        # (
        #     ClimatologicalStationXHistory,
        #     ClimatologicalStationXHistoryHistory,
        #     "climo_station_id",  # Use single primary key for now since helper doesn't support composite
        #     ("climo_station_id", "history_id", "role"),
        #     [(ClimatologicalStation, "climo_station_id")],  # Foreign key to climo_station table
        #     # insert tests
        #     {
        #         "values": {
        #             "climo_station_id": "2",
        #             "history_id": "13216",
        #             "role": "base",
        #         },
        #         "check": {
        #             "climo_station_id": 2,
        #             "history_id": 13216,
        #             "role": "base",
        #             "deleted": False,
        #         },
        #     },
        #     # update tests
        #     {
        #         "where": {"climo_station_id": "2", "history_id": "13216"},
        #         "values": {
        #             "climo_station_id": "3",
        #             "history_id": "13216",
        #             "role": "joint",
        #         },
        #         "check": {
        #             "climo_station_id": 3,
        #             "history_id": 13216,
        #             "role": "joint",
        #             "deleted": False,
        #         },
        #     },
        #     # delete tests
        #     {
        #         "where": {"climo_station_id": "3", "history_id": "13216"},
        #         "check": {
        #             "climo_station_id": 3,
        #             "history_id": 13216,
        #             "role": "joint",
        #             "deleted": True,
        #         },
        #     },
        # ),
    ],
)
@pytest.mark.usefixtures("db_with_large_data")
def test_migration_results(
    alembic_engine,
    alembic_runner,
    primary,
    history,
    primary_id,
    columns,
    foreign_tables,
    insert_info,
    update_info,
    delete_info,
    schema_name,
):
    """
    Test that contents of history tables are as expected, and that history tracking is
    working.

    The local conftest sets us up to revision 758be4f4ce0f = 7244176be9fa - 1.
    The database contains data loaded by `sesh_with_large_data` before we migrate to
    758be4f4ce0f. Then we migrate and check the results by comparing a specified
    subset of the columns shared by the two tables.
    """

    with alembic_engine.connect() as conn:
        check_migration_version(conn, version="758be4f4ce0f")

    # Now upgrade to 7244176be9fa
    alembic_runner.migrate_up_one()

    with Session(alembic_engine) as conn:
        # as a result of some side effect in the migration the search path
        # is being erronously set to "$user", public. I don't see us doing this in our code,
        # and the side effect happened when modifying the db_with_large_data fixture,
        # so I'm being practical and just forcing it to the right value here.
        conn.execute(text(f"SET search_path TO {schema_name}, public"))
        check_migration_version(conn, version="7244176be9fa")

        # Check the resulting tables
        check_history_table_initial_contents(
            conn,
            primary,
            history,
            primary_id,
            columns,
            foreign_tables,
            schema_name,
        )
        check_history_tracking(
            conn,
            primary,
            history,
            insert_info,
            update_info,
            delete_info,
            schema_name,
        )
