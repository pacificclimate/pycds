"""
These tests simulate applying history tracking to metadata tables containing
data, as we would find in a real migration. We do this by populating the tables
with data, applying the migration, and then examining the resulting history
tables.
"""

import datetime
import logging

import pytest
from sqlalchemy.orm import Session

from pycds import (
    History,
    Variable,
    Obs,
)
from pycds.alembic.change_history_utils import hx_id_name
from pycds.database import check_migration_version, get_schema_item_names
from pycds.orm.tables import ObsHistory, HistoryHistory, VariableHistory
from tests.alembic_migrations.helpers import (
    check_history_table_initial_contents,
    check_history_tracking,
    check_history_table_FKs,
)
from .....helpers import insert_crmp_data

logging.getLogger("sqlalchemy.engine").setLevel(logging.CRITICAL)


# test_table_contents
#
# This test includes a test of the bulk FK update -- i.e., we test values of the history
# FKs in Observations / obs_raw.
#
# There is a small complication in these tests: The schema migration occurs *before* the
# data is loaded from crmp_subset_data.sql into the tables. Ideally, the data would be
# loaded first to better exercise the copy (UPDATE ... SELECT FROM ...) from the base
#  table to the history table, and, more importantly, to initialize the history tables
# in order of base table primary key. As it stands, the history tables (correctly)
# contain the base table data in *insertion* order, which is not in PK order.
#
# Therefore, to determine the values of these FKs, look in the crmp_data_subset.sql
# and find their position in order of insertion into their respective base tables.
# We will continue with this slightly suboptimal arrangement because revising the code
# to do it better would take more time than we presently have available.
#
# TODO: Load data before migration.
@pytest.mark.parametrize(
    "primary, history, primary_id, columns, foreign_tables, insert_info, update_info, delete_info",
    [
        (
            Obs,
            ObsHistory,
            "obs_raw_id",
            ("time", "datum", "vars_id", "history_id"),
            ((History, HistoryHistory), (Variable, VariableHistory)),
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
                    hx_id_name(History.__tablename__): 1,  # First inserted
                    hx_id_name(Variable.__tablename__): 1,  # First inserted
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
                    hx_id_name(History.__tablename__): 1,  # First inserted
                    hx_id_name(Variable.__tablename__): 1,  # First inserted
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
                    hx_id_name(History.__tablename__): 1,  # First inserted
                    hx_id_name(Variable.__tablename__): 1,  # First inserted
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
    schema_name,
    alembic_engine,
    alembic_runner
):
    """
    Test that contents of history tables are as expected.

    The local conftest sets us up to revision a59d64cf16ca = 8c05da87cb79 - 1.
    The database contains data loaded by `sesh_with_large_data` before we migrate to
    8c05da87cb79. Then we migrate and check the results by comparing a specified
    subset of the columns shared by the two tables.
    """

    alembic_runner.migrate_up_to("a59d64cf16ca")

    with alembic_engine.begin() as conn:
        check_migration_version(conn, version="a59d64cf16ca")
        # assert table_count(pri_table) > 0  # This blocks upgrade that follows. Sigh

        insert_crmp_data(conn)

    # Now upgrade to a59d64cf16ca
    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as conn:
        check_migration_version(conn, version="8c05da87cb79")

        with Session(conn) as session:


            # Check the resulting tables
            check_history_table_initial_contents(
                session,
                primary,
                history,
                primary_id,
                columns,
                foreign_tables,
                schema_name,
            )

            for foreign_base, foreign_history in foreign_tables:
                check_history_table_FKs(session, primary, history, foreign_base, foreign_history)

            check_history_tracking(
                session,
                primary,
                history,
                insert_info,
                update_info,
                delete_info,
                schema_name,
            )
