"""
These tests simulate applying history tracking to metadata tables containing
data, as we would find in a real migration. We do this by populating the tables
with data, applying the migration, and then examining the resulting history
tables.
"""
import logging

import pytest
from alembic import command
from sqlalchemy import select, func, and_
from sqlalchemy.sql.operators import isnot_distinct_from
from sqlalchemy.dialects.postgresql import aggregate_order_by

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
from pycds.alembic.change_history_utils import hx_id_name
from pycds.database import check_migration_version, get_schema_item_names

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

    def table_count(table):
        return sesh.execute(select(func.count("*")).select_from(table)).scalar()

    check_migration_version(sesh, version="7ab87f8fbcf4")
    # assert table_count(pri_table) > 0  # This blocks upgrade that follows. Sigh

    for item_type in ["tables", "routines"]:
        names = set(get_schema_item_names(sesh, item_type, schema_name=schema_name))
        print(f"### {item_type} in {schema_name}:", sorted(names))

    # Now upgrade to a59d64cf16ca
    command.upgrade(alembic_config_left, "+1")
    check_migration_version(sesh, version="a59d64cf16ca")

    # Check the resulting tables

    # Count
    pri_count = table_count(primary)
    hx_count = table_count(history)
    assert pri_count == hx_count

    # Contents: check that every specified column matches between the two tables.
    stmt = (
        select(
            func.every(
                and_(
                    isnot_distinct_from(getattr(primary, col), getattr(history, col))
                    for col in columns
                )
            )
        )
        .select_from(primary)
        .join(history, primary.id == getattr(history, primary_id))
    )
    print("Contents test", stmt)
    result = sesh.execute(stmt).scalar()
    assert result

    # Order: Check that history table order by history key is the same as the order
    # by primary id.
    t1 = (
        sesh.query(
            func.array_agg(
                aggregate_order_by(
                    getattr(history, primary_id), getattr(history, primary_id)
                )
            ).label("pids")
        )
        .select_from(history)
        .subquery()
    )
    t2 = (
        sesh.query(
            func.array_agg(
                aggregate_order_by(
                    getattr(history, primary_id),
                    getattr(history, hx_id_name(primary.__tablename__)),
                )
            ).label("pids")
        )
        .select_from(history)
        .subquery()
    )
    result = sesh.query(t1.c.pids == t2.c.pids).scalar()
    assert result

    # Foreign keys: Check that foreign keys are non-null (value correctness is
    # checked elsewhere).
    if foreign_tables:
        assert sesh.query(
            func.every(
                and_(
                    getattr(history, hx_id_name(ft.__tablename__)).is_not(None)
                    for ft in foreign_tables
                )
            )
        ).scalar()
