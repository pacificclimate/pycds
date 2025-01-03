"""Smoke tests"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import Table, MetaData, func, select, text
from sqlalchemy.types import TIMESTAMP, VARCHAR, BOOLEAN, INTEGER

from pycds.alembic.change_history_utils import pri_table_name, hx_table_name, hx_id_name
from pycds.database import get_schema_item_names


logger = logging.getLogger("tests")


table_info = (
    ("meta_network", "network_id", []),
    ("meta_station", "station_id", [("meta_network", "network_id")]),
    ("meta_history", "history_id", [("meta_station", "station_id")]),
    ("meta_vars", "vars_id", [("meta_network", "network_id")]),
)


def check_column(table, col_name, col_type=None, present=True):
    """Check that expected column is present in a table and of specified type"""
    if present:
        assert col_name in table.columns
        assert isinstance(table.columns[col_name].type, col_type)
    else:
        assert col_name not in table.columns


def check_triggers(table, expected, present=True):
    """Check that expected triggers are present on a table."""
    triggers = table.bind.execute(
        text(
            f"""
select trigger_name, array_agg(event_manipulation order by event_manipulation asc)
from information_schema.triggers
where trigger_schema = :schema_name
and event_object_schema = :schema_name
and event_object_table = :table_name
group by trigger_name
"""
        ),
        {"schema_name": table.schema, "table_name": table.name},
    ).fetchall()
    for item in expected:
        if present:
            assert item in triggers
        else:
            assert item not in triggers


tg_prefix = "t100_"


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration to a59d64cf16ca."""
    # TODO: Remove
    print("Testing in schema", schema_name)

    # Set up database to target version (a59d64cf16ca)
    engine, script = prepared_schema_from_migrations_left

    # Check that tables have been altered or created as expected.
    names = set(get_schema_item_names(engine, "tables", schema_name=schema_name))
    metadata = MetaData(schema=schema_name, bind=engine)
    for table_name, pri_key_name, foreign_keys in table_info:
        # Primary table: columns added
        pri_name = pri_table_name(table_name, qualify=False)
        assert pri_name in names
        pri_table = Table(pri_name, metadata, autoload_with=engine)
        check_column(pri_table, "mod_time", TIMESTAMP)
        check_column(pri_table, "mod_user", VARCHAR)

        # History table columns: primary plus additional columns
        hx_name = hx_table_name(table_name, qualify=False)
        assert hx_name in names
        hx_table = Table(hx_name, metadata, autoload_with=engine)
        for col in pri_table.columns:
            check_column(hx_table, col.name, col.type.__class__)
        check_column(hx_table, "deleted", BOOLEAN)
        check_column(hx_table, hx_id_name(table_name), INTEGER)
        for fk_table_name, fk_key_name in foreign_keys:
            check_column(hx_table, fk_key_name, INTEGER)
            check_column(hx_table, hx_id_name(fk_table_name), INTEGER)

        # Triggers
        check_triggers(
            pri_table,
            [
                (
                    f"{tg_prefix}primary_control_hx_cols",
                    "{DELETE,INSERT,UPDATE}",
                ),
                (
                    f"{tg_prefix}primary_ops_to_hx",
                    "{DELETE,INSERT,UPDATE}",
                ),
            ],
        )

        check_triggers(
            hx_table,
            [
                (
                    f"{tg_prefix}add_foreign_hx_keys",
                    "{INSERT}",
                )
            ],
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
    names = set(get_schema_item_names(engine, "tables", schema_name=schema_name))
    metadata = MetaData(schema=schema_name, bind=engine)
    for table_name, _, _ in table_info:
        # Primary table: columns dropped
        pri_name = pri_table_name(table_name, qualify=False)
        assert pri_name in names
        pri_table = Table(pri_name, metadata, autoload_with=engine)
        check_column(pri_table, "mod_time", present=False)
        check_column(pri_table, "mod_user", present=False)

        # History table: dropped
        hx_name = hx_table_name(table_name, qualify=False)
        assert hx_name not in names

        # Triggers (primary): dropped
        check_triggers(
            pri_table,
            [
                (
                    f"{tg_prefix}primary_control_hx_cols",
                    "{DELETE,INSERT,UPDATE}",
                ),
                (
                    f"{tg_prefix}primary_ops_to_hx",
                    "{DELETE,INSERT,UPDATE}",
                ),
            ],
            present=False,
        )
