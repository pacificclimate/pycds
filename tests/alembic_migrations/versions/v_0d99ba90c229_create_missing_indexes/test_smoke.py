"""Smoke tests:
- Upgrade
- Downgrade
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from sqlalchemy import inspect
from alembic import command
from sqlalchemy.schema import CreateIndex

from pycds import History, Station, Variable


logger = logging.getLogger("tests")
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

table_and_index = (
    ("climo_obs_count_mv", "climo_obs_count_idx"),
    ("collapsed_vars_mv", "collapsed_vars_idx"),
    ("meta_history", "fki_meta_history_station_id_fk"),
    ("meta_station", "fki_meta_station_network_id_fkey"),
    ("meta_vars", "fki_meta_vars_network_id_fkey"),
    ("meta_history", "meta_history_freq_idx"),
    ("obs_count_per_month_history_mv", "obs_count_per_month_history_idx"),
    ("station_obs_stats_mv", "station_obs_stats_mv_idx"),
)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", ("2914c6c8a7f9",), indirect=True
)
@pytest.mark.parametrize("pre_create", [tuple(), (History, Station, Variable)])
def test_upgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name, pre_create,
):
    """Test the schema migration from 2914c6c8a7f9 to 0d99ba90c229."""

    # Set up database to revision 2914c6c8a7f9
    engine, script = prepared_schema_from_migrations_left

    # Pre-create some indexes to exercise "if not exists"
    for ORMClass in pre_create:
        for index in ORMClass.__table__.indexes:
            engine.execute(CreateIndex(index))

    # Upgrade to 0d99ba90c229
    command.upgrade(alembic_config_left, "0d99ba90c229")

    # Check that all indexes have been added
    for table_name, index_name in table_and_index:
        assert index_name in {
            index["name"]
            for index in inspect(engine).get_indexes(
                table_name=table_name, schema=schema_name
            )
        }


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 0d99ba90c229 to 2914c6c8a7f9."""

    # Set up database to revision 0d99ba90c229
    engine, script = prepared_schema_from_migrations_left

    # Downgrade to revision 2914c6c8a7f9
    command.downgrade(alembic_config_left, "-1")

    # Check that indexes have been removed
    for table_name, index_name in table_and_index:
        assert index_name not in {
            index["name"]
            for index in inspect(engine).get_indexes(
                table_name=table_name, schema=schema_name
            )
        }
