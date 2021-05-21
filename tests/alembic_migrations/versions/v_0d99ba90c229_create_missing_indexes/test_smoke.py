"""Smoke tests:
- Upgrade
- Downgrade
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from pycds.database import get_schema_item_names
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
    ("meta_climo_attrs", "meta_climo_attrs_idx"),
    ("meta_history", "meta_history_freq_idx"),
    ("obs_count_per_month_history_mv", "obs_count_per_month_history_idx"),
    ("station_obs_stats_mv", "station_obs_stats_mv_idx"),
)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", ("e688e520d265",), indirect=True
)
@pytest.mark.parametrize("pre_create", [tuple(), (History, Station, Variable)])
def test_upgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    schema_name,
    pre_create,
):
    """Test the schema migration from e688e520d265 to 0d99ba90c229."""

    # Set up database to revision e688e520d265
    engine, script = prepared_schema_from_migrations_left

    # Pre-create some indexes to exercise "if not exists"
    for ORMClass in pre_create:
        for index in ORMClass.__table__.indexes:
            engine.execute(CreateIndex(index))

    # Upgrade to 0d99ba90c229
    command.upgrade(alembic_config_left, "0d99ba90c229")

    # Check that all indexes have been added
    for table_name, index_name in table_and_index:
        assert index_name in get_schema_item_names(
            engine, "indexes", table_name=table_name, schema_name=schema_name
        )


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", ("0d99ba90c229",), indirect=True
)
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 0d99ba90c229 to e688e520d265."""

    # Set up database to revision 0d99ba90c229
    engine, script = prepared_schema_from_migrations_left

    # Downgrade to revision e688e520d265
    command.downgrade(alembic_config_left, "-1")

    # Check that indexes have been removed
    for table_name, index_name in table_and_index:
        assert index_name not in get_schema_item_names(
            engine, "indexes", table_name=table_name, schema_name=schema_name
        )
