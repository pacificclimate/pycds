"""Smoke tests:
- Upgrade drops table and creates matview
- Downgrade drops matview and creates table
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command

import pycds.database
from pycds.database import get_schema_item_names


logger = logging.getLogger("tests")
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


matviews = {"climo_obs_count_mv": {"indexes": {"climo_obs_count_idx"}}}
matview_names = set(matviews)


def check_matviews(engine, schema_name, present):
    """Check whether matviews are present or absent."""
    if present:
        # Check that table has been replaced with matview
        names = get_schema_item_names(engine, "tables", schema_name=schema_name)
        assert names & matview_names == set(), "matview(s) should not be present as table(s)"
        names = get_schema_item_names(engine, "matviews", schema_name=schema_name)
        assert names >= matview_names, "matview(s) should be present as matviews"

        # Check that indexes were installed too
        for table_name, contents in matviews.items():
            names = get_schema_item_names(
                engine,
                "indexes",
                table_name=table_name,
                schema_name=schema_name,
            )
            assert names == contents["indexes"], "matview indexes should be installed"
    else:
        # Check that matview is not present and table is
        names = get_schema_item_names(engine, "matviews", schema_name=schema_name)
        assert names & matview_names == set(), "matview(s) should not be present as matviews"
        names = get_schema_item_names(engine, "tables", schema_name=schema_name)
        assert names >= matview_names, "matview(s) should be present as table(s)"

        # Check that matview indexes are not present
        for table_name, contents in matviews.items():
            names = get_schema_item_names(
                engine,
                "indexes",
                table_name=table_name,
                schema_name=schema_name,
            )
            assert names & contents["indexes"] == set(), "table indexes should not be present"


@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", [True, False], indirect=True
)
@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration from 84b7fc2596d5 to 7a3b247c577b."""

    # Set up database to revision 7a3b247c577b
    engine, script = prepared_schema_from_migrations_left

    # Matviews present if and only if supported by database.
    # check_matviews(engine, schema_name, pycds.database.db_supports_matviews(engine))
    check_matviews(engine, schema_name, True)


@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", [True, False], indirect=True
)
@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 7a3b247c577b to 84b7fc2596d5."""

    # Set up database to revision 7a3b247c577b
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration to revision
    command.downgrade(alembic_config_left, "-1")

    # Matviews are always absent after downgrade
    check_matviews(engine, schema_name, False)
