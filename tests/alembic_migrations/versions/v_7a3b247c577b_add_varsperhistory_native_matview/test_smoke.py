"""Smoke tests:
- Upgrade drops table and creates matview
- Downgrade drops matview and creates table
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command

import pycds.database
from .. import check_matviews


logger = logging.getLogger("tests")


matview_defns = {"vars_per_history_mv": {"indexes": {"var_hist_idx"}}}


@pytest.mark.parametrize("supports_matviews", [True, False])
def test_mock(mocker, supports_matviews):
    mocker.patch("pycds.database.db_supports_matviews", return_value=supports_matviews)
    assert pycds.database.db_supports_matviews() is supports_matviews


@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", [True, False], indirect=True
)
@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration from 84b7fc2596d5 to 7a3b247c577b."""

    # Set up database to revision 7a3b247c577b
    engine, script = prepared_schema_from_migrations_left

    # Matviews present if and only if supported by database.
    check_matviews(
        engine, matview_defns, schema_name, pycds.database.db_supports_matviews(engine)
    )


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
    check_matviews(engine, matview_defns, schema_name, False)
