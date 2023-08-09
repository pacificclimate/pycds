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
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


matview_defns = {"climo_obs_count_mv": {"indexes": {"climo_obs_count_idx"}}}


@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", [True, False], indirect=True
)
@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the upgrade schema migration."""

    # Set up database at latest revision
    engine, script = prepared_schema_from_migrations_left

    # Matviews should be present, tables absent.
    check_matviews(engine, matview_defns, schema_name, matviews_present=True)


@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", [True, False], indirect=True
)
@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 7a3b247c577b to 84b7fc2596d5."""

    # Set up database at latest revision
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration to prev revision
    command.downgrade(alembic_config_left, "-1")

    # Matviews should absent after downgrade, tables present
    check_matviews(engine, matview_defns, schema_name, matviews_present=False)
