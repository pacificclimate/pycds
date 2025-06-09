"""Smoke tests:
- Upgrade drops table and creates matview
- Downgrade drops matview and creates table
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command

from .. import check_matviews


logger = logging.getLogger("tests")
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


matview_defns = {"climo_obs_count_mv": {"indexes": {"climo_obs_count_idx"}}}


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the upgrade schema migration."""

    # Set up database at 96729d6db8b3 (this migration)
    alembic_runner.migrate_up_to("96729d6db8b3")

    with alembic_engine.begin() as conn:
        # Matviews should be present, tables absent.
        check_matviews(conn, matview_defns, schema_name, matviews_present=True)


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 7a3b247c577b to 84b7fc2596d5."""

    # Set up database at 96729d6db8b3 (this migration)
    alembic_runner.migrate_up_to("96729d6db8b3")

    # Run downgrade migration to prev revision
    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as conn:
        # Matviews should absent after downgrade, tables present
        check_matviews(conn, matview_defns, schema_name, matviews_present=False)
