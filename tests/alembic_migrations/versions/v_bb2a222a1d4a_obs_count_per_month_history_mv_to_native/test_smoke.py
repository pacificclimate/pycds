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


matview_defns = {
    "obs_count_per_month_history_mv": {"indexes": {"obs_count_per_month_history_idx"}}
}


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the upgrade schema migration."""

    # Set up database at bb2a222a1d4a (this migration)
    alembic_runner.migrate_up_to("bb2a222a1d4a")

    with alembic_engine.connect() as conn:
        # Matviews should be present, tables absent.
        check_matviews(conn, matview_defns, schema_name, matviews_present=True)


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 7a3b247c577b to 84b7fc2596d5."""

    # Set up database at bb2a222a1d4a (this migration)
    alembic_runner.migrate_up_to("bb2a222a1d4a")

    # Run downgrade migration to prev revision
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        # Matviews should absent after downgrade, tables present
        check_matviews(conn, matview_defns, schema_name, matviews_present=False)
