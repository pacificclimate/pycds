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


matview_defns = {"collapsed_vars_mv": {"indexes": {"collapsed_vars_idx"}}}


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the upgrade schema migration."""

    # Set up database at 22819129a609 (this migration)
    alembic_runner.migrate_up_to("22819129a609")

    with alembic_engine.connect() as conn:
        # Matviews should be present, tables absent.
        check_matviews(conn, matview_defns, schema_name, matviews_present=True)


def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema down migration from 22819129a609 to bf366199f463."""

    # Set up database at this migration ready for downgrade
    alembic_runner.migrate_up_to("22819129a609")

    # Run downgrade migration to prev revision
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        # Matviews should absent after downgrade, tables present
        check_matviews(conn, matview_defns, schema_name, matviews_present=False)
