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


@pytest.mark.update20
@pytest.mark.parametrize("supports_matviews", [True, False])
def test_mock(mocker, supports_matviews):
    mocker.patch("pycds.database.db_supports_matviews", return_value=supports_matviews)
    assert pycds.database.db_supports_matviews() is supports_matviews


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 84b7fc2596d5 to 7a3b247c577b."""
    alembic_runner.migrate_up_to("7a3b247c577b")

    # Matviews present if and only if supported by database.
    check_matviews(
        alembic_engine,
        matview_defns,
        schema_name,
        pycds.database.db_supports_matviews(alembic_engine),
    )


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 7a3b247c577b to 84b7fc2596d5."""

    alembic_runner.migrate_up_to("7a3b247c577b")
    alembic_runner.migrate_down_one()

    # Matviews are always absent after downgrade
    check_matviews(alembic_engine, matview_defns, schema_name, False)
