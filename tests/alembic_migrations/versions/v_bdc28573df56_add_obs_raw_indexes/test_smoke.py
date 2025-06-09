"""Smoke tests:
- Upgrade adds indexes
- Downgrade drops indexes
"""

# -*- coding: utf-8 -*-
import logging
import pytest
import pycds.database
from pycds.database import get_schema_item_names


logger = logging.getLogger("tests")

index_names = {
    "mod_time_idx",
    "obs_raw_comp_idx",
    "obs_raw_history_id_idx",
    "obs_raw_id_idx",
}


@pytest.mark.update20
@pytest.mark.parametrize("item_names", [set(), {"alpha", "beta"}])
def test_mock(mocker, item_names):
    mocker.patch("pycds.database.get_schema_item_names", return_value=item_names)
    assert pycds.database.get_schema_item_names() == item_names


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 7a3b247c577b to bdc28573df56."""
    alembic_runner.migrate_up_before("bdc28573df56")

    with alembic_engine.begin() as conn:
        # Check that indexes have been added
        before_upgrade_index_names = get_schema_item_names(
            conn, "indexes", table_name="obs_raw", schema_name=schema_name
        )
    assert not (index_names <= set() | before_upgrade_index_names)

    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as conn:
        after_upgrade_index_names = get_schema_item_names(
            conn, "indexes", table_name="obs_raw", schema_name=schema_name
        )

    assert index_names <= before_upgrade_index_names | after_upgrade_index_names


@pytest.mark.update20
def test_downgrade(
    alembic_engine,
    alembic_runner,
    schema_name,
):
    """
    Test the schema migration from bdc28573df56 to 7a3b247c577b.
    """

    alembic_runner.migrate_up_before("bdc28573df56")

    with alembic_engine.begin() as conn:
        before_upgrade_index_names = get_schema_item_names(
            conn, "indexes", table_name="obs_raw", schema_name=schema_name
        )

    assert not (index_names <= before_upgrade_index_names)

    alembic_runner.migrate_up_one()  # Upgrade to bdc28573df56, indexes added

    with alembic_engine.begin() as conn:
        after_upgrade_index_names = get_schema_item_names(
            conn, "indexes", table_name="obs_raw", schema_name=schema_name
        )

    assert index_names <= after_upgrade_index_names

    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as conn:
        after_downgrade_index_names = get_schema_item_names(
            conn, "indexes", table_name="obs_raw", schema_name=schema_name
        )

    assert before_upgrade_index_names == after_downgrade_index_names
