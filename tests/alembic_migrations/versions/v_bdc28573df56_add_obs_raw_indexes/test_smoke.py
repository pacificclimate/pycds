"""Smoke tests:
- Upgrade adds indexes
- Downgrade drops indexes
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
import pycds.database
from pycds.database import get_schema_item_names


logger = logging.getLogger("tests")

index_names = {
    "mod_time_idx",
    "obs_raw_comp_idx",
    "obs_raw_history_id_idx",
    "obs_raw_id_idx",
}


@pytest.mark.parametrize("item_names", [set(), {"alpha", "beta"}])
def test_mock(mocker, item_names):
    mocker.patch(
        "pycds.database.get_schema_item_names", return_value=item_names
    )
    assert pycds.database.get_schema_item_names() == item_names


@pytest.mark.parametrize(
    # We need the values of the mocked prior index names for the test
    # assertions. However, it's not convenient to get it in advance of running
    # the migration. Therefore we supply it direct. Inelegant but workable.
    "prepared_schema_from_migrations_left, prior_index_names",
    [(set(),) * 2, (index_names,) * 2],
    indirect=["prepared_schema_from_migrations_left"],
)
@pytest.mark.usefixtures("new_db_left")
def test_upgrade(
    prepared_schema_from_migrations_left, prior_index_names, schema_name
):
    """Test the schema migration from 7a3b247c577b to bdc28573df56. """

    # Set up database to revision bdc28573df56
    engine, script = prepared_schema_from_migrations_left

    # Check that indexes have been added
    after_upgrade_index_names = get_schema_item_names(
        engine, "indexes", table_name="obs_raw", schema_name=schema_name
    )
    assert index_names <= prior_index_names | after_upgrade_index_names


@pytest.mark.usefixtures("prepared_schema_from_migrations_left")
@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize("prior_index_names", [set(), index_names])
def test_downgrade(
    prepared_schema_from_migrations_left,
    prior_index_names,
    alembic_config_left,
    schema_name,
    mocker,
):
    """
    Test the schema migration from bdc28573df56 to 7a3b247c577b.

    If we mock the new indexes as already existing before schema is prepared
    (i.e., before the upgrade happens), then the upgrade operation won't
    add them, and they won't actually exist in the database because they are
    only mocked to pre-exist. The downgrade operation will then try to remove
    these existing indexes, but fail because they were never actually there.

    Solution: Don't mock the pre-existing indexes until *after* the upgrade
    has added them. Then check whether, depending on what was mocked, they
    have been removed or left in place. This is the obverse of the reason
    that fixture can pre-mock the existing indexes.
    """

    # Set up database to revision bdc28573df56
    engine, script = prepared_schema_from_migrations_left

    # Tell the downgrade operation what we want it to think.
    patcher = mocker.patch(
        "pycds.database.get_schema_item_names",
        return_value=prior_index_names,
    )

    # Run downgrade migration to revision 7a3b247c577b
    command.downgrade(alembic_config_left, "-1")

    # Stop mocking; now we can see what's actually in the database.
    patcher.stop()

    # Check that indexes have been removed according to mocked values
    after_downgrade_index_names = get_schema_item_names(
        engine, "indexes", table_name="obs_raw", schema_name=schema_name
    )
    # This could be expressed as
    # index_names <= prior_index_names.symmetric_difference(after_downgrade_index_names)
    # but the following is clearer:
    for name in index_names:
        assert (
            name in prior_index_names
            and name not in after_downgrade_index_names
        ) or (
            name not in prior_index_names
            and name in after_downgrade_index_names
        )
