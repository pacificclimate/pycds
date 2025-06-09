import pytest
from alembic import command

from ..alembicverify_util import prepare_schema_from_migrations
from pycds.alembic.info import get_current_head
from pycds.database import check_migration_version


@pytest.mark.update20
def test_get_current_head():
    assert get_current_head() == "8c05da87cb79"


@pytest.mark.update20
def test_check_migration_version(alembic_engine, alembic_runner):
    """Test that `check_migration_version` passes on the latest migration
    and raises an exception on other versions.

    If this test fails, you must update `check_migration_version` with the latest
    migration version.
    """
    alembic_runner.migrate_up_to(get_current_head())

    # Test against the most up to date migration in the database;
    # this should pass, i.e., not raise an exception.
    check_migration_version(alembic_engine)

    # Back off to the previous migration.
    alembic_runner.migrate_down_one()

    # Now the checker should raise an exception.
    with pytest.raises(ValueError):
        check_migration_version(alembic_engine)
