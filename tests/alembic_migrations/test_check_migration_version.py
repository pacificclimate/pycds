import pytest
from alembic import command

from .alembicverify_util import prepare_schema_from_migrations
from pycds.alembic.info import get_current_head
from pycds.database import check_migration_version


def test_get_current_head():
    assert get_current_head() == "0d99ba90c229"


@pytest.mark.usefixtures("new_db_left")
def test_check_migration_version(
    uri_left, alembic_config_left, db_setup, env_config
):
    """Test that `check_migration_version` passes on the latest migration
    and raises an exception on other versions.

    If this test fails, you must update `check_migration_version` with the latest
    migration version.
    """
    engine, script = prepare_schema_from_migrations(
        uri_left, alembic_config_left, db_setup=db_setup
    )

    # Test against the most up to date migration in the database;
    # this should pass, i.e., not raise an exception.
    check_migration_version(engine)

    # Back off to the previous migration.
    command.downgrade(alembic_config_left, "-1")

    # Now the checker should raise an exception.
    with pytest.raises(ValueError):
        check_migration_version(engine)
