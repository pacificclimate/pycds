"""Test that `check_migration_version` passes on the latest migration
and raises an exception on other versions.

If this test fails, you must update `check_migration_version` with the latest
migration version.
"""
import pytest
from alembic import command
from .alembicverify_util import prepare_schema_from_migrations
from pycds.util import check_migration_version


@pytest.mark.usefixtures('new_db_left')
def test_check_migration_version(
        uri_left, alembic_config_left, db_setup, env_config,
):
    engine, script = prepare_schema_from_migrations(
        uri_left, alembic_config_left, db_setup=db_setup
    )
    check_migration_version(engine)

    command.downgrade(alembic_config_left, '-1')

    with pytest.raises(ValueError):
        check_migration_version(engine)
