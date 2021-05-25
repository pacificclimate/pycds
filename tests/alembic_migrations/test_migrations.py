# -*- coding: utf-8 -*-
import logging

import pytest

from alembic import command

from sqlalchemydiff import compare

from .sqlalchemydiff_util import prepare_schema_from_models

from .alembicverify_util import (
    get_current_revision,
    get_head_revision,
    prepare_schema_from_migrations,
)

from pycds import Base


logger = logging.getLogger("tests")


@pytest.mark.usefixtures("new_db_left")
def test_upgrade_and_downgrade(
    uri_left, alembic_config_left, db_setup, env_config
):
    """Test all migrations up and down.

    Tests that we can apply all migrations from a brand new empty
    database, and also that we can remove them all.
    """
    engine, script = prepare_schema_from_migrations(
        uri_left, alembic_config_left, db_setup=db_setup
    )

    head = get_head_revision(alembic_config_left, engine, script)
    current = get_current_revision(
        alembic_config_left, engine, script, env_config=env_config
    )

    assert head == current

    while current is not None:
        command.downgrade(alembic_config_left, "-1")
        current = get_current_revision(alembic_config_left, engine, script)


@pytest.mark.skip(reason="utility; not really a test")
@pytest.mark.usefixtures("new_db_left")
def test_indexes(uri_left, alembic_config_left, db_setup, env_config):
    engine, script = prepare_schema_from_migrations(
        uri_left, alembic_config_left, db_setup=db_setup
    )

    indexes = engine.execute(
        """
        select indexname, indexdef 
        from pg_indexes 
        where schemaname ='crmp' 
        order by indexname
        """
    )
    print(f"### indexes")
    for index in indexes:
        print(index[0], index[1])


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.usefixtures("new_db_right")
def test_model_and_migration_schemas_are_the_same(
    uri_left, uri_right, alembic_config_left, db_setup
):
    """Compare two databases.

    Compares the database obtained with all migrations against the
    one we get out of the models.
    """
    prepare_schema_from_migrations(
        uri_left, alembic_config_left, db_setup=db_setup
    )
    prepare_schema_from_models(uri_right, Base, db_setup=db_setup)

    result = compare(uri_left, uri_right)

    assert result.is_match
