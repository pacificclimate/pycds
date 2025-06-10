# -*- coding: utf-8 -*-
import logging

import pytest

from alembic import command
from sqlalchemy import text

from sqlalchemydiff import compare

from tests.conftest import db_setup

from .sqlalchemydiff_util import prepare_schema_from_models

from pycds import Base


logger = logging.getLogger("tests")


def test_upgrade_and_downgrade(alembic_runner):
    """Test all migrations up and down.

    Tests that we can apply all migrations from a brand new empty
    database, and also that we can remove them all.
    """
    alembic_runner.migrate_up_to("head")

    assert len(alembic_runner.heads) == 1
    head = alembic_runner.heads[0]
    current = alembic_runner.current

    assert head == current

    while current != "base":
        alembic_runner.migrate_down_one()
        current = alembic_runner.current


@pytest.mark.skip(reason="utility; not really a test")
@pytest.mark.usefixtures("new_db_left")
def test_indexes(alembic_engine, alembic_runner):
    alembic_runner.migrate_up_to("head")

    with alembic_engine.connect() as conn:
        indexes = conn.execute(
            text(
                """
            select indexname, indexdef 
            from pg_indexes 
            where schemaname ='crmp' 
            order by indexname
            """
            )
        )
        print(f"### indexes")
        for index in indexes:
            print(index[0], index[1])


@pytest.mark.usefixtures("new_db_right")
def test_model_and_migration_schemas_are_the_same(
    uri_right, alembic_engine, alembic_runner, schema_name
):
    """Compare two databases.

    Compares the database obtained with all migrations against the
    one we get out of the models.
    """
    alembic_runner.migrate_up_to("head")
    prepare_schema_from_models(uri_right, Base, schema_name, db_setup=db_setup)

    result = compare(alembic_engine.url, uri_right)

    assert result.is_match
