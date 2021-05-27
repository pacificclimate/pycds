"""Smoke tests:
- Upgrade
- Downgrade
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from sqlalchemy import inspect
from sqlalchemy.schema import DropTable
from alembic import command
from pycds import ClimatologyAttributes


logger = logging.getLogger("tests")
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", ("e688e520d265",), indirect=True
)
@pytest.mark.parametrize("table_exists", [False, True])
def test_upgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    schema_name,
    table_exists,
):
    """Test the schema migration from e688e520d265 to 2914c6c8a7f9."""

    # Set up database to revision e688e520d265
    engine, script = prepared_schema_from_migrations_left

    # Exercise both cases of "if not exists"
    if not table_exists:
        engine.execute(DropTable(ClimatologyAttributes.__table__))

    # Upgrade to 0d99ba90c229
    command.upgrade(alembic_config_left, "2914c6c8a7f9")

    # Check that table has been dropped
    assert ClimatologyAttributes.__tablename__ not in inspect(
        engine
    ).get_table_names(schema=schema_name)


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 2914c6c8a7f9 to e688e520d265."""

    # Set up database to revision 2914c6c8a7f9
    engine, script = prepared_schema_from_migrations_left

    # Downgrade to revision e688e520d265
    command.downgrade(alembic_config_left, "-1")

    # Check that table has been added
    assert ClimatologyAttributes.__tablename__ in inspect(
        engine
    ).get_table_names(schema=schema_name)
