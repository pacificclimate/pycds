"""Smoke tests:
- Upgrade adds not null constraint
- Downgrade drops not null constraint
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import inspect, text

logger = logging.getLogger("tests")

@pytest.mark.parametrize(
        "prepared_schema_from_migrations_left", ("3d50ec832e47",),indirect=True
)
@pytest.mark.usefixtures("new_db_left")
def test_upgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """test migration from 3d50ec832e47 to 7b139906ac46. """
    
    # Set up database to version 7b139906ac46
    engine, script = prepared_schema_from_migrations_left

    engine.execute(
        f"INSERT INTO {schema_name}.meta_vars(vars_id)"
        f"VALUES (1)"
    )

    command.upgrade(alembic_config_left, "+1")

    # Check that data has been modified to remove nulls
    result = engine.execute(
        f"SELECT * FROM {schema_name}.meta_vars"
    )
    for row in result:
        if row.vars_id == 1:
            assert row.cell_method == "foo: bar"
            assert row.standard_name == "foo_bar"
            assert row.display_name == "foo bar"

    # Check that not null constraints have been added
    table = inspect(engine).get_columns("meta_vars", schema=schema_name)

    for col in table:
        if col["name"]=="cell_method" or col["name"]=="standard_name" or col["name"]=="display_name":
            assert(col["nullable"]==False)


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 7b139906ac46 to 3d50ec832e47. """

    # Set up database to version 7b139906ac46
    engine, script = prepared_schema_from_migrations_left

    engine.execute(
        f"INSERT INTO {schema_name}.meta_vars(vars_id, cell_method, standard_name, display_name)"
        f"VALUES (10000, 'foo: bar', 'foo_bar', 'foo bar')"
    )

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # Check that data has been modified to insert nulls back in

    result = engine.execute(
        f"SELECT * FROM {schema_name}.meta_vars"
    )
    for row in result:
        if row.vars_id == 10000:
            assert row.cell_method == None
            assert row.standard_name == None
            assert row.display_name == None

    # Check that not null constraints have been removed
    table = inspect(engine).get_columns("meta_vars", schema=schema_name)

    for col in table:
        if col["name"]=="cell_method" or col["name"]=="standard_name" or col["name"]=="display_name":
            assert(col["nullable"]==True)
