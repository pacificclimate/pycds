"""Smoke tests:
- Upgrade adds check constraint to meta_vars.net_var_name such that they are valid sql identifiers 
    (this boils down to values inserted into the column should not contain whitespace)
- Check constraint can still enter valid data
- Check constraint rejects bad data after migration
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import CheckViolation

logger = logging.getLogger("tests")

# this is the revision *before* the migration is to be run.
prior_revision = "bb2a222a1d4a"
final_revision = "78260d36e42b"


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", (prior_revision,), indirect=True
)
def test_upgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    schema_name,
):
    """test migration from bb2a222a1d4a to 78260d36e42b."""

    # Set up database to version bb2a222a1d4a (previous migration)
    engine, script = prepared_schema_from_migrations_left

    engine.execute(
        f"INSERT INTO {schema_name}.meta_vars(vars_id, net_var_name, standard_name, cell_method, display_name)"
        f"VALUES (1, 'bad var name with\nwhitespace', 'test', 'test', 'test')"
    )

    command.upgrade(alembic_config_left, "+1")

    result = engine.execute(
        f"SELECT vars_id, net_var_name FROM {schema_name}.meta_vars"
    )

    row = next(result)

    # After migration is run, bad strings should have whitespace replaced with underscores
    assert row == (1, "bad_var_name_with_whitespace")


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", (final_revision,), indirect=True
)
@pytest.mark.parametrize(
    "test_value", ["allow_underscores", "allowfullwords", "allowCapitals"]
)
def test_check_good_constraint_values(
    prepared_schema_from_migrations_left, schema_name, test_value
):
    # Set up database to version 78260d36e42b (after migration)
    engine, script = prepared_schema_from_migrations_left

    engine.execute(
        f"INSERT INTO {schema_name}.meta_vars(vars_id, net_var_name, standard_name, cell_method, display_name)"
        f"VALUES (1, '{test_value}' , 'test', 'test', 'test')"
    )

    result = engine.execute(
        f"SELECT vars_id, net_var_name FROM {schema_name}.meta_vars"
    )

    row = next(result)

    assert row == (1, test_value)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", (final_revision,), indirect=True
)
@pytest.mark.parametrize("test_value", ["bad string", "bad\nnewline"])
def test_check_bad_constraint_values(
    prepared_schema_from_migrations_left, schema_name, test_value
):
    # Set up database to version 78260d36e42b (after migration)
    engine, script = prepared_schema_from_migrations_left
    # This test passes bad data and expects an integrity error back from SQLAlchemy when executed
    with pytest.raises(IntegrityError) as excinfo:
        engine.execute(
            f"INSERT INTO {schema_name}.meta_vars(vars_id, net_var_name, standard_name, cell_method, display_name)"
            f"VALUES (1, '{test_value}' , 'test', 'test', 'test')"
        )

    # The specific exception raised by psycopg2 is stored internally in SQLAlchemy's IntegrityError
    # By checking this inner exception we can know that it is specifically a Check Constraint violation
    assert isinstance(excinfo.value.orig, CheckViolation)
