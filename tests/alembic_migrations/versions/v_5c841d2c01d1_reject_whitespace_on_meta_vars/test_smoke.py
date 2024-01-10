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
down_revision = "6cb393f711c3"
target_revision = "5c841d2c01d1"

columns_to_apply = [
    "unit",
    "standard_name",
    "cell_method",
    "display_name",
    "short_name",
    "long_description",
]


# This higher order function turns a test value into a column prefixed value optionally quoted with single quotes for SQL statements
def column_value(value, quoted=True):
    def wrap_sql_string(column_name):
        prefixed = f"{column_name}_{value}"
        return f"'{prefixed}'" if quoted else prefixed

    return wrap_sql_string


def get_insert_statement(schema_name, insertion_value, vars_id=1):
    return (
        f"INSERT INTO {schema_name}.meta_vars(vars_id, {', '.join(columns_to_apply)})\n"
        f"VALUES ({vars_id}, {',  '.join(list(map(column_value(insertion_value), columns_to_apply)))});\n"
    )


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", (down_revision,), indirect=True
)
def test_upgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    schema_name,
):
    """test migration from fecff1a73d7e to 5c841d2c01d1."""

    # Set up database to version fecff1a73d7e (down migration)
    engine, script = prepared_schema_from_migrations_left

    # (insertion_id, insertion_value, expected_value)
    test_values = [
        (1, "bad\rfeed", "bad feed"),
        (2, "bad\nnewline", "bad newline"),
        (3, "bad\r\nreturn", "bad return"),
        (4, "bad\rmix\nof\r\nall", "bad mix of all"),
    ]

    def inserter(x):
        return get_insert_statement(schema_name, x[1], x[0])

    statement = "\n".join(list(map(inserter, test_values)))
    engine.execute(statement)

    command.upgrade(alembic_config_left, "+1")

    result = engine.execute(
        f"SELECT vars_id, {', '.join(columns_to_apply)} FROM {schema_name}.meta_vars ORDER BY vars_id"
    )

    for index, row in enumerate(result):
        test_sample = test_values[index]
        assert row == (
            test_sample[0],
            # Spread our tested columns test values to create a tuple, each is made unique by prefixing the
            # column name so we shouldn't get any false positive tests due to column mis-ordering
            *list(map(column_value(test_sample[2], quoted=False), columns_to_apply)),
        )


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", ((target_revision),), indirect=True
)
def test_downgrade(
    prepared_schema_from_migrations_left,
    alembic_config_left,
    schema_name,
):
    """test migration from 5c841d2c01d1 to fecff1a73d7e."""

    # Set up database to version 5c841d2c01d1 (target migration)
    engine, script = prepared_schema_from_migrations_left

    statement = get_insert_statement(
        schema_name, "good value that does not contain newlines"
    )
    engine.execute(statement)

    command.downgrade(alembic_config_left, "-1")

    result = engine.execute(
        f"SELECT vars_id, {', '.join(columns_to_apply)} FROM {schema_name}.meta_vars"
    )

    row = next(result)

    # After migration is reversed, data should be unchanged
    assert row == (
        1,
        *list(
            map(
                column_value("good value that does not contain newlines", quoted=False),
                columns_to_apply,
            )
        ),
    )


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", (target_revision,), indirect=True
)
# good test values should contain anything but newline characters, this can be a pretty broad sample
@pytest.mark.parametrize(
    "test_value",
    ["allow_underscores", "allowfullwords", "allowCapitals", "allowNumbers123"],
)
def test_check_good_constraint_values(
    prepared_schema_from_migrations_left, schema_name, test_value
):
    # Set up database to version 5c841d2c01d1 (target migration)
    engine, script = prepared_schema_from_migrations_left

    statement = get_insert_statement(schema_name, test_value)
    engine.execute(statement)

    result = engine.execute(
        f"SELECT vars_id, {', '.join(columns_to_apply)} FROM {schema_name}.meta_vars"
    )

    row = next(result)

    assert row == (
        1,
        *list(map(column_value(test_value, quoted=False), columns_to_apply)),
    )


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left", (target_revision,), indirect=True
)
# bad test values will cause the constraint check to error with one or more newline type characters.
@pytest.mark.parametrize(
    "test_value",
    ["bad\rreturn", "bad\nfeed", "bad\r\nnewline", "bad\rmix\nof\r\nall"],
)
def test_check_bad_constraint_values(
    prepared_schema_from_migrations_left, schema_name, test_value
):
    # Set up database to version 5c841d2c01d1 (target migration)
    engine, script = prepared_schema_from_migrations_left
    # This test passes bad data and expects an integrity error back from SQLAlchemy when executed
    with pytest.raises(IntegrityError) as excinfo:
        statement = get_insert_statement(schema_name, test_value)
        engine.execute(statement)

    # The specific exception raised by psycopg2 is stored internally in SQLAlchemy's IntegrityError
    # By checking this inner exception we can know that it is specifically a Check Constraint violation
    assert isinstance(excinfo.value.orig, CheckViolation)
