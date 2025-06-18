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
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import CheckViolation

logger = logging.getLogger("tests")

# this is the revision *before* the migration is to be run.
down_revision = "78260d36e42b"
target_revision = "5c841d2c01d1"

columns_to_test = [
    "unit",
    "standard_name",
    "cell_method",
    "display_name",
    "short_name",
    "long_description",
]


def column_value(value, quoted=True):
    """
    This higher order function turns a test value into a column prefixed value optionally quoted
    with single quotes for SQL statements. Prefixing the values lets us make sure we aren't
    getting false positives by accidentally looking at the wrong column when looping over values later.
    """

    def wrap_sql_string(column_name):
        prefixed = f"{column_name}_{value}"
        return f"'{prefixed}'" if quoted else prefixed

    return wrap_sql_string


def get_insert_statement(schema_name, insertion_value, vars_id=1):
    return (
        f"INSERT INTO {schema_name}.meta_vars(vars_id, {', '.join(columns_to_test)})\n"
        f"VALUES ({vars_id}, {',  '.join(list(map(column_value(insertion_value), columns_to_test)))});\n"
    )


@pytest.mark.update20
def test_upgrade(
    alembic_engine,
    alembic_runner,
    schema_name,
):
    """test migration from fecff1a73d7e to 5c841d2c01d1."""

    # Set up database to version fecff1a73d7e (down migration)
    alembic_runner.migrate_up_before("5c841d2c01d1")

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

    with alembic_engine.begin() as conn:
        conn.execute(text(statement))

    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as conn:
        result = conn.execute(
            text(
                f"SELECT vars_id, {', '.join(columns_to_test)} FROM {schema_name}.meta_vars ORDER BY vars_id"
            )
        )

    for row, test_sample in zip(result, test_values):
        assert row == (
            test_sample[0],
            *list(map(column_value(test_sample[2], quoted=False), columns_to_test)),
        )


@pytest.mark.update20
def test_downgrade(
    alembic_engine,
    alembic_runner,
    schema_name,
):
    """test migration from 5c841d2c01d1 to fecff1a73d7e."""

    # Set up database to version 5c841d2c01d1 (target migration)
    alembic_runner.migrate_up_to("5c841d2c01d1")

    with alembic_engine.begin() as conn:
        statement = get_insert_statement(
            schema_name, "good value that does not contain newlines"
        )
        conn.execute(text(statement))

    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as conn:
        result = conn.execute(
            text(
                f"SELECT vars_id, {', '.join(columns_to_test)} FROM {schema_name}.meta_vars"
            )
        )

    row = next(result)

    # After migration is reversed, data should be unchanged
    assert row == (
        1,
        *list(
            map(
                column_value("good value that does not contain newlines", quoted=False),
                columns_to_test,
            )
        ),
    )


# good test values should contain anything but newline characters, this can be a pretty broad sample
@pytest.mark.parametrize(
    "test_value",
    ["allow_underscores", "allowfullwords", "allowCapitals", "allowNumbers123"],
)
@pytest.mark.update20
def test_check_good_constraint_values(
    alembic_engine, alembic_runner, schema_name, test_value
):
    # Set up database to version 5c841d2c01d1 (target migration)
    alembic_runner.migrate_up_to("5c841d2c01d1")

    with alembic_engine.begin() as conn:
        conn.execute(text(get_insert_statement(schema_name, test_value)))

        result = conn.execute(
            text(
                f"SELECT vars_id, {', '.join(columns_to_test)} FROM {schema_name}.meta_vars"
            )
        )

    row = next(result)

    assert row == (
        1,
        *list(map(column_value(test_value, quoted=False), columns_to_test)),
    )


# bad test values will cause the constraint check to error with one or more newline type characters.
@pytest.mark.parametrize(
    "test_value",
    ["bad\rreturn", "bad\nfeed", "bad\r\nnewline", "bad\rmix\nof\r\nall"],
)
@pytest.mark.update20
def test_check_bad_constraint_values(
    alembic_engine, alembic_runner, schema_name, test_value
):
    # Set up database to version 5c841d2c01d1 (target migration)
    alembic_runner.migrate_up_to("5c841d2c01d1")

    # This test passes bad data and expects an integrity error back from SQLAlchemy when executed
    with pytest.raises(IntegrityError) as excinfo:
        statement = get_insert_statement(schema_name, test_value)
        with alembic_engine.connect() as conn:
            conn.execute(text(statement))

    # The specific exception raised by psycopg2 is stored internally in SQLAlchemy's IntegrityError
    # By checking this inner exception we can know that it is specifically a Check Constraint violation
    assert isinstance(excinfo.value.orig, CheckViolation)
