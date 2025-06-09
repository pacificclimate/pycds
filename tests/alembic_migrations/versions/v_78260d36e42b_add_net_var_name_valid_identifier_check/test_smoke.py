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
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import CheckViolation

logger = logging.getLogger("tests")

# this is the revision *before* the migration is to be run.
prior_revision = "83896ee79b06"
final_revision = "78260d36e42b"


def get_insert_statement(schema_name, insertion_value, vars_id=1):
    return (
        f"INSERT INTO {schema_name}.meta_vars(vars_id, net_var_name, standard_name, cell_method, display_name)\n"
        f"VALUES ({vars_id}, '{insertion_value}', 'test', 'test', 'test');\n"
    )


@pytest.mark.update20
def test_upgrade(
    alembic_engine,
    alembic_runner,
    alembic_config_left,
    schema_name,
):
    """test migration from 83896ee79b06 to 78260d36e42b."""
    alembic_runner.migrate_up_before("78260d36e42b")

    # (insertion_id, insertion_value, expected_value)
    test_values = [
        (1, "bad var name with whitespace", "bad_var_name_with_whitespace"),
        (2, "bad\nnewline", "bad_newline"),
        (3, "bad-dash", "bad_dash"),
        (4, "1badnumber", "_badnumber"),
        (5, "$badDollar", "_badDollar"),
        (6, "schön", "schon"),
        (7, "forêt", "foret"),
        (8, "tréma", "trema"),
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
                f"SELECT vars_id, net_var_name FROM {schema_name}.meta_vars ORDER BY vars_id"
            )
        )

    for row, test_sample in zip(result, test_values):
        assert row == (test_sample[0], test_sample[2])


@pytest.mark.update20
def test_downgrade(
    alembic_engine,
    alembic_runner,
    schema_name,
):
    """test migration from 78260d36e42b to 83896ee79b06."""

    # Set up database to version bb2a222a1d4a (previous migration)
    alembic_runner.migrate_up_to("78260d36e42b")

    statement = get_insert_statement(schema_name, "good_var_name_with_underscores")
    with alembic_engine.begin() as conn:
        conn.execute(text(statement))

    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT vars_id, net_var_name FROM {schema_name}.meta_vars")
        )

    row = next(result)

    # After migration is downgrade, existing data that has been updated to conform to the new constraint
    # should still be valid and retrievable.
    assert row == (1, "good_var_name_with_underscores")


# Correct values should conform to https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
# Notably numbers and $ are allowed, but not as the first character
@pytest.mark.parametrize(
    "test_value",
    ["allow_underscores", "allowfullwords", "allowCapitals", "allowNumbers123"],
)
@pytest.mark.update20
def test_check_good_constraint_values(
    alembic_engine, alembic_runner, schema_name, test_value
):
    # Set up database to version 78260d36e42b (after migration)
    alembic_runner.migrate_up_to("78260d36e42b")

    statement = get_insert_statement(schema_name, test_value)
    with alembic_engine.begin() as conn:
        conn.execute(text(statement))
        result = conn.execute(
            text(f"SELECT vars_id, net_var_name FROM {schema_name}.meta_vars")
        )

    row = next(result)

    assert row == (1, test_value)


# Correct values should conform to https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
# Notably numbers and $ are allowed, but not as the first character
@pytest.mark.parametrize(
    "test_value",
    [
        "bad space",
        "bad\nnewline",
        "bad-dash",
        "1badnumber",
        "$badDollar",
        "schön",
        "forêt",
        "tréma",
    ],
)
@pytest.mark.update20
def test_check_bad_constraint_values(
    alembic_engine, alembic_runner, schema_name, test_value
):
    # Set up database to version 78260d36e42b (after migration)
    alembic_runner.migrate_up_to("78260d36e42b")
    # This test passes bad data and expects an integrity error back from SQLAlchemy when executed
    with pytest.raises(IntegrityError) as excinfo:
        statement = get_insert_statement(schema_name, test_value)
        with alembic_engine.begin() as conn:
            conn.execute(text(statement))

    # The specific exception raised by psycopg2 is stored internally in SQLAlchemy's IntegrityError
    # By checking this inner exception we can know that it is specifically a Check Constraint violation
    assert isinstance(excinfo.value.orig, CheckViolation)
