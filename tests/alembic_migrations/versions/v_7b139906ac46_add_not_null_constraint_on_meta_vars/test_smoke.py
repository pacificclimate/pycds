"""Smoke tests:
- Upgrade adds not null constraint
- Downgrade drops not null constraint
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from sqlalchemy import inspect, text

logger = logging.getLogger("tests")


@pytest.mark.update20
def test_upgrade(alembic_engine, alembic_runner, schema_name):
    """test migration from 3d50ec832e47 to 7b139906ac46."""
    alembic_runner.migrate_up_before("7b139906ac46")

    with alembic_engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO {schema_name}.meta_vars(vars_id)" f"VALUES (1)")
        )

    alembic_runner.migrate_up_one()

    with alembic_engine.begin() as conn:
        # Check that data has been modified to remove nulls
        result = conn.execute(
            text(
                f"SELECT vars_id, cell_method, standard_name, display_name FROM {schema_name}.meta_vars"
            )
        )

    row = next(result)
    assert row == (1, "foo: bar", "foo_bar", "foo bar")

    # Check that not null constraints have been added
    table = inspect(alembic_engine).get_columns("meta_vars", schema=schema_name)

    for col in table:
        if (
            col["name"] == "cell_method"
            or col["name"] == "standard_name"
            or col["name"] == "display_name"
        ):
            assert col["nullable"] == False


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 7b139906ac46 to 3d50ec832e47."""
    alembic_runner.migrate_up_to("7b139906ac46")
    # Set up database to version 7b139906ac46

    with alembic_engine.begin() as conn:
        conn.execute(
            text(
                f"INSERT INTO {schema_name}.meta_vars(vars_id, cell_method, standard_name, display_name)"
                f"VALUES (10000, 'foo: bar', 'foo_bar', 'foo bar')"
            )
        )

    # Run downgrade migration
    alembic_runner.migrate_down_one()

    # Check that data has been modified to insert nulls back in

    with alembic_engine.begin() as conn:
        result = conn.execute(
            text(
                f"SELECT vars_id, cell_method, standard_name, display_name FROM {schema_name}.meta_vars"
            )
        )

    row = next(result)
    assert row == (10000, None, None, None)

    # Check that not null constraints have been removed
    table = inspect(alembic_engine).get_columns("meta_vars", schema=schema_name)

    for col in table:
        if (
            col["name"] == "cell_method"
            or col["name"] == "standard_name"
            or col["name"] == "display_name"
        ):
            assert col["nullable"] == True
