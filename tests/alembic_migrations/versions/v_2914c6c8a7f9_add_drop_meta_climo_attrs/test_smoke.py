"""Smoke tests:
- Upgrade
- Downgrade
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from sqlalchemy import inspect, text


logger = logging.getLogger("tests")
# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


table_name = "meta_climo_attrs"


@pytest.mark.update20
@pytest.mark.parametrize("table_exists", [False, True])
def test_upgrade(
    alembic_engine,
    alembic_runner,
    schema_name,
    table_exists,
):
    """Test the schema migration from e688e520d265 to 2914c6c8a7f9."""

    alembic_runner.migrate_up_to("e688e520d265")

    # Exercise both cases of "if not exists"
    if not table_exists:
        with alembic_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE {schema_name}.{table_name}"))

    # Upgrade to 0d99ba90c229
    alembic_runner.migrate_up_one()

    # Check that table has been dropped
    assert table_name not in inspect(alembic_engine).get_table_names(schema=schema_name)


@pytest.mark.update20
def test_downgrade(alembic_engine, alembic_runner, schema_name):
    """Test the schema migration from 2914c6c8a7f9 to e688e520d265."""
    alembic_runner.migrate_up_to("2914c6c8a7f9")

    # Downgrade to revision e688e520d265
    alembic_runner.migrate_down_one()

    # Check that table has been added
    assert table_name in inspect(alembic_engine).get_table_names(schema=schema_name)
