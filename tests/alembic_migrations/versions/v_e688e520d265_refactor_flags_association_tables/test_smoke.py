"""Smoke tests:
- Upgrade
- Downgrade
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from ....helpers import get_schema_item_names
import pycds.alembic.helpers


logger = logging.getLogger("tests")
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "table_name, unique_constraint_name, primary_key_name",
    [
        (
            "obs_raw_native_flags",
            "obs_raw_native_flag_unique",
            "obs_raw_native_flags_pkey",
        ),
        (
            "obs_raw_pcic_flags",
            "obs_raw_pcic_flag_unique",
            "obs_raw_pcic_flags_pkey",
        ),
    ],
)
def test_upgrade(
    prepared_schema_from_migrations_left,
    table_name,
    unique_constraint_name,
    primary_key_name,
    schema_name,
):
    """Test the schema migration from bdc28573df56 to e688e520d265. """

    # Set up database to revision e688e520d265
    engine, script = prepared_schema_from_migrations_left

    # Check that unique constraint does not exist
    unique_constraint_names = get_schema_item_names(
        engine,
        "constraints",
        table_name=table_name,
        constraint_type="unique",
        schema_name=schema_name,
    )
    assert unique_constraint_name not in unique_constraint_names

    # Check that primary key exists
    pkey_constraint_names = get_schema_item_names(
        engine,
        "constraints",
        table_name=table_name,
        constraint_type="primary",
        schema_name=schema_name,
    )
    assert primary_key_name in pkey_constraint_names


@pytest.mark.usefixtures("prepared_schema_from_migrations_left")
@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left,
    prior_index_names,
    alembic_config_left,
    schema_name,
    mocker,
):
    """
    """

    # Set up database to revision bdc28573df56
    engine, script = prepared_schema_from_migrations_left
