"""Smoke tests:
- Upgrade creates matview
- Downgrade removes matviews
"""

# -*- coding: utf-8 -*-
import logging
import pytest
from alembic import command
from sqlalchemy import inspect
from pycds.database import get_schema_item_names

logger = logging.getLogger("tests")


from .. import check_matviews

matview_defns = { "cum_pcpn_month_minmax_mv" : {"indexes": {}},
"cum_pcpn_month_start_time_mv" : {"indexes": {}},
"cum_pcpn_month_end_time_mv" : {"indexes": {}},
"cum_pcpn_month_start_mv" : {"indexes": {}},
"cum_pcpn_month_end_mv" : {"indexes": {"cum_pcpn_per_month_idx"}}}


@pytest.mark.usefixtures("new_db_left")
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration to version 9e5ca6dc3f02."""

    # Set up database to version 3505750d3416
    engine, script = prepared_schema_from_migrations_left

    # this check only confirms that the matview and its index exist;
    # it's hard to directly check the columns via sqlalchemy.
    # Behavioural tests address this elsewhere.
    check_matviews(engine, matview_defns, schema_name, matviews_present=True)


@pytest.mark.usefixtures("new_db_left")
def test_downgrade(
    prepared_schema_from_migrations_left, alembic_config_left, schema_name
):
    """Test the schema migration from 9e5ca6dc3f02 to 3505750d3416."""

    # Set up database to version efde19ea4f52
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, "-1")

    # this check only confirms that the matview and its index exist;
    # it's hard to directly check the columns via sqlalchemy.
    # Behavioural tests address this elsewhere.
    check_matviews(engine, matview_defns, schema_name, matviews_present=True)
