"""Smoke tests:
- Upgrade creates functions
- Downgrade drops functions
- Created functions are executable
"""

# -*- coding: utf-8 -*-
from _datetime import datetime, date
import logging
import pytest
from alembic import command
from ....helpers import get_schema_item_names


logger = logging.getLogger('tests')


function_names = {"closest_stns_within_threshold", "daily_ts",
                  "daysinmonth", "do_query_one_station", "effective_day",
                  "getstationvariabletable", "lastdateofmonth", "monthly_ts",
                  "query_one_station", "query_one_station_climo", "season",
                  "updatesdateedate", }


@pytest.mark.usefixtures('new_db_left')
def test_upgrade(prepared_schema_from_migrations_left, schema_name):
    """Test the schema migration from 522eed334c85 to 4a2f1879293a. """

    # Set up database to version 4a2f1879293a
    engine, script = prepared_schema_from_migrations_left

    # Check that functions have been added
    names = get_schema_item_names(engine, 'routines', schema_name=schema_name)
    assert names >= function_names


@pytest.mark.usefixtures('new_db_left')
def test_downgrade(prepared_schema_from_migrations_left, alembic_config_left, schema_name):
    """Test the schema migration from 4a2f1879293a to 522eed334c85. """

    # Set up database to version 4a2f1879293a
    engine, script = prepared_schema_from_migrations_left

    # Run downgrade migration
    command.downgrade(alembic_config_left, '-1')

    # Check that functions have been removed
    names = get_schema_item_names(engine, 'routines', schema_name=schema_name)
    assert names & function_names == set()


@pytest.mark.usefixtures('new_db_left')
@pytest.mark.parametrize('func, args', [
    ('closest_stns_within_threshold', (-120.0, 50.0, 1)),
    pytest.param(
        'daily_ts', (1, 1, 50.0),
        marks=pytest.mark.xfail(reason='Bug in function definition')
    ),
    ('daysinmonth', (datetime(2000, 3, 4),)),
    pytest.param(
        'do_query_one_station', (999,),
        marks=pytest.mark.xfail(reason='No data in test database')
    ),
    ('effective_day', (datetime(2000, 1, 1, 7, 39), 'max', '1-hourly')),
    ('getstationvariabletable', (999, False)),
    ('lastdateofmonth', (date(2000, 3, 4),)),
    pytest.param(
        'monthly_ts', (1, 1, 50.0),
        marks=pytest.mark.xfail(reason='Bug in function definition')
    ),
    ('query_one_station', (999,),),
    ('query_one_station_climo', (999,),),
    ('season', (datetime(2000, 3, 4),)),
    ('updatesdateedate', ()),
])
def test_executable(
        func, args, sesh_in_prepared_schema_left, schema_func,
):
    """Smoke test that a function invoked with the given arguments doesn't raise
    an exception. This proves nothing much except that (part of) the function
    is executable.
    """
    fn = getattr(schema_func, func)
    q = sesh_in_prepared_schema_left.query(fn(*args))
    q.all()
