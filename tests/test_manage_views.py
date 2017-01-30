from pytest import fixture, mark
import testing.postgresql

from sqlalchemy import text, inspect

import pycds
from pycds.manage_views import manage_views


base_view_names = ['discarded_obs_v']
daily_view_names = ['daily_max_temperature_mv', 'daily_min_temperature_mv']
monthly_view_names = ['monthly_average_of_daily_max_temperature_mv', 'monthly_average_of_daily_min_temperature_mv',
                 'monthly_total_precipitation_mv']


@fixture()
def cleanup_db_session(session):
    """To cause manage_views to take effect, session.commit() must be called.
    Unfortunately, that durably modifies the database; after a commit, the normal `session` rollback doesn't have any
    effect. So either we need to set up a database engine with 'function' scope, not 'session', or we need to clean
    up the database each time. This fixture implements the latter."""
    yield session
    for name in reversed(daily_view_names + monthly_view_names):
        session.execute('DROP TABLE IF EXISTS crmp.{}'.format(name))
    for name in reversed(base_view_names):
        session.execute('DROP VIEW IF EXISTS crmp.{}'.format(name))
    session.commit()

@mark.parametrize(', operation, view, exp_matview_names', [
    ('create', 'daily', daily_view_names),
    # `create monthly` will fail if the daily views do not already exist, succeed if they do;
    # we don't bother setting up the test machinery to test that
    ('create', 'all', daily_view_names + monthly_view_names),
])
def test_it(engine, cleanup_db_session, operation, view, exp_matview_names):

    def check_views_and_tables(present):
        def check(expected_names, actual_names):
            for name in expected_names:
                assert (present and (name in actual_names)) or (not present and (name not in actual_names))

        check(base_view_names, inspect(engine).get_view_names(schema='crmp'))
        check(exp_matview_names, inspect(engine).get_table_names(schema='crmp'))

    check_views_and_tables(False)
    manage_views(cleanup_db_session, operation, view)
    cleanup_db_session.commit()
    check_views_and_tables(True)
