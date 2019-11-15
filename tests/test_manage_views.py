import datetime
import logging

from pytest import fixture, mark
import testing.postgresql

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DDL, CreateSchema
from sqlalchemy import text, inspect

import pycds
import pycds.weather_anomaly
from pycds import Obs
from pycds.manage_views import daily_views, monthly_views, manage_views

from pycds.util import get_search_path, set_search_path


daily_view_names = [v.viewname() for v in daily_views]
monthly_view_names = [v.viewname() for v in monthly_views]


@mark.parametrize('what, exp_matview_names', [
    ('daily', daily_view_names),
    # `create monthly-only` will fail if the daily views do not already exist, succeed if they do;
    # we don't bother setting up the test machinery to test that
    ('all', daily_view_names + monthly_view_names),
])
def test_create(
        per_test_engine, per_test_session, what, exp_matview_names, schema_name
):

    print('### test_create')
    engine_inspector = inspect(per_test_engine)
    print('### search_path', get_search_path(per_test_session))

    schema_names = engine_inspector.get_schema_names()
    for name in schema_names:
        print('### schema', name)
        print('   ### tables', engine_inspector.get_table_names(schema=name))

    def check_views_and_tables(present):
        def check(expected_names, actual_names):
            for name in expected_names:
                assert (present and (name in actual_names)) or (not present and (name not in actual_names))

        check(exp_matview_names, inspect(per_test_engine).get_table_names(schema=schema_name))

    check_views_and_tables(False)
    manage_views(per_test_session, 'create', what)
    per_test_session.commit()
    check_views_and_tables(True)


@fixture
def session_with_views(session):
    """Test fixture for manage_views('refresh'): Session with views defined."""
    logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    views = daily_views + monthly_views
    for view in views:
        view.create(session)
    session.flush()
    print('### session_with_views search_path', get_search_path(session))
    yield session
    for view in reversed(views):
        view.drop(session)


@mark.parametrize('what, exp_views', [
    ('daily', daily_views),
    # `refresh monthly-only` will fail if the daily views haven't been refreshed, succeed if they do;
    # we don't bother setting up the test machinery to test that
    ('all', daily_views + monthly_views),
])
def test_refresh(session_with_views, what, exp_views,
                 network1, station1, history_stn1_hourly, var_temp_point, var_precip_net1_1):

    # initially, each view should be empty, because no data
    for view in exp_views:
        assert session_with_views.query(view).count() == 0

    # Add observations (and all the equipment necessary therefor) so that the views will have something to chew
    session = session_with_views
    session.add(network1)
    session.add(station1)
    session.add(history_stn1_hourly)
    session.add(var_temp_point)
    session.add_all([
        Obs(
            variable=variable,
            history=history_stn1_hourly,
            time=datetime.datetime(2000, 1, 1, 1),
            datum = 1.0
        )
        for variable in [var_temp_point, var_precip_net1_1]
    ])
    session.flush()

    # chew
    manage_views(session_with_views, 'refresh', what)
    session_with_views.flush()

    # now each view should contain something
    for view in exp_views:
        assert session_with_views.query(view).count() > 0
