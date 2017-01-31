import datetime
from pytest import fixture, mark
import testing.postgresql

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy import text, inspect

import pycds
from pycds import Obs
from pycds.manage_views import base_views, daily_views, monthly_views, manage_views


base_view_names = [v.viewname() for v in base_views]
daily_view_names = [v.viewname() for v in daily_views]
monthly_view_names = [v.viewname() for v in monthly_views]


@fixture(scope='function')
def per_test_engine():
    """Single-test database engine, so that we are starting with a clean database for each individual test.
    Somewhat slow (computationally expensive) but simple. We need this mechanism because:

    - to confirm that tables have been created, we use inspection
      (http://docs.sqlalchemy.org/en/latest/core/reflection.html#fine-grained-reflection-with-inspector)
    - ``inspect`` is bound to an engine, not a session
    - any session which creates views has to commit its actions to make them visible to the engine
    - commiting prevents the usual session rollback mechanism from working
    - therefore we either must manually remove the tables on teardown of each session, or just use a fresh database
    - we opt for the latter; the former proves unwieldy

    Fortunately this mechanism is only necessary for testing the create operation."""
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        pycds.Base.metadata.create_all(bind=engine)
        pycds.weather_anomaly.Base.metadata.create_all(bind=engine)
        yield engine


@fixture
def per_test_session(per_test_engine):
    session = sessionmaker(bind=per_test_engine)()
    # Default search path is `"$user", public`. Need to reset that to search crmp (for our db/orm content) and
    # public (for postgis functions)
    session.execute('SET search_path TO crmp, public')
    # print('\nsearch_path', [r for r in session.execute('SHOW search_path')])
    yield session


@mark.parametrize('what, exp_matview_names', [
    ('daily', daily_view_names),
    # `create monthly` will fail if the daily views do not already exist, succeed if they do;
    # we don't bother setting up the test machinery to test that
    ('all', daily_view_names + monthly_view_names),
])
def test_create(per_test_engine, per_test_session, what, exp_matview_names):

    def check_views_and_tables(present):
        def check(expected_names, actual_names):
            for name in expected_names:
                assert (present and (name in actual_names)) or (not present and (name not in actual_names))

        check(base_view_names, inspect(per_test_engine).get_view_names(schema='crmp'))
        check(exp_matview_names, inspect(per_test_engine).get_table_names(schema='crmp'))

    check_views_and_tables(False)
    manage_views(per_test_session, 'create', what)
    per_test_session.commit()
    check_views_and_tables(True)


@fixture
def session_with_views(session):
    """Test fixture for manage_views('refresh'): Session with views defined."""
    print('\nsession_with_views: SETUP')
    views = base_views + daily_views + monthly_views
    for view in views:
        view.create(session)
    session.flush()
    yield session
    print('\nsession_with_views: TEARDOWN')
    for view in reversed(views):
        view.drop(session)
    print('\nsession_with_views: DONE')


@mark.parametrize('what, exp_views', [
    ('daily', daily_views),
    # `refresh monthly` will fail if the daily views haven't been refreshed, succeed if they do;
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
