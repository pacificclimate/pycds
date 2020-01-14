import logging
from pytest import fixture
import testing.postgresql

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DDL, CreateSchema

import pycds
import pycds.weather_anomaly
from pycds.manage_views import daily_views, monthly_views


@fixture
def sesh_with_views(tfs_sesh):
    """Test fixture for manage_views('refresh'): Session with views defined."""
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    views = daily_views + monthly_views
    for view in views:
        view.create(tfs_sesh)
    tfs_sesh.flush()
    yield tfs_sesh
    for view in reversed(views):
        view.drop(tfs_sesh)


@fixture(scope='function')
def per_test_engine(set_search_path, add_functions):
    """Single-test database engine, so that we are starting with a clean
    database for each individual test. Somewhat slow (computationally
    expensive) but simple. We need this mechanism because:

    - to confirm that tables have been created, we use inspection
      (http://docs.sqlalchemy.org/en/latest/core/reflection.html#fine-grained-reflection-with-inspector)
    - ``inspect`` is bound to an engine, not a session
    - any session which creates views has to commit its actions to make them
        visible to the engine
    - commiting prevents the usual session rollback mechanism from working
    - therefore we either must manually remove the tables on teardown of each
        session, or just use a fresh database
    - we opt for the latter; the former proves unwieldy

    Fortunately this mechanism is only necessary for testing the view create
    operation."""
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        set_search_path(engine)  # for functions, but does not carry over to seshs
        add_functions(engine)
        pycds.Base.metadata.create_all(bind=engine)
        pycds.weather_anomaly.Base.metadata.create_all(bind=engine)
        yield engine


@fixture
def per_test_session(per_test_engine, set_search_path):
    sesh = sessionmaker(bind=per_test_engine)()
    # Default search path is `"$user", public`. Need to reset that to search
    # crmp (for our db/orm content) and public (for postgis functions)
    set_search_path(sesh)
    # session.execute('SET search_path TO crmp, public')
    # print('\nsearch_path', [r for r in session.execute('SHOW search_path')])
    yield sesh


