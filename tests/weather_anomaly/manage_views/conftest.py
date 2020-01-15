import logging
from pytest import fixture
import testing.postgresql

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DDL, CreateSchema

from ...helpers import create_then_drop_views
import pycds
import pycds.weather_anomaly
from pycds.manage_views import daily_views, monthly_views


@fixture
def sesh_with_views(tfs_pycds_sesh):
    """Test fixture for manage_views('refresh'): Session with views defined."""
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    for s in create_then_drop_views(
            tfs_pycds_sesh, daily_views + monthly_views):
        s.flush()
        yield s


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
    set_search_path(sesh)
    yield sesh


