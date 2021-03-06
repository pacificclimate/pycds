import logging, logging.config
import sys

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DDL, CreateSchema

from pytest import fixture
import testing.postgresql

import pycds
import pycds.weather_anomaly


def pytest_runtest_setup():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logger = logging.getLogger('tests')
    logger.setLevel(logging.DEBUG)


@fixture(scope='session')
def schema_name():
    return pycds.get_schema_name()


@fixture(scope='session')
def schema_func(schema_name):
    return getattr(func, schema_name)


@fixture(scope='session')
def set_search_path():
    def f(executor):
        executor.execute(f'SET search_path TO public')
    return f


# TODO: Remove altogether after transfer of matviews to migration
@fixture(scope='session')
def add_functions():
    def f(executor):
        # executor.execute(daysinmonth())
        # executor.execute(effective_day())
        pass
    return f


@fixture(scope='session')
def base_database_uri():
    """Test-session scoped base database.
    """
    with testing.postgresql.Postgresql() as pg:
        yield pg.url()


# TODO: Separate out add_functions
@fixture(scope='session')
def base_engine(base_database_uri, schema_name, set_search_path, add_functions):
    """Test-session scoped base database engine.
    "Base" engine indicates that it has no ORM content created in it.
    """
    engine = create_engine(base_database_uri)
    engine.execute('CREATE EXTENSION postgis')
    engine.execute('CREATE EXTENSION plpythonu')
    engine.execute(CreateSchema(schema_name))
    set_search_path(engine)
    add_functions(engine)
    yield engine


@fixture(scope='session')
def pycds_engine(base_engine):
    """Test-session scoped database engine, with pycds ORM created in it.
    """
    pycds.Base.metadata.create_all(bind=base_engine)
    yield base_engine


@fixture(scope='function')
def pycds_sesh(pycds_engine, set_search_path):
    """Test-function scoped database session.
    All session actions are rolled back on teardown.
    """
    sesh = sessionmaker(bind=pycds_engine)()
    set_search_path(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()
