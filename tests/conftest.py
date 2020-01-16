import logging, logging.config
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DDL, CreateSchema

from pytest import fixture
import testing.postgresql

import pycds
import pycds.weather_anomaly
from pycds.functions import daysinmonth, effective_day


def pytest_runtest_setup():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


@fixture(scope='session')
def schema_name():
    return pycds.get_schema_name()


@fixture(scope='session')
def set_search_path():
    def f(executor):
        executor.execute(f'SET search_path TO public')
    return f


@fixture(scope='session')
def add_functions():
    def f(executor):
        executor.execute(daysinmonth())
        executor.execute(effective_day())
    return f


@fixture(scope='session')
def base_engine(schema_name, set_search_path, add_functions):
    """Test-session scoped base database engine.
    "Base" engine indicates that it has no ORM content created in it.
    """
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema(schema_name))
        set_search_path(engine)
        add_functions(engine)
        yield engine


@fixture(scope='session')
def pycds_engine(base_engine):
    """Test-session scoped database engine, with pycds ORM created in it.
    """
    pycds.Base.metadata.create_all(bind=base_engine)
    # pycds.weather_anomaly.Base.metadata.create_all(bind=engine)
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
