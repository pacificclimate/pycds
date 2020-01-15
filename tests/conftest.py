import logging, logging.config
import sys

import testing.postgresql
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DDL, CreateSchema

import pytest
from pytest import fixture

import pycds
import pycds.weather_anomaly
from pycds.views import \
    CrmpNetworkGeoserver, HistoryStationNetwork, ObsCountPerDayHistory, \
    ObsWithFlags
from pycds.functions import daysinmonth, effective_day


def pytest_runtest_setup():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


all_views = [
    CrmpNetworkGeoserver,
    HistoryStationNetwork,
    ObsCountPerDayHistory,
    ObsWithFlags
]


#################################################################

# These fixtures are intended to replace the welter of existing fixtures.
# They are intended to be minimal in number and to perform a common sequence
# of actions (e.g., create schema, set search path) in a common ordering,
# and as much as possible with a common fixture breakdown or dependency pattern
# (e.g., engine <-- sesh)

@fixture(scope='session')
def set_search_path():
    def f(executor):
        executor.execute('SET search_path TO crmp, public')
    return f


@fixture(scope='session')
def add_functions():
    def f(executor):
        executor.execute(daysinmonth)
        executor.execute(effective_day)
    return f


@fixture(scope='session')
def tss_base_engine(set_search_path, add_functions):
    """Test-session scoped (tss) database engine.
    "Base" engine indicates that it has no ORM content created in it.
    """
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        set_search_path(engine)
        add_functions(engine)
        yield engine


@fixture(scope='session')
def tss_pycds_engine(tss_base_engine):
    """Test-session scoped (tss) database engine,
    with pycds ORM created in it.
    """
    pycds.Base.metadata.create_all(bind=tss_base_engine)
    # pycds.weather_anomaly.Base.metadata.create_all(bind=engine)
    yield tss_base_engine


@fixture(scope='function')
def tfs_pycds_sesh(tss_pycds_engine, set_search_path):
    """Test-function scoped (tfs) database session.
    All session actions are rolled back on teardown.

    NOTE: We use the term 'sesh' throughout for database sessions, to
    disambiguate it from test sessions, for which we use the term 'session'.
    """
    sesh = sessionmaker(bind=tss_pycds_engine)()
    set_search_path(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()



#################################################################

# Set up database environment before testing. This is triggered each time a
# database is created; in these tests, by `.metadata.create_all()` calls.
@event.listens_for(pycds.weather_anomaly.Base.metadata, 'before_create')
def do_before_create(target, connection, **kw):
    return
    connection.execute('SET search_path TO crmp, public')
    # Add required functions
    connection.execute(daysinmonth)
    connection.execute(effective_day)
