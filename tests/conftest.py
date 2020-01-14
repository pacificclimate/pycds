import datetime

from pkg_resources import resource_filename
import logging, logging.config
import sys

import testing.postgresql
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import DDL, CreateSchema

import pytest
from pytest import fixture

import pycds
import pycds.weather_anomaly
from pycds import Contact, Network, Station, History, Variable, Obs, \
    NativeFlag, PCICFlag
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
def tss_engine(set_search_path, add_functions):
    """Test-session scoped (tss) database engine"""
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        set_search_path(engine)
        add_functions(engine)
        pycds.Base.metadata.create_all(bind=engine)
        # pycds.weather_anomaly.Base.metadata.create_all(bind=engine)
        yield engine


@fixture(scope='function')
def tfs_sesh(tss_engine, set_search_path):
    """Test-function scoped (tfs) database session.
    All session actions are rolled back on teardown.

    NOTE: We use the term 'sesh' throughout for database sessions, to
    disambiguate it from test sessions, for which we use the term 'session'.
    """
    sesh = sessionmaker(bind=tss_engine)()
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


@fixture(scope='session')
def engine():
    """Test-session-wide database engine"""
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        pycds.Base.metadata.create_all(bind=engine)
        pycds.weather_anomaly.Base.metadata.create_all(bind=engine)
        yield engine


@fixture(scope='function')
def session(engine):
    """Single-test database session. All session actions are rolled back on teardown"""
    session = sessionmaker(bind=engine)()
    yield session
    session.rollback()
    session.close()


@fixture(scope='module')
def mod_blank_postgis_session():
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        sesh = sessionmaker(bind=engine)()
        yield sesh

@fixture(scope='module')
def mod_empty_database_session(mod_blank_postgis_session):
    sesh = mod_blank_postgis_session
    engine = sesh.get_bind()
    pycds.Base.metadata.create_all(bind=engine)
    pycds.weather_anomaly.Base.metadata.create_all(bind=engine)
    yield sesh

@pytest.yield_fixture(scope='function')
def blank_postgis_session():
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        sesh = sessionmaker(bind=engine)()
        sesh.execute('SET search_path TO crmp, public')

        yield sesh

@pytest.yield_fixture(scope='function')
def test_session(blank_postgis_session):
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    engine = blank_postgis_session.get_bind()
    pycds.Base.metadata.create_all(bind=engine)

    moti = Network(name='MoTIe')
    ec = Network(name='EC')
    wmb = Network(name='FLNROW-WMB')
    blank_postgis_session.add_all([moti, ec, wmb])

    simon = Contact(name='Simon', networks=[moti])
    eric = Contact(name='Eric', networks=[wmb])
    pat = Contact(name='Pat', networks=[ec])
    blank_postgis_session.add_all([simon, eric, pat])

    stations = [
        Station(native_id='11091', network=moti, histories=[History(station_name='Brandywine', the_geom='SRID=4326;POINT(-123.11806 50.05417)')]),
        Station(native_id='1029', network=wmb, histories=[History(station_name='FIVE MILE', the_geom='SRID=4326;POINT(-122.68889 50.91089)')]),
        Station(native_id='2100160', network=ec, histories=[History(station_name='Beaver Creek Airport', the_geom='SRID=4326;POINT(-140.866667 62.416667)')])
        ]
    blank_postgis_session.add_all(stations)

    variables = [Variable(name='CURRENT_AIR_TEMPERATURE1', unit='celsius', network=moti),
                 Variable(name='precipitation', unit='mm', network=ec),
                 Variable(name='relative_humidity', unit='percent', network=wmb)
                 ]
    blank_postgis_session.add_all(variables)

    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO) # Let's not log all the db setup stuff...

    yield blank_postgis_session

@pytest.yield_fixture(scope='function')
def large_test_session(blank_postgis_session):
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    engine = blank_postgis_session.get_bind()
    pycds.Base.metadata.create_all(bind=engine)

    with open(resource_filename('pycds', 'data/crmp_subset_data.sql'), 'r') as f:
        sql = f.read()
    blank_postgis_session.execute(sql)

    # Hmmm... this should have been created by
    # `pycds.Base.metadata.create_all(bind=engine)` above
    for view in all_views:
        view.create(blank_postgis_session)

    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO) # Let's not log all the db setup stuff...

    yield blank_postgis_session

    for view in reversed(all_views):
        view.drop(blank_postgis_session)

# To maintain database consistency, objects must be added (and flushed) in this order:
#   Network
#   Station, History
#   Variable
#   Observation
#
# This imposes an order on the definition of session fixtures, and on the nesting of describe blocks that use them.

@fixture
def network1():
    return Network(name='Network 1')

@fixture
def network2():
    return Network(name='Network 2')

@fixture
def station1(network1):
    return Station(network=network1)

@fixture
def station2(network2):
    return Station(network=network2)

history_transition_date = datetime.datetime(2010, 1, 1)

@fixture
def history_stn1_hourly(station1):
    return History(station=station1, station_name='Station 1',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')

@fixture
def history_stn1_12_hourly(station1):
    return History(station=station1, station_name='Station 1',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='12-hourly')

@fixture
def history_stn1_daily(station1):
    return History(station=station1, station_name='Station 1',
                   sdate=history_transition_date, edate=None, freq='daily')

@fixture
def history_stn2_hourly(station2):
    return History(station=station2, station_name='Station 2',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')

@fixture
def var_temp_point(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: point')

@fixture
def var_temp_point2(network2):
    return Variable(network=network2, standard_name='air_temperature', cell_method='time: point')

@fixture
def var_temp_max(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: maximum')

@fixture
def var_temp_min(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: minimum')

@fixture
def var_temp_mean(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: mean')

@fixture
def var_foo(network1):
    return Variable(network=network1, standard_name='foo', cell_method='time: point')

@fixture
def var_precip_net1_1(network1):
    return Variable(network=network1, standard_name='thickness_of_rainfall_amount', cell_method='time: sum')

@fixture
def var_precip_net1_2(network1):
    return Variable(network=network1, standard_name='thickness_of_rainfall_amount', cell_method='time: sum')

@fixture
def var_precip_net2_1(network2):
    return Variable(network=network2, standard_name='thickness_of_rainfall_amount', cell_method='time: sum')

@fixture
def native_flag_discard():
    return NativeFlag(discard=True)

@fixture
def native_flag_non_discard():
    return NativeFlag(discard=False)

@fixture
def pcic_flag_discard():
    return PCICFlag(discard=True)

@fixture
def pcic_flag_non_discard():
    return PCICFlag(discard=False)
