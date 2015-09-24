from pkg_resources import resource_filename
import logging, logging.config
import sys

import testing.postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest

import pycds
from pycds import Network, Contact, Station, History, Variable

def pytest_runtest_setup():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

@pytest.yield_fixture(scope='function')
def blank_postgis_session():
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        sesh = sessionmaker(bind=engine)()

        yield sesh

@pytest.yield_fixture(scope='function')
def test_session(blank_postgis_session):
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    engine = blank_postgis_session.get_bind()
    pycds.Base.metadata.create_all(bind=engine)
    pycds.DeferredBase.metadata.create_all(bind=engine)

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

@pytest.fixture(scope="module")
def conn_params():
    return 'sqlite:///{0}'.format(resource_filename('pycds', 'data/crmp.sqlite'))
