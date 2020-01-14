from pytest import fixture
from sqlalchemy.orm import sessionmaker
from pycds import Contact, Network, Station, History, Variable, Obs, \
    NativeFlag, PCICFlag


@fixture
def tfs_empty_sesh(tss_base_engine, set_search_path):
    """Test-function scoped (tfs) database session, with no content whatsoever.
    All session actions are rolled back on teardown.
    """
    sesh = sessionmaker(bind=tss_base_engine)()
    set_search_path(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()


@fixture
def tfs_pycds_sesh_with_small_data(tfs_pycds_sesh):
    moti = Network(name='MoTIe')
    ec = Network(name='EC')
    wmb = Network(name='FLNROW-WMB')
    tfs_pycds_sesh.add_all([moti, ec, wmb])

    simon = Contact(name='Simon', networks=[moti])
    eric = Contact(name='Eric', networks=[wmb])
    pat = Contact(name='Pat', networks=[ec])
    tfs_pycds_sesh.add_all([simon, eric, pat])

    stations = [
        Station(native_id='11091', network=moti, histories=[History(station_name='Brandywine', the_geom='SRID=4326;POINT(-123.11806 50.05417)')]),
        Station(native_id='1029', network=wmb, histories=[History(station_name='FIVE MILE', the_geom='SRID=4326;POINT(-122.68889 50.91089)')]),
        Station(native_id='2100160', network=ec, histories=[History(station_name='Beaver Creek Airport', the_geom='SRID=4326;POINT(-140.866667 62.416667)')])
    ]
    tfs_pycds_sesh.add_all(stations)

    variables = [Variable(name='CURRENT_AIR_TEMPERATURE1', unit='celsius', network=moti),
                 Variable(name='precipitation', unit='mm', network=ec),
                 Variable(name='relative_humidity', unit='percent', network=wmb)
                 ]
    tfs_pycds_sesh.add_all(variables)

    yield tfs_pycds_sesh
