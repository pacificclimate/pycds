from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pycds.util import create_test_database, create_test_data, insert_crmp_data
from pycds import Contact, History, Obs

def test_reflect_tables_into_session(blank_postgis_session):
    engine = blank_postgis_session.get_bind()
    create_test_database(engine)

    res = blank_postgis_session.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    res = [x[0] for x in res.fetchall()]
    print res

    assert set(res).issuperset(set(['meta_sensor', 'meta_contact', 'climo_obs_count_mv',
        'obs_count_per_month_history_mv', 'meta_network_geoserver',
        'vars_per_history_mv', 'crmp_network_geoserver', 'meta_history',
        'meta_vars', 'meta_network', 'meta_station', 'obs_with_flags',
        'obs_raw', 'meta_native_flag', 'obs_raw_native_flags']))


def test_can_create_test_db(blank_postgis_session):
    engine = blank_postgis_session.get_bind()
    create_test_database(engine)
    create_test_data(engine)
    # Get some data
    q = blank_postgis_session.query(Contact)
    assert len(q.all()) == 2

def test_can_create_crmp_subset_db(blank_postgis_session):
    engine = blank_postgis_session.get_bind()
    create_test_database(engine)
    insert_crmp_data(blank_postgis_session)

    q = blank_postgis_session.query(History)
    assert q.count() > 0
