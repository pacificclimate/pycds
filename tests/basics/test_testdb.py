from ..helpers import insert_test_data, insert_crmp_data
from pycds import Contact, History, Obs


def test_reflect_tables_into_session(schema_name, pycds_sesh):
    res = pycds_sesh.execute(f'''
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}';
    ''')

    assert {x[0] for x in res.fetchall()} >= {
        'meta_sensor', 'meta_contact', 'climo_obs_count_mv',
        'obs_count_per_month_history_mv',
        'vars_per_history_mv', 'meta_history',
        'meta_vars', 'meta_network', 'meta_station',
        'obs_raw', 'meta_native_flag', 'obs_raw_native_flags'
    }


def test_can_create_test_db(pycds_sesh):
    insert_test_data(pycds_sesh)
    q = pycds_sesh.query(Contact)
    assert len(q.all()) == 2


def test_can_create_crmp_subset_db(pycds_sesh):
    insert_crmp_data(pycds_sesh)
    q = pycds_sesh.query(History)
    assert q.count() > 0
