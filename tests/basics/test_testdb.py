from pycds.util import create_test_data, insert_crmp_data
from pycds import Contact, History, Obs


def test_reflect_tables_into_session(tfs_pycds_sesh):
    res = tfs_pycds_sesh.execute('''
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'crmp';
    ''')

    assert {x[0] for x in res.fetchall()} >= {
        'meta_sensor', 'meta_contact', 'climo_obs_count_mv',
        'obs_count_per_month_history_mv',
        'vars_per_history_mv', 'meta_history',
        'meta_vars', 'meta_network', 'meta_station',
        'obs_raw', 'meta_native_flag', 'obs_raw_native_flags'
    }


def test_can_create_test_db(tfs_pycds_sesh):
    create_test_data(tfs_pycds_sesh)
    q = tfs_pycds_sesh.query(Contact)
    assert len(q.all()) == 2


def test_can_create_crmp_subset_db(tfs_pycds_sesh):
    insert_crmp_data(tfs_pycds_sesh)
    q = tfs_pycds_sesh.query(History)
    assert q.count() > 0
