from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pycds.util import create_test_database, create_test_data, insert_crmp_data
from pycds import Contact, History, Obs


def set_and_verify_search_path(search_path, executor):
    search_path_str = ', '.join(search_path)
    executor.execute('SET search_path TO {}'.format(search_path_str))
    show = executor.execute('SHOW search_path').scalar()
    if show != search_path_str:
        raise ValueError(
            "search_path not set to expected value: expected '{}', got '{}'"
                .format(search_path_str, show)
        )

def test_reflect_tables_into_session(blank_postgis_session, schema_name):
    engine = blank_postgis_session.get_bind()
    set_and_verify_search_path([schema_name, 'public'], blank_postgis_session)
    # print('### xxx', engine.execute('SET search_path TO foo, public'.format(schema_name)))
    # # print('### xxx', engine.execute('SET search_path TO {}, public'.format(schema_name)))
    # print('### test_reflect_tables_into_session', engine.execute("show search_path").scalar())
    create_test_database(engine)

    res = blank_postgis_session.execute('''
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{}';
    '''.format(schema_name))

    assert {x[0] for x in res.fetchall()} >= {
        'meta_sensor', 'meta_contact', 'climo_obs_count_mv',
        'obs_count_per_month_history_mv',
        'vars_per_history_mv', 'meta_history',
        'meta_vars', 'meta_network', 'meta_station',
        'obs_raw', 'meta_native_flag', 'obs_raw_native_flags'
    }


def test_can_create_test_db(blank_postgis_session, schema_name):
    engine = blank_postgis_session.get_bind()
    engine.execute('SET search_path TO {}, public'.format(schema_name))
    create_test_database(engine)
    create_test_data(engine)
    # Get some data
    q = blank_postgis_session.query(Contact)
    assert len(q.all()) == 2

def test_can_create_crmp_subset_db(blank_postgis_session, schema_name):
    engine = blank_postgis_session.get_bind()
    engine.execute('SET search_path TO {}, public'.format(schema_name))
    create_test_database(engine)
    insert_crmp_data(blank_postgis_session)

    q = blank_postgis_session.query(History)
    assert q.count() > 0
