from ..helpers import insert_test_data, insert_crmp_data
from pycds.database import get_schema_item_names
from pycds import Contact, History, Obs


def test_schema_content(pycds_sesh):
    assert get_schema_item_names(pycds_sesh, "tables") >= {
        "meta_sensor",
        "meta_contact",
        "climo_obs_count_mv",
        "obs_count_per_month_history_mv",
        "meta_history",
        "meta_vars",
        "meta_network",
        "meta_station",
        "obs_raw",
        "meta_native_flag",
        "obs_raw_native_flags",
    }


def test_can_create_test_db(pycds_sesh):
    insert_test_data(pycds_sesh)
    q = pycds_sesh.query(Contact)
    assert len(q.all()) == 2


def test_can_create_crmp_subset_db(pycds_sesh):
    insert_crmp_data(pycds_sesh)
    q = pycds_sesh.query(History)
    assert q.count() > 0
