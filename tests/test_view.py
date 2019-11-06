from sqlalchemy import text

from pycds.views import CrmpNetworkGeoserver


def test_crmp_network_geoserver(large_test_session):
    q = large_test_session.query(CrmpNetworkGeoserver.network_name)
    rv = q.all()

    # Test that the number of rows is not zero
    nrows = len(rv)
    assert nrows != 0

    # Select all rows from network FLNRO-WMB
    where_clause = text("network_name = 'FLNRO-WMB'")
    q = q.filter(where_clause)
    rv = q.all()

    # Assert that number of rows is less
    filtered_nrows = len(rv)
    assert filtered_nrows < nrows
    nrows = filtered_nrows

    # Select all rows where max_obs_time is before 2005
    where_clause = text("max_obs_time < '2005-01-01'")

    # Assert that number of rows is less
    rv = q.filter(where_clause).all()
    filtered_nrows = len(rv)
    assert filtered_nrows != 0
    assert filtered_nrows < nrows
