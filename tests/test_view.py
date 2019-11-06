from sqlalchemy import text

from pycds import Obs
from pycds.views import CrmpNetworkGeoserver, ObsWithFlags


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


def test_obs_with_flags(large_test_session):
    # extensions = (
    #     large_test_session.execute('''
    #         SELECT * FROM pg_extension
    #     ''')
    # )
    # print('### extensions', {
    #     '{}: {}'.format(e.extname, e.extversion) for e in extensions
    # })

    obs_with_flags_q = (
        large_test_session.query(ObsWithFlags)
            .order_by(ObsWithFlags.obs_raw_id)
    )
    obs_q = (
        large_test_session.query(Obs)
            .order_by(Obs.id)
    )

    assert obs_with_flags_q.count() == obs_q.count()

    for obs_with_flags, obs in zip(obs_with_flags_q.all(), obs_q.all()):
        assert obs_with_flags.obs_raw_id == obs.id
        assert obs_with_flags.vars_id == obs.vars_id
        assert obs_with_flags.network_id == obs.variable.network_id
        # FIXME: The following results in an error:
        #  Error "function st_asewkb(public.geometry) does not exist"
        #  PostGIS is installed, but its functions are not available.
        # assert obs_with_flags.station_id == obs.history.station_id
