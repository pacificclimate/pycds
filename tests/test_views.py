from sqlalchemy import func, text

from pycds import Obs, History
from pycds.views import \
    CrmpNetworkGeoserver, HistoryStationNetwork, ObsCountPerDayHistory, \
    ObsWithFlags


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


def test_history_station_network(large_test_session):
    hsn_q = (
        large_test_session.query(HistoryStationNetwork)
        .order_by(HistoryStationNetwork.history_id)
    )
    hx_q = (
        large_test_session.query(History)
        .order_by(History.id)
    )

    assert hsn_q.count() == hx_q.count()

    for hsn, hx in zip(hsn_q.all(), hx_q.all()):
        assert hsn.history_id == hx.id
        assert hsn.station_id == hx.station.id
        assert hsn.network_id == hx.station.network.id


def test_obs_count_per_day_history(large_test_session):
    ocdh_count_over_hx_q = (
        large_test_session.query(
            ObsCountPerDayHistory.history_id.label('history_id'),
            func.sum(ObsCountPerDayHistory.count).label('count')
        )
        .select_from(ObsCountPerDayHistory)
        .group_by(ObsCountPerDayHistory.history_id)
        .order_by(ObsCountPerDayHistory.history_id)
    )

    obs_count_over_hx_q = (
        large_test_session.query(
            Obs.history_id.label('history_id'),
            func.count(Obs.id).label('count')
        )
        .select_from(Obs)
        .group_by(Obs.history_id)
        .order_by(Obs.history_id)
    )

    assert ocdh_count_over_hx_q.count() == obs_count_over_hx_q.count()

    for ocdh_count, obs_count in \
            zip(ocdh_count_over_hx_q.all(), obs_count_over_hx_q.all()):
        assert ocdh_count.history_id == obs_count.history_id
        assert ocdh_count.count == obs_count.count


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
        assert obs_with_flags.station_id == obs.history.station_id
