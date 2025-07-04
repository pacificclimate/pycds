import pytest
from sqlalchemy import func, text

from pycds import Obs, History
from pycds.orm.views import HistoryStationNetwork, ObsCountPerDayHistory, ObsWithFlags


@pytest.mark.usefixtures("new_db_left")
def test_history_station_network(sesh_with_large_data_rw):
    hsn_q = sesh_with_large_data_rw.query(HistoryStationNetwork).order_by(
        HistoryStationNetwork.history_id
    )
    hx_q = sesh_with_large_data_rw.query(History).order_by(History.id)

    assert hsn_q.count() == hx_q.count()

    for hsn, hx in zip(hsn_q.all(), hx_q.all()):
        assert hsn.history_id == hx.id
        assert hsn.station_id == hx.station.id
        assert hsn.network_id == hx.station.network.id


@pytest.mark.usefixtures("new_db_left")
def test_obs_count_per_day_history(sesh_with_large_data_rw):
    ocdh_count_over_hx_q = (
        sesh_with_large_data_rw.query(
            ObsCountPerDayHistory.history_id.label("history_id"),
            func.sum(ObsCountPerDayHistory.count).label("count"),
        )
        .select_from(ObsCountPerDayHistory)
        .group_by(ObsCountPerDayHistory.history_id)
        .order_by(ObsCountPerDayHistory.history_id)
    )

    obs_count_over_hx_q = (
        sesh_with_large_data_rw.query(
            Obs.history_id.label("history_id"),
            func.count(Obs.id).label("count"),
        )
        .select_from(Obs)
        .group_by(Obs.history_id)
        .order_by(Obs.history_id)
    )

    assert ocdh_count_over_hx_q.count() == obs_count_over_hx_q.count()

    for ocdh_count, obs_count in zip(
        ocdh_count_over_hx_q.all(), obs_count_over_hx_q.all()
    ):
        assert ocdh_count.history_id == obs_count.history_id
        assert ocdh_count.count == obs_count.count


@pytest.mark.usefixtures("new_db_left")
def test_obs_with_flags(sesh_with_large_data_rw):
    obs_with_flags_q = sesh_with_large_data_rw.query(ObsWithFlags).order_by(
        ObsWithFlags.obs_raw_id
    )
    obs_q = sesh_with_large_data_rw.query(Obs).order_by(Obs.id)

    assert obs_with_flags_q.count() == obs_q.count()

    for obs_with_flags, obs in zip(obs_with_flags_q.all(), obs_q.all()):
        assert obs_with_flags.obs_raw_id == obs.id
        assert obs_with_flags.vars_id == obs.vars_id
        assert obs_with_flags.network_id == obs.variable.network_id
        assert obs_with_flags.station_id == obs.history.station_id
