from pytest import fixture
from ..helpers import create_then_drop_views, insert_crmp_data
from pycds.views import \
    CrmpNetworkGeoserver, HistoryStationNetwork, ObsCountPerDayHistory, \
    ObsWithFlags


@fixture
def tfs_pycds_sesh_with_large_data(tfs_pycds_sesh):
    insert_crmp_data(tfs_pycds_sesh)
    yield tfs_pycds_sesh


all_views = [
    CrmpNetworkGeoserver,
    HistoryStationNetwork,
    ObsCountPerDayHistory,
    ObsWithFlags
]


@fixture
def views_sesh(tfs_pycds_sesh_with_large_data):
    for s in create_then_drop_views(tfs_pycds_sesh_with_large_data, all_views):
        yield s
