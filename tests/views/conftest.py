from pkg_resources import resource_filename
from pytest import fixture
from pycds.views import \
    CrmpNetworkGeoserver, HistoryStationNetwork, ObsCountPerDayHistory, \
    ObsWithFlags


@fixture
def tfs_pycds_sesh_with_large_data(tfs_pycds_sesh):
    with open(resource_filename('pycds', 'data/crmp_subset_data.sql'), 'r') \
            as f:
        sql = f.read()
    tfs_pycds_sesh.execute(sql)
    yield tfs_pycds_sesh


all_views = [
    CrmpNetworkGeoserver,
    HistoryStationNetwork,
    ObsCountPerDayHistory,
    ObsWithFlags
]


@fixture
def views_sesh(tfs_pycds_sesh_with_large_data):
    sesh = tfs_pycds_sesh_with_large_data
    for view in all_views:
        view.create(sesh)
    yield sesh
    for view in reversed(all_views):
        view.drop(sesh)
