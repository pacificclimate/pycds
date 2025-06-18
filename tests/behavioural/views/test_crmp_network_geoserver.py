import pytest

from sqlalchemy import func, text

from pycds import VarsPerHistory
from pycds.orm.native_matviews import StationObservationStats, CollapsedVariables
from pycds.orm.views import CrmpNetworkGeoserver


@pytest.mark.usefixtures("new_db_left")
def test_basic_content(sesh_with_large_data_rw):
    sesh_with_large_data_rw.execute(StationObservationStats.refresh())
    q = sesh_with_large_data_rw.query(CrmpNetworkGeoserver.network_name)
    rv = q.all()

    # Test that the number of rows is not zero
    num_all_rows = len(rv)
    assert num_all_rows != 0

    # Select all rows from network FLNRO-WMB
    where_clause = text("network_name = 'FLNRO-WMB'")
    q = q.filter(where_clause)
    rv = q.all()

    # Assert that number of rows is less
    num_filtered_rows = len(rv)
    assert num_filtered_rows < num_all_rows
    num_all_rows = num_filtered_rows

    # Select all rows where max_obs_time is before 2005
    where_clause = text("max_obs_time < '2005-01-01'")

    # Assert that number of rows is less
    rv = q.filter(where_clause).all()
    num_filtered_rows = len(rv)
    assert num_filtered_rows != 0
    assert num_filtered_rows < num_all_rows


@pytest.mark.usefixtures("new_db_left")
def test_collapsed_vars_content(sesh_with_large_data_rw):
    """Test that data from CollapsedVariables is present in CrmpNetworkGeoserver"""

    # Refresh contributing matviews
    sesh_with_large_data_rw.execute(VarsPerHistory.refresh())
    sesh_with_large_data_rw.execute(CollapsedVariables.refresh())
    sesh_with_large_data_rw.execute(StationObservationStats.refresh())

    num_cng_rows = sesh_with_large_data_rw.query(CrmpNetworkGeoserver).count()
    assert num_cng_rows > 0
    num_cv_rows = sesh_with_large_data_rw.query(CollapsedVariables).count()
    assert num_cv_rows > 0
    assert num_cng_rows == num_cng_rows

    q = (
        sesh_with_large_data_rw.query(CrmpNetworkGeoserver, CollapsedVariables)
        .select_from(CrmpNetworkGeoserver)
        .join(
            CollapsedVariables,
            CrmpNetworkGeoserver.history_id == CollapsedVariables.history_id,
        )
    )
    results = q.all()
    for row in results:
        assert row.CrmpNetworkGeoserver.vars_ids == row.CollapsedVariables.vars_ids
        assert (
            row.CrmpNetworkGeoserver.unique_variable_tags
            == row.CollapsedVariables.unique_variable_tags
        )
        assert row.CrmpNetworkGeoserver.vars == row.CollapsedVariables.vars
        assert (
            row.CrmpNetworkGeoserver.display_names
            == row.CollapsedVariables.display_names
        )
