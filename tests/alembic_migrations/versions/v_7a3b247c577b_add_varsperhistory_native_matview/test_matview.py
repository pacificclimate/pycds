import pytest
from pycds.materialized_views.version_7a3b247c577b import VarsPerHistory


@pytest.mark.usefixtures('new_db_left')
def test_vars_per_history(sesh_with_large_data):
    # No content before matview is refreshed
    q = sesh_with_large_data.query(VarsPerHistory)
    assert q.count() == 0

    # Refresh
    VarsPerHistory.refresh(sesh_with_large_data)

    # Et voila
    assert q.count() > 0

    # This test sucks, relying as it does on hardcoded magic numbers taken from
    # the large dataset. But it's what we've got just now.
    pairs = tuple((row.history_id, row.vars_id) for row in q.all())
    for pair in (
        (8316, 555),
        (2716, 497),
        (5716, 526),
        (1816, 556),
        (1616, 556),
        (7716, 526),
        (5916, 526),
        (8016, 528),
        (3516, 562),
        (7816, 527),
        (5816, 519),
    ):
        assert pair in pairs
