import pytest
import sqlalchemy
from sqlalchemy import inspect
from datetime import datetime
from pycds.orm.native_matviews import VarsPerHistory


@pytest.mark.usefixtures("new_db_left")
def test_matview_content(sesh_with_large_data_rw):
    """Test that VarsPerHistory definition is correct."""

    # No content before matview is refreshed
    q = sesh_with_large_data_rw.query(VarsPerHistory)
    assert q.count() == 0

    # Refresh
    sesh_with_large_data_rw.execute(VarsPerHistory.refresh())

    # Et voila
    assert q.count() > 0

    # This test sucks, relying as it does on hardcoded magic numbers taken from
    # the large dataset. But it's what we've got just now.
    expected_pairs = {
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
    }
    result_pairs = {(row.history_id, row.vars_id) for row in q.all()}
    assert expected_pairs <= result_pairs


# test start and stop times on newest revision
@pytest.mark.usefixtures("new_db_left")
def test_matview_dates(sesh_with_large_data_rw):
    q = sesh_with_large_data_rw.query(VarsPerHistory)
    assert q.count() == 0

    sesh_with_large_data_rw.execute(VarsPerHistory.refresh())
    assert q.count() > 0

    expected_timestamps = {
        (6116, 526, datetime(1969, 6, 29, 0, 0), datetime(1970, 9, 28, 0, 0)),
        (8216, 526, datetime(1975, 8, 5, 0, 0), datetime(1975, 8, 5, 0, 0)),
        (6716, 528, datetime(1969, 12, 29, 0, 0), datetime(1971, 1, 29, 0, 0)),
        (8516, 544, datetime(2012, 9, 16, 6, 0), datetime(2012, 9, 16, 6, 0)),
        (
            1716,
            559,
            datetime(2000, 1, 31, 23, 59, 59),
            datetime(2000, 12, 31, 23, 59, 59),
        ),
        (
            1816,
            556,
            datetime(2000, 1, 31, 23, 59, 59),
            datetime(2000, 12, 31, 23, 59, 59),
        ),
        (3416, 437, datetime(2015, 9, 24, 10, 0), datetime(2015, 9, 24, 16, 0)),
        (8316, 552, datetime(2013, 8, 22, 4, 0), datetime(2013, 8, 22, 10, 0)),
        (1916, 494, datetime(2006, 9, 15, 0, 0), datetime(2006, 11, 3, 0, 0)),
        (
            1616,
            558,
            datetime(2000, 1, 31, 23, 59, 59),
            datetime(2000, 12, 31, 23, 59, 59),
        ),
        (6316, 527, datetime(1977, 7, 6, 0, 0), datetime(1977, 10, 19, 0, 0)),
        (3616, 434, datetime(2015, 9, 9, 3, 0), datetime(2015, 9, 24, 16, 0)),
    }

    result_timestamps = {
        (row.history_id, row.vars_id, row.start_time, row.end_time) for row in q.all()
    }

    assert expected_timestamps <= result_timestamps


@pytest.mark.usefixtures("new_db_left")
def test_index(schema_name, prepared_schema_from_migrations_left):
    """Test that VarsPerHistory has the expected index."""
    engine = prepared_schema_from_migrations_left
    inspector = sqlalchemy.inspect(engine)
    indexes = inspector.get_indexes(
        table_name=(VarsPerHistory.base_name()), schema=schema_name
    )
    assert indexes == [
        {
            "name": "var_hist_idx",
            "column_names": ["history_id", "vars_id"],
            "unique": False,
            "include_columns": [],
            "dialect_options": {"postgresql_include": []},
        }
    ]
