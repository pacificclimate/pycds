import pytest
import sqlalchemy
from sqlalchemy import inspect
from datetime import datetime
from pycds.orm.native_matviews import VarsPerHistory


@pytest.mark.usefixtures("new_db_left")
def test_matview_content(sesh_with_large_data):
    """Test that VarsPerHistory definition is correct."""

    # No content before matview is refreshed
    q = sesh_with_large_data.query(VarsPerHistory)
    assert q.count() == 0

    # Refresh
    sesh_with_large_data.execute(VarsPerHistory.refresh())

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


#test start and stop times on newest revision
@pytest.mark.usefixtures("new_db_left")
# try explicitly set the migration.
@pytest.mark.parametrize(
    "prepared_schema_from_migrations_left",
    ("3505750d3416",),
    indirect=True,
    )
def test_matview_dates(sesh_with_large_data):
    #version = sesh_with_large_data.execute("SELECT * FROM crmp.alembic_version")
    #print(version.one())
    # prints 3505750d3416, which is correct
    
    q = sesh_with_large_data.query(VarsPerHistory)
    assert q.count() == 0
    
    sesh_with_large_data.execute(VarsPerHistory.refresh())
    assert q.count() > 0
    
    #raw = sesh_with_large_data.execute("SELECT * FROM crmp.vars_per_history_mv")
    #print(raw.all()[0])
    #includes a start_time and end_time, which is correct

    
    expected_timestamps = {
        (8316, 555, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (2716, 497, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (5716, 526, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (1816, 556, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (1616, 556, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (7716, 526, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (5916, 526, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (8016, 528, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (3516, 562, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (7816, 527, datetime(1980, 1, 1), datetime(1980, 2, 1)),
        (5816, 519, datetime(1980, 1, 1), datetime(1980, 2, 1)),
    }
    
    #error here - start_time does not exist - why??
    result_timestamps = {(row.history_id, row.vars_id, row.start_time, row.end_time) for row in q.all()}
    
    assert expected_timestamps <= result_timestamps


@pytest.mark.usefixtures("new_db_left")
def test_index(schema_name, prepared_schema_from_migrations_left):
    """Test that VarsPerHistory has the expected index."""
    engine, script = prepared_schema_from_migrations_left
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
