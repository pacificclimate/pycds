import pytest
import sqlalchemy
from pycds.orm.native_matviews import ClimoObsCount


@pytest.mark.usefixtures("new_db_left")
def test_matview_content(sesh_with_large_data_rw):
    """Test that ClimoObsCount definition is correct."""

    # No content before matview is refreshed
    q = sesh_with_large_data_rw.query(ClimoObsCount)
    assert q.count() == 0

    # Refresh
    sesh_with_large_data_rw.execute(ClimoObsCount.refresh())

    # Et voila
    assert q.count() > 0

    # This test sucks, relying as it does on hardcoded magic numbers taken from
    # the large dataset. But it's what we've got just now.
    expected_pairs = {
        (48, 816),
        (48, 1516),
        (2, 2516),
        (48, 1216),
        (48, 1016),
        (48, 1616),
        (12, 516),
        (36, 1816),
        (48, 1716),
        (48, 1316),
        (21, 3516),
        (5, 2016),
    }
    result_pairs = {(row.count, row.history_id) for row in q.all()}
    assert expected_pairs <= result_pairs
    for pair in expected_pairs:
        assert pair in result_pairs


@pytest.mark.usefixtures("new_db_left")
def test_index(schema_name, prepared_schema_from_migrations_left):
    """Test that ClimoObsCount has the expected index."""
    engine = prepared_schema_from_migrations_left
    inspector = sqlalchemy.inspect(engine)
    indexes = inspector.get_indexes(
        table_name=(ClimoObsCount.base_name()), schema=schema_name
    )
    assert indexes == [
        {
            "name": "climo_obs_count_idx",
            "column_names": ["history_id"],
            "unique": False,
            "include_columns": [],
            "dialect_options": {"postgresql_include": []},
        }
    ]
