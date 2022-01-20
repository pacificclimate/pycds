import pytest
from sqlalchemy.engine.reflection import Inspector
from pycds.orm.native_matviews import VarsPerHistory


@pytest.mark.usefixtures("new_db_left")
def test_vars_content(sesh_with_large_data):
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


@pytest.mark.usefixtures("new_db_left")
def test_index(schema_name, prepared_schema_from_migrations_left):
    """Test that VarsPerHistory has the expected index."""
    engine, script = prepared_schema_from_migrations_left
    inspector = Inspector(engine)
    viewname = VarsPerHistory.base_name()
    indexes = inspector.get_indexes(table_name=viewname, schema=schema_name)
    assert indexes == [
        {
            "name": "var_hist_idx",
            "column_names": ["history_id", "vars_id"],
            "unique": False,
        }
    ]
