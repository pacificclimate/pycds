import pytest
import sqlalchemy
from pycds import VarsPerHistory, Variable, Obs
from pycds.orm.native_matviews import CollapsedVariables


@pytest.mark.usefixtures("new_db_left")
def test_matview_content(sesh_with_large_data):
    """Test that CollapsedVariables definition is correct."""

    q = sesh_with_large_data.query(CollapsedVariables)

    # No content before matview is refreshed
    assert q.count() == 0

    # Refresh contributing matview and this one
    sesh_with_large_data.execute(VarsPerHistory.refresh())
    sesh_with_large_data.execute(CollapsedVariables.refresh())

    # Et voila
    assert q.count() > 0

    # Test content

    result = q.all()

    for row in result:
        # We'll compare the content of the matview to the results of a query for the
        # relevant Variables without using VarsPerHistory as an intermediary.
        relevant_variables = (
            sesh_with_large_data.query(Variable)
            .join(Obs, Obs.vars_id == Variable.id)
            .where(Obs.history_id == row.history_id)
        ).all()

        var_names = row.vars.split(", ")
        assert len(var_names) > 0
        assert all(len(name) > 0 for name in var_names)

        # The *intent* of this matview's query is probably expressed by the following
        # assertion, but it's likely the query for this view/matview is wrong.
        # See https://github.com/pacificclimate/pycds/issues/180
        # assert all(re.fullmatch(r"\w+", name) for name in var_names)

        relevant_var_values = {
            v.standard_name + v.cell_method.replace("time: ", "_")
            for v in relevant_variables
        }
        for name in var_names:
            assert name in relevant_var_values

        display_names = {name for name in row.display_names.split("|") if name}
        assert len(display_names) > 0
        assert all(len(name) > 0 for name in display_names)
        relevant_display_names = {v.display_name for v in relevant_variables}
        assert all(name in relevant_display_names for name in display_names)


@pytest.mark.usefixtures("new_db_left")
def test_index(schema_name, prepared_schema_from_migrations_left):
    """Test that CollapsedVariables has the expected index."""
    engine, script = prepared_schema_from_migrations_left
    inspector = sqlalchemy.inspect(engine)
    indexes = inspector.get_indexes(
        table_name=(CollapsedVariables.base_name()), schema=schema_name
    )
    assert indexes == [
        {
            "name": "collapsed_vars_idx",
            "column_names": ["history_id"],
            "unique": False,
            "include_columns": [],
            "dialect_options": {"postgresql_include": []},
        }
    ]
