from pytest import mark
from sqlalchemy.sql import text

from pycds.alembic.change_history_utils import hx_table_name, hx_id_name


@mark.usefixtures("new_db_left")
@mark.parametrize(
    "db_fn_name, py_fn, argument, expected",
    [
        ("hxtk_hx_table_name", hx_table_name, "spoo", "spoo_hx"),
        ("hxtk_hx_id_name", hx_id_name, "spoo", "spoo_hx_id"),
        ("hxtk_collection_name_from_hx", None, "spoo_hx", "spoo"),
    ],
)
def test_function(
    db_fn_name, py_fn, argument, expected, schema_name, sesh_in_prepared_schema_left
):
    # Test db function
    result = sesh_in_prepared_schema_left.execute(
        text(f"SELECT {schema_name}.{db_fn_name}(:argument) AS value"),
        {"argument": argument},
    ).scalar()
    assert result == expected

    # Test local util function
    if py_fn is not None:
        assert py_fn(argument, schema=None) == expected
