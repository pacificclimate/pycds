from pytest import mark


@mark.usefixtures("new_db_left")
@mark.parametrize(
    "fn_name, argument, expected",
    [
        ("hxtk_hx_table_name", "spoo", "spoo_hx"),
        ("hxtk_hx_id_name", "spoo", "spoo_hx_id"),
        ("hxtk_collection_name_from_hx", "spoo_hx", "spoo"),
    ],
)
def test_function(
    fn_name, argument, expected, schema_name, sesh_in_prepared_schema_left
):
    result = sesh_in_prepared_schema_left.execute(
        f"SELECT {schema_name}.{fn_name}(:argument) AS value", {"argument": argument}
    ).scalar()
    assert result == expected
