"""Basic tests of functions used by WA"""
from ...helpers import get_schema_item_names


def test_functions_exist(schema_name, views_sesh):
    names = get_schema_item_names(
        views_sesh, 'routines', schema_name=schema_name
    )
    assert names >= {'daysinmonth', 'effective_day'}
