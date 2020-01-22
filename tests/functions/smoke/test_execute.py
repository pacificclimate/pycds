"""Smoke tests that the functions are executable. This doesn't test function
correctness, just that they don't explode on contact."""
from _datetime import datetime, date
from pytest import mark, param


@mark.parametrize('func, args', [
    ('closest_stns_within_threshold', (-120.0, 50.0, 1)),
    param(
        'daily_ts', (1, 1, 50.0),
        marks=mark.xfail(reason='Bug in function definition')
    ),
    ('daysinmonth', (datetime(2000, 3, 4),)),
    param(
        'do_query_one_station', (999,),
        marks=mark.xfail(reason='No data in test database')
    ),
    ('effective_day', (datetime(2000, 1, 1, 7, 39), 'max', '1-hourly')),
    ('getstationvariabletable', (999, False)),
    ('lastdateofmonth', (date(2000, 3, 4),)),
    param(
        'monthly_ts', (1, 1, 50.0),
        marks=mark.xfail(reason='Bug in function definition')
    ),
    ('query_one_station', (999,),),
    ('query_one_station_climo', (999,),),
    ('season', (datetime(2000, 3, 4),)),
    ('updatesdateedate', ()),
])
def test_execute(functions_sesh, schema_func, func, args):
    """Test that a function invoked with the given arguments doesn't raise
    an exception. This proves nothing much except that (part of) the function
    is executable.
    """
    fn = getattr(schema_func, func)
    q = functions_sesh.query(fn(*args))
    q.all()
