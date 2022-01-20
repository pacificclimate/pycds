from _datetime import datetime, date
import pytest


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "func, args",
    [
        ("closest_stns_within_threshold", (-120.0, 50.0, 1)),
        pytest.param(
            "daily_ts",
            (1, 1, 50.0),
            marks=pytest.mark.xfail(reason="Bug in function definition"),
        ),
        ("daysinmonth", (datetime(2000, 3, 4),)),
        pytest.param(
            "do_query_one_station",
            (999,),
            marks=pytest.mark.xfail(reason="No data in test database"),
        ),
        ("effective_day", (datetime(2000, 1, 1, 7, 39), "max", "1-hourly")),
        ("getstationvariabletable", (999, False)),
        ("lastdateofmonth", (date(2000, 3, 4),)),
        pytest.param(
            "monthly_ts",
            (1, 1, 50.0),
            marks=pytest.mark.xfail(reason="Bug in function definition"),
        ),
        ("query_one_station", (999,)),
        ("query_one_station_climo", (999,)),
        ("season", (datetime(2000, 3, 4),)),
        ("updatesdateedate", ()),
    ],
)
def test_executable(func, args, sesh_in_prepared_schema_left, schema_func):
    """Test that a function invoked with the given arguments doesn't raise
    an exception. This proves nothing much except that (part of) the function
    is executable.
    """
    fn = getattr(schema_func, func)
    q = sesh_in_prepared_schema_left.query(fn(*args))
    q.all()
