from pytest import fixture

from pycds.orm.manual_matviews import (
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
    MonthlyTotalPrecipitation,
)


# All tests in this directory need fixture `new_db_left`
@fixture(autouse=True)
def autouse_new_db_left(new_db_left):
    pass


@fixture
def daily_views():
    return [DailyMaxTemperature, DailyMinTemperature]


@fixture
def monthly_views():
    return [
        MonthlyAverageOfDailyMaxTemperature,
        MonthlyAverageOfDailyMinTemperature,
        MonthlyTotalPrecipitation,
    ]


@fixture
def all_views(daily_views, monthly_views):
    return daily_views + monthly_views


@fixture
def refresh_views():
    def f(views, sesh):
        for view in views:
            sesh.execute(view.refresh())

    return f
