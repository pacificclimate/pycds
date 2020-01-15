from pytest import fixture

from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation


@fixture
def daily_views():
    return [
        DailyMaxTemperature,
        DailyMinTemperature,
    ]


@fixture
def monthly_views():
    return [
        MonthlyAverageOfDailyMaxTemperature,
        MonthlyAverageOfDailyMinTemperature,
        MonthlyTotalPrecipitation
    ]


@fixture
def all_views(daily_views, monthly_views):
    return daily_views + monthly_views


@fixture
def refresh_views():
    def f(views, sesh):
        for view in views:
            view.refresh(sesh)
    return f
