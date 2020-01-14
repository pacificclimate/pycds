from pytest import fixture

from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation


@fixture
def all_views():
    return [
        DailyMaxTemperature,
        DailyMinTemperature,
        MonthlyAverageOfDailyMaxTemperature,
        MonthlyAverageOfDailyMinTemperature,
        MonthlyTotalPrecipitation
    ]


@fixture
def refresh_views(all_views):
    def f(sesh):
        for view in all_views:
            view.refresh(sesh)
    return f



