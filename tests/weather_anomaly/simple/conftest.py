import datetime

import pytest
from pytest import fixture

from pycds import Network, Station, History, Variable, Obs
from ...helpers import generic_sesh
from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation



@fixture
def views():
    return (
        DailyMaxTemperature,
        DailyMinTemperature,
        MonthlyAverageOfDailyMaxTemperature,
        MonthlyAverageOfDailyMinTemperature,
        MonthlyTotalPrecipitation,
    )


@fixture
def views_sesh(pycds_sesh, views):
    for view in views:
        view.create(pycds_sesh)
    yield pycds_sesh
    for view in reversed(views):
        view.drop(pycds_sesh)


@fixture
def network1_sesh(views_sesh, network1):
    for sesh in generic_sesh(views_sesh , [network1]):
        yield sesh


@fixture
def station1_sesh(network1_sesh, station1):
    for sesh in generic_sesh(network1_sesh , [station1]):
        yield sesh


@fixture
def history1_sesh(station1_sesh, history_stn1_hourly):
    for sesh in generic_sesh(station1_sesh, [history_stn1_hourly]):
        yield sesh


@fixture
def variable1_sesh(history1_sesh, var_temp_point):
    for sesh in generic_sesh(history1_sesh , [var_temp_point]):
        yield sesh


@fixture
def obs1_months():
    return (1, 6, 8, 12)


@fixture
def obs1_days():
    # TODO: Does this need to be converted to a tuple? Factory, though.
    return range(1, 29, 2)


@fixture
def obs1_hours():
    # TODO: Does this need to be converted to a tuple? Factory, though.
    return range(1, 24, 2)


# @fixture
# def obs1_sesh(
#         request, variable_sesh,
#         var_temp_point, var_precip_net1_1, history_stn1_hourly
# ):
#     """Yield a session with particular observations added to variable1_sesh.
#     Observations added depend on the value of request.param:
#     MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, or MonthlyTotalPrecipitation.
#     This fixture is used as an indirect fixture for parametrized tests.
#     """
#     if request.param in [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature]:
#         observations = [Obs(variable=var_temp_point, history=history_stn1_hourly,
#                             time=datetime.datetime(2000, 1, day, hour), datum=float(hour))
#                         for day in days for hour in hours]
#     else:
#         observations = [Obs(variable=var_precip_net1_1, history=history_stn1_hourly,
#                             time=datetime.datetime(2000, 1, day, hour), datum=1.0)
#                         for day in days for hour in hours]
#     for sesh in generic_sesh(variable_sesh , observations):
#         yield sesh


@fixture
def obs1_temp_sesh(
        variable1_sesh,
        obs1_months, obs1_days, obs1_hours,
        var_temp_point, history_stn1_hourly
):
    """Yield a session with particular observations added to variable1_sesh.
    """
    observations = [
        Obs(variable=var_temp_point, history=history_stn1_hourly,
            time=datetime.datetime(2000, month, day, hour), datum=float(hour))
        for month in obs1_months for day in obs1_days for hour in obs1_hours
    ]
    for sesh in generic_sesh(variable1_sesh , observations):
        yield sesh


@fixture
def obs1_precip_sesh(
        variable1_sesh,
        obs1_months, obs1_days, obs1_hours,
        var_precip_net1_1, history_stn1_hourly
):
    """Yield a session with particular observations added to variable1_sesh.
    """
    observations = [
        Obs(variable=var_precip_net1_1, history=history_stn1_hourly,
            time=datetime.datetime(2000, month, day, hour), datum=1.0)
        for month in obs1_months for day in obs1_days for hour in obs1_hours
    ]
    for sesh in generic_sesh(variable1_sesh , observations):
        yield sesh




