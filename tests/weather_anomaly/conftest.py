"""
Database objects for tests of weather anomaly views.

To maintain database consistency, objects must be added (and flushed) in this
order:
  Network
  Station, History
  Variable
  Observation

This imposes an order on the definition of session fixtures, and on the
nesting of describe blocks that use them.
"""

import datetime
from pytest import fixture

from pycds import Contact, Network, Station, History, Variable, Obs, \
    NativeFlag, PCICFlag


@fixture
def network1():
    return Network(name='Network 1')


@fixture
def network2():
    return Network(name='Network 2')


@fixture
def station1(network1):
    return Station(network=network1)


@fixture
def station2(network2):
    return Station(network=network2)

history_transition_date = datetime.datetime(2010, 1, 1)


@fixture
def history_stn1_hourly(station1):
    return History(station=station1, station_name='Station 1',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')


@fixture
def history_stn1_12_hourly(station1):
    return History(station=station1, station_name='Station 1',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='12-hourly')


@fixture
def history_stn1_daily(station1):
    return History(station=station1, station_name='Station 1',
                   sdate=history_transition_date, edate=None, freq='daily')


@fixture
def history_stn2_hourly(station2):
    return History(station=station2, station_name='Station 2',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')


@fixture
def var_temp_point(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: point')


@fixture
def var_temp_point2(network2):
    return Variable(network=network2, standard_name='air_temperature', cell_method='time: point')


@fixture
def var_temp_max(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: maximum')


@fixture
def var_temp_min(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: minimum')


@fixture
def var_temp_mean(network1):
    return Variable(network=network1, standard_name='air_temperature', cell_method='time: mean')


@fixture
def var_foo(network1):
    return Variable(network=network1, standard_name='foo', cell_method='time: point')


@fixture
def var_precip_net1_1(network1):
    return Variable(network=network1, standard_name='thickness_of_rainfall_amount', cell_method='time: sum')


@fixture
def var_precip_net1_2(network1):
    return Variable(network=network1, standard_name='thickness_of_rainfall_amount', cell_method='time: sum')


@fixture
def var_precip_net2_1(network2):
    return Variable(network=network2, standard_name='thickness_of_rainfall_amount', cell_method='time: sum')


@fixture
def native_flag_discard():
    return NativeFlag(discard=True)


@fixture
def native_flag_non_discard():
    return NativeFlag(discard=False)


@fixture
def pcic_flag_discard():
    return PCICFlag(discard=True)


@fixture
def pcic_flag_non_discard():
    return PCICFlag(discard=False)
