import datetime

from pytest import fixture

from .common import climatology_var_names

from pycds import Network, Station, History, Variable, DerivedValue


@fixture
def other_network():
    return Network(name="Other Network")


@fixture
def other_climatology_variables(other_network):
    return [
        Variable(
            name=name,
            standard_name="foo: bar",
            cell_method="foo_bar",
            display_name="foo bar",
            network=other_network,
        )
        for name in climatology_var_names
    ]


@fixture
def stations():
    stns = [Station(native_id=native_id) for native_id in "100 200".split()]
    return stns


@fixture
def histories(stations):
    return [
        History(
            station=station,
            station_name="Station {0}".format(station.native_id),
            sdate=datetime.datetime(year, 1, 1),
            edate=datetime.datetime(year + 1, 1, 1),
        )
        for station in stations
        for year in [2000, 2001]
    ]
