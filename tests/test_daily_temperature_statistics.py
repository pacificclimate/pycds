import datetime
import pytest
import pycds
from pycds import Network, Station, History, Variable, Obs
from pycds import DailyMaxTemperature

@pytest.fixture(scope='function')
def minimal_session(blank_postgis_session):
    sesh = blank_postgis_session
    engine = sesh.get_bind()
    pycds.Base.metadata.create_all(bind=engine)

    network1 = Network(id=1, name='Network 1')
    networks = [network1]

    station1 = Station(id=1, network_id=network1.id)
    stations = [station1]

    history1 = History(id=1, station_id=station1.id, station_name='Station 1',
                       sdate=datetime.datetime.min, edate=None, freq='daily')
    histories = [history1]

    variable1 = Variable(id=1, network_id=network1.id,
                         standard_name='air_temperature', cell_method='time: maximum')
    variables = [variable1]

    obs1 = Obs(id=1, vars_id=variable1.id, history_id=history1.id,
               time=datetime.datetime(2000, 1, 1, 12, 34, 56), datum=5.0)
    observations = [obs1]

    sesh.add_all(networks)
    sesh.flush()
    sesh.add_all(stations)
    sesh.add_all(variables)
    sesh.flush()
    sesh.add_all(histories)
    sesh.flush()
    sesh.add_all(observations)
    sesh.flush()

    return sesh


def test_minimal_session(minimal_session):
    sesh = minimal_session

    networks = sesh.query(Network)
    assert(networks.count() == 1)

    stations = sesh.query(Station)
    assert(stations.count() == 1)

    histories = sesh.query(History)
    assert(histories.count() == 1)

    variables = sesh.query(Variable)
    assert(variables.count() == 1)

    observations = sesh.query(Obs)
    assert(observations.count() == 1)


class TestDailyMaxTemperature:

    def test_minimal(self, minimal_session):
        sesh = minimal_session

        temps = sesh.query(DailyMaxTemperature)
        assert(temps.count() == 1)

        temp = temps.first()
        assert(temp.station_id == 1)
        assert(temp.vars_id == 1)
        assert(temp.obs_day == datetime.datetime(2000, 1, 1))
        assert(temp.statistic == 5.0)
        assert(temp.data_coverage == 1.0)
