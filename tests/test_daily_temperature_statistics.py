import datetime
import pytest
from pycds import Network, Station, History, Variable, Obs
from pycds import DailyMaxTemperature

# TODO: Remove print statements and other cruft

@pytest.fixture
def network1():
    return Network(id=1, name='Network 1')

@pytest.fixture
def network2():
    return Network(id=2, name='Network 2')

@pytest.fixture
def station1(network1):
    return Station(id=1, network_id=network1.id)

@pytest.fixture
def station2(network2):
    return Station(id=2, network_id=network2.id)

history_transition_date = datetime.datetime(2010, 1, 1)

@pytest.fixture
def history_stn1_hourly(station1):
    return History(id=1, station_id=station1.id, station_name='Station 1',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')

@pytest.fixture
def history_stn1_daily(station1):
    return History(id=2, station_id=station1.id, station_name='Station 1',
                   sdate=history_transition_date, edate=None, freq='daily')

@pytest.fixture
def history_stn2_hourly(station2):
    return History(id=3, station_id=station2.id, station_name='Station 2',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')

@pytest.fixture
def var_temp_point(network1): 
    return Variable(id=10, network_id=network1.id,
                    standard_name='air_temperature', cell_method='time: point')

@pytest.fixture
def var_temp_point2(network2):
    return Variable(id=11, network_id=network2.id,
                    standard_name='air_temperature', cell_method='time: point')

@pytest.fixture
def var_temp_max(network1):
    return Variable(id=20, network_id=network1.id,
                    standard_name='air_temperature', cell_method='time: maximum')

@pytest.fixture
def var_temp_min(network1):
    return Variable(id=30, network_id=network1.id,
                    standard_name='air_temperature', cell_method='time: minimum')

@pytest.fixture
def var_foo(network1):
    return Variable(id=40, network_id=network1.id,
                    standard_name='foo', cell_method='time: point')

@pytest.fixture
def vars_many(var_temp_point, var_temp_max, var_temp_min, var_foo):
    return [var_temp_point, var_temp_max, var_temp_min, var_foo]



# To maintain database consistency, objects must be added (and flushed) in this order:
#   Network
#   Station, History
#   Variable
#   Observation
#
# This imposes an order on the definition of fixtures, and on the nesting of describe blocks that use them.

def describe_DailyMaxTemperature():
    def describe_with_1_network():

        @pytest.fixture
        def network_sesh(mod_empty_database_session, network1):
            sesh = mod_empty_database_session
            sesh.add(network1)
            sesh.flush()
            yield sesh
            sesh.query(Network).delete()
            sesh.flush()

        def describe_with_1_station():

            @pytest.fixture
            def station_sesh(network_sesh, station1):
                sesh = network_sesh
                sesh.add(station1)
                sesh.flush()
                yield sesh
                sesh.query(Station).delete()
                sesh.flush()

            def describe_with_1_history_hourly():

                @pytest.fixture
                def history_sesh(station_sesh, history_stn1_hourly):
                    sesh = station_sesh
                    sesh.add(history_stn1_hourly)
                    sesh.flush()
                    yield sesh
                    sesh.query(History).delete()
                    sesh.flush()

                def describe_with_1_variable():

                    @pytest.fixture
                    def variable_sesh(history_sesh, var_temp_point):
                        sesh = history_sesh
                        sesh.add(var_temp_point)
                        sesh.flush()
                        yield sesh
                        sesh.query(Variable).delete()
                        sesh.flush()

                    def describe_with_many_observations_in_one_day():

                        @pytest.fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                            sesh = variable_sesh
                            sesh.add_all([
                                (Obs(id=1, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 1, 12), datum=1.0)),
                                (Obs(id=2, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 1, 13), datum=2.0)),
                                (Obs(id=3, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 1, 14), datum=3.0))
                            ])
                            sesh.flush()
                            yield sesh
                            sesh.query(Obs).delete()
                            sesh.flush()

                        @pytest.fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_a_single_row(results):
                            assert(results.count() == 1)

                        def it_returns_the_expected_station_variable_and_day(results, station1, var_temp_point):
                            result = results.first()
                            assert(result.station_id == station1.id)
                            assert(result.vars_id == var_temp_point.id)
                            assert(result.obs_day == datetime.datetime(2000, 1, 1))

                        def it_returns_the_expected_maximum_value(results):
                            assert(results.first().statistic == 3.0)

                        def it_returns_the_expected_data_coverage(results):
                            assert(float(results.first().data_coverage) == 3.0 / 24.0)

                    def describe_with_many_observations_on_two_different_days():

                        @pytest.fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                            sesh = variable_sesh
                            sesh.add_all([
                                (Obs(id=1, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 1, 12), datum=1.0)),
                                (Obs(id=2, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 1, 13), datum=2.0)),
                                (Obs(id=3, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 1, 14), datum=3.0)),
                                (Obs(id=4, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 2, 12), datum=4.0)),
                                (Obs(id=5, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 2, 13), datum=5.0)),
                                (Obs(id=6, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 2, 14), datum=6.0)),
                                (Obs(id=7, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                     time=datetime.datetime(2000, 1, 2, 15), datum=7.0))
                            ])
                            sesh.flush()
                            yield sesh
                            sesh.query(Obs).delete()
                            sesh.flush()

                        @pytest.fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature).order_by(DailyMaxTemperature.obs_day)

                        def it_returns_two_rows(results):
                            assert(results.count() == 2)

                        def it_returns_the_expected_station_variables(results, station1, var_temp_point):
                            for result in results:
                                assert(result.station_id == station1.id)
                            assert(result.vars_id == var_temp_point.id)

                        def it_returns_the_expected_days(results):
                            assert([r.obs_day for r in results] == [datetime.datetime(2000, 1, 1), datetime.datetime(2000, 1, 2)])

                        def it_returns_the_expected_max_values(results):
                            assert([r.statistic for r in results] == [3.0, 7.0])

                        def it_returns_the_expected_data_coverages(results):
                            assert([float(r.data_coverage) for r in results] == [3.0/24.0, 4.0/24.0])

                def describe_with_many_variables():

                    @pytest.fixture
                    def variable_sesh(history_sesh, var_temp_point, var_temp_max, var_temp_min, var_foo):
                        sesh = history_sesh
                        sesh.add_all([var_temp_point, var_temp_max, var_temp_min, var_foo])
                        sesh.flush()
                        yield sesh
                        sesh.query(Variable).delete()
                        sesh.flush()

                    def describe_with_many_observations_per_variable():

                        @pytest.fixture
                        def obs_sesh(variable_sesh, history_stn1_hourly, var_temp_point, var_temp_max, var_temp_min, var_foo):
                            sesh = variable_sesh
                            id = 0
                            for var in [var_temp_point, var_temp_max, var_temp_min, var_foo]:
                                for i in range(0,2):
                                    id += 1
                                    sesh.add(Obs(id=id, vars_id=var.id, history_id=history_stn1_hourly.id,
                                                 time=datetime.datetime(2000, 1, 1, 12, id), datum=float(id)))
                            sesh.flush()
                            yield sesh
                            sesh.query(Obs).delete()
                            sesh.flush()

                        @pytest.fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_exactly_the_expected_variables(results, var_temp_point, var_temp_max):
                            assert([r.vars_id for r in results].sort() == [var_temp_point.id, var_temp_max.id].sort())

            def describe_with_1_history_daily():

                @pytest.fixture
                def history_sesh(station_sesh, history_stn1_daily):
                    sesh = station_sesh
                    sesh.add(history_stn1_daily)
                    sesh.flush()
                    yield sesh
                    sesh.query(History).delete()
                    sesh.flush()

                def describe_with_1_variable():

                    @pytest.fixture
                    def variable_sesh(history_sesh, var_temp_point):
                        sesh = history_sesh
                        sesh.add(var_temp_point)
                        sesh.flush()
                        yield sesh
                        sesh.query(Variable).delete()
                        sesh.flush()

                    def describe_with_many_observations_on_different_days():

                        n_days = 3

                        @pytest.fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_daily):
                            sesh = variable_sesh
                            sesh.add_all(
                                [(Obs(id=i+100, vars_id=var_temp_point.id, history_id=history_stn1_daily.id,
                                      time=datetime.datetime(2000, 1, i+10, 12), datum=float(i+10)))
                                 for i in range(0,n_days)]
                            )
                            sesh.flush()
                            yield sesh
                            sesh.query(Obs).delete()
                            sesh.flush()

                        @pytest.fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_the_expected_number_of_rows(results):
                            assert(results.count() == n_days)

                        def it_returns_the_expected_days(results):
                            assert(set([r.obs_day for r in results]) ==
                                   set([datetime.datetime(2000, 1, i+10) for i in range(0,n_days)]))

                        def it_returns_the_expected_coverage(results):
                            assert(all(map(lambda r: r.data_coverage == 1.0, results)))

            def describe_with_1_history_hourly_1_history_daily():

                @pytest.fixture
                def history_sesh(station_sesh, history_stn1_hourly, history_stn1_daily):
                    sesh = station_sesh
                    sesh.add_all([history_stn1_hourly, history_stn1_daily])
                    sesh.flush()
                    yield sesh
                    sesh.query(History).delete()
                    sesh.flush()

                def describe_with_1_variable():

                    @pytest.fixture
                    def variable_sesh(history_sesh, var_temp_point):
                        sesh = history_sesh
                        sesh.add(var_temp_point)
                        sesh.flush()
                        yield sesh
                        sesh.query(Variable).delete()
                        sesh.flush()

                    def describe_with_observations_in_both_histories():

                        n_hours = 4

                        @pytest.fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly, history_stn1_daily):
                            sesh = variable_sesh
                            # hourly observations
                            sesh.add_all(
                                    [Obs(id=i, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                          time=datetime.datetime(2000, 1, 1, 12+i), datum=float(i))
                                     for i in range(0,n_hours)]
                            )
                            # daily observations
                            sesh.add(Obs(id=99, vars_id=var_temp_point.id, history_id=history_stn1_daily.id,
                                         time=datetime.datetime(2000, 1, 2, 12), datum=10.0))
                            sesh.flush()
                            yield sesh
                            sesh.query(Obs).delete()
                            sesh.flush()

                        @pytest.fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_the_expected_coverage(results):
                            # This tests in a mixed-history case that the correct history record is used for each
                            # observation, and that the correct coverage computation is done based on history
                            assert([float(r.data_coverage) for r in results.order_by(DailyMaxTemperature.obs_day)] ==
                                   [n_hours/24.0, 1.0])

    def describe_with_2_networks():

        @pytest.fixture
        def network_sesh(mod_empty_database_session, network1, network2):
            sesh = mod_empty_database_session
            sesh.add_all([network1, network2])
            sesh.flush()
            yield sesh
            sesh.query(Network).delete()
            sesh.flush()

        def describe_with_1_station_per_network():

            @pytest.fixture
            def station_sesh(network_sesh, station1, station2):
                sesh = network_sesh
                sesh.add_all([station1, station2])
                sesh.flush()
                yield sesh
                sesh.query(Station).delete()
                sesh.flush()

            def describe_with_1_history_hourly_per_station():

                @pytest.fixture
                def history_sesh(station_sesh, history_stn1_hourly, history_stn2_hourly):
                    sesh = station_sesh
                    sesh.add_all([history_stn1_hourly, history_stn2_hourly])
                    sesh.flush()
                    yield sesh
                    sesh.query(History).delete()
                    sesh.flush()

                def describe_with_1_variable_per_network(): # temp: point

                    @pytest.fixture
                    def variable_sesh(history_sesh, var_temp_point, var_temp_point2):
                        sesh = history_sesh
                        sesh.add_all([var_temp_point, var_temp_point2])
                        sesh.flush()
                        yield sesh
                        sesh.query(Variable).delete()
                        sesh.flush()

                    def describe_with_observations_for_each_station_variable():

                        n_days = 3
                        n_hours = 4

                        @pytest.fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly, var_temp_point2, history_stn2_hourly):
                            sesh = variable_sesh
                            id = 0
                            for (var, hx) in [(var_temp_point, history_stn1_hourly), (var_temp_point2, history_stn2_hourly)]:
                                for day in range(1, n_days+1):
                                    for hour in range(0, n_hours):
                                        id += 1
                                        sesh.add(
                                            Obs(id=id, vars_id=var.id, history_id=hx.id,
                                                  time=datetime.datetime(2000, 1, day, 12+hour), datum=float(id))
                                        )
                            sesh.flush()
                            print()
                            for obs in sesh.query(Obs):
                                print('### obs (id, var, hx, time): ', obs.id, obs.vars_id, obs.history_id, obs.time)
                            yield sesh
                            sesh.query(Obs).delete()
                            sesh.flush()

                        @pytest.fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_one_row_per_unique_combo_stn_var_day(results, var_temp_point, station1, var_temp_point2, station2):
                            assert(set([(r.station_id, r.vars_id, r.obs_day) for r in results]) ==
                                   set([(stn.id, var.id, datetime.datetime(2000, 1, day))
                                        for (var, stn) in [(var_temp_point, station1), (var_temp_point2, station2)]
                                        for day in range(1, n_days+1)]))



