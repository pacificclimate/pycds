import datetime
from pytest import fixture, approx
from pycds import Network, Station, History, Variable, Obs, NativeFlag, PCICFlag
from pycds import DailyMaxTemperature

# To maintain database consistency, objects must be added (and flushed) in this order:
#   Network
#   Station, History
#   Variable
#   Observation
#
# This imposes an order on the definition of session fixtures, and on the nesting of describe blocks that use them.

@fixture
def network1():
    return Network(id=1, name='Network 1')

@fixture
def network2():
    return Network(id=2, name='Network 2')

@fixture
def station1(network1):
    return Station(id=1, network_id=network1.id)

@fixture
def station2(network2):
    return Station(id=2, network_id=network2.id)

history_transition_date = datetime.datetime(2010, 1, 1)

@fixture
def history_stn1_hourly(station1):
    return History(id=1, station_id=station1.id, station_name='Station 1',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')

@fixture
def history_stn1_daily(station1):
    return History(id=2, station_id=station1.id, station_name='Station 1',
                   sdate=history_transition_date, edate=None, freq='daily')

@fixture
def history_stn2_hourly(station2):
    return History(id=3, station_id=station2.id, station_name='Station 2',
                   sdate=datetime.datetime.min, edate=history_transition_date, freq='1-hourly')

@fixture
def var_temp_point(network1): 
    return Variable(id=10, network_id=network1.id,
                    standard_name='air_temperature', cell_method='time: point')

@fixture
def var_temp_point2(network2):
    return Variable(id=11, network_id=network2.id,
                    standard_name='air_temperature', cell_method='time: point')

@fixture
def var_temp_max(network1):
    return Variable(id=20, network_id=network1.id,
                    standard_name='air_temperature', cell_method='time: maximum')

@fixture
def var_temp_min(network1):
    return Variable(id=30, network_id=network1.id,
                    standard_name='air_temperature', cell_method='time: minimum')

@fixture
def var_foo(network1):
    return Variable(id=40, network_id=network1.id,
                    standard_name='foo', cell_method='time: point')

@fixture
def native_flag_discard():
    return NativeFlag(id=1, discard=True)

@fixture
def native_flag_non_discard():
    return NativeFlag(id=2, discard=False)

@fixture
def pcic_flag_discard():
    return PCICFlag(id=1, discard=True)

@fixture
def pcic_flag_non_discard():
    return PCICFlag(id=2, discard=False)


def generic_sesh(sesh, sa_class, sa_objects):
    '''All session fixtures follow a common pattern, abstracted in this generator function.
    To use the generator correctly, i.e., so that the teardown after the yield is also performed,
    a fixture must first yield the result of next(g), then call next(g) again. This can be done two ways:

      gs = generic_sesh(...)
      yield next(gs)
      next(gs)

    or, slightly shorter:

      for sesh in generic_sesh(...):
          yield sesh

    The shorter method is used throughout.
    '''
    sesh.add_all(sa_objects)
    sesh.flush()
    yield sesh
    sesh.query(sa_class).delete()
    sesh.flush()

def describe_DailyMaxTemperature():
    def describe_with_1_network():

        @fixture
        def network_sesh(mod_empty_database_session, network1):
            for sesh in generic_sesh(mod_empty_database_session, Network, [network1]):
                yield sesh

        def describe_with_1_station():

            @fixture
            def station_sesh(network_sesh, station1):
                for sesh in generic_sesh(network_sesh, Station, [station1]):
                    yield sesh

            def describe_with_1_history_hourly():

                @fixture
                def history_sesh(station_sesh, history_stn1_hourly):
                    for sesh in generic_sesh(station_sesh, History, [history_stn1_hourly]):
                        yield sesh

                def describe_with_1_variable():

                    @fixture
                    def variable_sesh(history_sesh, var_temp_point):
                        for sesh in generic_sesh(history_sesh, Variable, [var_temp_point]):
                            yield sesh

                    def describe_with_many_observations_in_one_day():

                        @fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                            observations = [
                                Obs(id=i, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                    time=datetime.datetime(2000, 1, 1, 12+i), datum=float(i))
                                for i in range(1, 4)
                            ]
                            for sesh in generic_sesh(variable_sesh, Obs, observations):
                                yield sesh

                        @fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_a_single_row(results):
                            assert(results.count() == 1)

                        def it_returns_the_expected_station_variable_and_day(results, history_stn1_hourly, var_temp_point):
                            result = results.first()
                            assert(result.history_id == history_stn1_hourly.id)
                            assert(result.vars_id == var_temp_point.id)
                            assert(result.obs_day == datetime.datetime(2000, 1, 1))

                        def it_returns_the_expected_maximum_value(results):
                            assert(results.first().statistic == 3.0)

                        def it_returns_the_expected_data_coverage(results):
                            assert(results.first().data_coverage == approx(3.0 / 24.0))

                    def describe_with_many_observations_on_two_different_days():

                        @fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                            observations = [(Obs(id=1, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                             time=datetime.datetime(2000, 1, 1, 12), datum=1.0)), (
                                        Obs(id=2, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, 1, 13), datum=2.0)), (
                                        Obs(id=3, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, 1, 14), datum=3.0)), (
                                        Obs(id=4, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, 2, 12), datum=4.0)), (
                                        Obs(id=5, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, 2, 13), datum=5.0)), (
                                        Obs(id=6, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, 2, 14), datum=6.0)), (
                                        Obs(id=7, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, 2, 15), datum=7.0))]
                            for sesh in generic_sesh(variable_sesh, Obs, observations):
                                yield sesh

                        @fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature).order_by(DailyMaxTemperature.obs_day)

                        def it_returns_two_rows(results):
                            assert(results.count() == 2)

                        def it_returns_the_expected_station_variables(results, history_stn1_hourly, var_temp_point):
                            for result in results:
                                assert(result.history_id == history_stn1_hourly.id)
                                assert(result.vars_id == var_temp_point.id)

                        def it_returns_the_expected_days(results):
                            assert([r.obs_day for r in results] == [datetime.datetime(2000, 1, 1), datetime.datetime(2000, 1, 2)])

                        def it_returns_the_expected_max_values(results):
                            assert([r.statistic for r in results] == [3.0, 7.0])

                        def it_returns_the_expected_data_coverages(results):
                            assert([r.data_coverage for r in results] == approx([3.0/24.0, 4.0/24.0]))

                    def describe_with_many_observations_in_one_day_bis():
                        '''Set up observations for native flag tests'''

                        @fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                            observations = [
                                Obs(id=i, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                    time=datetime.datetime(2000, 1, 1, i), datum=float(i))
                                for i in range(0, 24)
                                ]
                            for sesh in generic_sesh(variable_sesh, Obs, observations):
                                yield sesh

                        # It would be better to DRY up describe_with_native_flags and describe_with_pcic_flags by
                        # parametrizing, but pytest doesn't yet support parameterization over fixtures:
                        # see https://github.com/pytest-dev/pytest/issues/349
                        def describe_with_native_flags():
                            '''2 native flags, 1 discard, 1 not discard'''

                            @fixture
                            def flag_sesh(obs_sesh, native_flag_discard, native_flag_non_discard):
                                for sesh in generic_sesh(obs_sesh, NativeFlag, [native_flag_discard, native_flag_non_discard]):
                                    yield sesh

                            def describe_with_native_flag_associations():
                                '''m < n associations of discard native flag to observations;
                                k < n associations of not discard native flag to observations'''

                                @fixture
                                def flag_assoc_sesh(flag_sesh, native_flag_discard, native_flag_non_discard):
                                    sesh = flag_sesh
                                    obs = sesh.query(Obs)
                                    for id in range(0, 12):
                                        obs.filter_by(id=id).first().native_flags.append(native_flag_discard)
                                    for id in range(6, 18):
                                        obs.filter_by(id=id).first().native_flags.append(native_flag_non_discard)
                                    sesh.flush()
                                    yield sesh
                                    for id in range(0, 24):
                                        obs.filter_by(id=id).first().native_flags = []
                                    sesh.flush()

                                @fixture
                                def results(flag_assoc_sesh):
                                    return flag_assoc_sesh.query(DailyMaxTemperature)

                                def setup_is_correct(flag_assoc_sesh):
                                    obs = flag_assoc_sesh.query(Obs)
                                    obs_flagged_discard = obs.filter(Obs.native_flags.any(NativeFlag.discard == True))
                                    assert(obs_flagged_discard.count() == 12)
                                    obs_flagged_not_discard = obs.filter(Obs.native_flags.any(NativeFlag.discard == False))
                                    assert(obs_flagged_not_discard.count() == 12)

                                def it_excludes_all_and_only_discarded_observations(results):
                                    assert(results.count() == 1)
                                    result = results.first()
                                    assert(result.data_coverage == approx(0.5))

                        def describe_with_pcic_flags():
                            '''2 pcic flags, 1 discard, 1 not discard'''

                            @fixture
                            def flag_sesh(obs_sesh, pcic_flag_discard, pcic_flag_non_discard):
                                for sesh in generic_sesh(obs_sesh, PCICFlag, [pcic_flag_discard, pcic_flag_non_discard]):
                                    yield sesh

                            def describe_with_pcic_flag_associations():
                                '''m < n associations of discard pcic flag to observations;
                                k < n associations of not discard pcic flag to observations'''

                                @fixture
                                def flag_assoc_sesh(flag_sesh, pcic_flag_discard, pcic_flag_non_discard):
                                    sesh = flag_sesh
                                    obs = sesh.query(Obs)
                                    for id in range(0, 12):
                                        obs.filter_by(id=id).first().pcic_flags.append(pcic_flag_discard)
                                    for id in range(6, 18):
                                        obs.filter_by(id=id).first().pcic_flags.append(pcic_flag_non_discard)
                                    sesh.flush()
                                    yield sesh
                                    for id in range(0, 24):
                                        obs.filter_by(id=id).first().pcic_flags = []
                                    sesh.flush()

                                @fixture
                                def results(flag_assoc_sesh):
                                    return flag_assoc_sesh.query(DailyMaxTemperature)

                                def setup_is_correct(flag_assoc_sesh):
                                    obs = flag_assoc_sesh.query(Obs)
                                    obs_flagged_discard = obs.filter(Obs.pcic_flags.any(PCICFlag.discard == True))
                                    assert(obs_flagged_discard.count() == 12)
                                    obs_flagged_not_discard = obs.filter(Obs.pcic_flags.any(PCICFlag.discard == False))
                                    assert(obs_flagged_not_discard.count() == 12)

                                def it_excludes_all_and_only_discarded_observations(results):
                                    assert(results.count() == 1)
                                    result = results.first()
                                    assert(result.data_coverage == approx(0.5))

                def describe_with_many_variables():

                    @fixture
                    def variable_sesh(history_sesh, var_temp_point, var_temp_max, var_temp_min, var_foo):
                        for sesh in generic_sesh(history_sesh, Variable, [var_temp_point, var_temp_max, var_temp_min, var_foo]):
                            yield sesh

                    def describe_with_many_observations_per_variable():

                        @fixture
                        def obs_sesh(variable_sesh, history_stn1_hourly, var_temp_point, var_temp_max, var_temp_min, var_foo):
                            observations = []
                            id = 0
                            for var in [var_temp_point, var_temp_max, var_temp_min, var_foo]:
                                for i in range(0,2):
                                    id += 1
                                    observations.append(Obs(id=id, vars_id=var.id, history_id=history_stn1_hourly.id,
                                                 time=datetime.datetime(2000, 1, 1, 12, id), datum=float(id)))
                            for sesh in generic_sesh(variable_sesh, Obs, observations):
                                yield sesh

                        @fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_exactly_the_expected_variables(results, var_temp_point, var_temp_max):
                            assert(set([r.vars_id for r in results]) == set([var_temp_point.id, var_temp_max.id]))

            def describe_with_1_history_daily():

                @fixture
                def history_sesh(station_sesh, history_stn1_daily):
                    for sesh in generic_sesh(station_sesh, History, [history_stn1_daily]):
                        yield sesh

                def describe_with_1_variable():

                    @fixture
                    def variable_sesh(history_sesh, var_temp_point):
                        for sesh in generic_sesh(history_sesh, Variable, [var_temp_point]):
                            yield sesh

                    def describe_with_many_observations_on_different_days():

                        n_days = 3

                        @fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_daily):
                            observations = [
                                Obs(id=i + 100, vars_id=var_temp_point.id, history_id=history_stn1_daily.id,
                                      time=datetime.datetime(2000, 1, i+10, 12), datum=float(i+10))
                                 for i in range(0,n_days)
                                ]
                            for sesh in generic_sesh(variable_sesh, Obs, observations):
                                yield sesh

                        @fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_the_expected_number_of_rows(results):
                            assert(results.count() == n_days)

                        def it_returns_the_expected_days(results):
                            assert(set([r.obs_day for r in results]) ==
                                   set([datetime.datetime(2000, 1, i+10) for i in range(0,n_days)]))

                        def it_returns_the_expected_coverage(results):
                            assert(all(map(lambda r: r.data_coverage == approx(1.0), results)))

            def describe_with_1_history_hourly_1_history_daily():

                @fixture
                def history_sesh(station_sesh, history_stn1_hourly, history_stn1_daily):
                    for sesh in generic_sesh(station_sesh, History, [history_stn1_hourly, history_stn1_daily]):
                        yield sesh

                def describe_with_1_variable():

                    @fixture
                    def variable_sesh(history_sesh, var_temp_point):
                        for sesh in generic_sesh(history_sesh, Variable, [var_temp_point]):
                            yield sesh

                    def describe_with_observations_in_both_histories():

                        n_hours = 4

                        @fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly, history_stn1_daily):
                            # hourly observations
                            observations = [
                                Obs(id=i, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                          time=datetime.datetime(2000, 1, 1, 12+i), datum=float(i))
                                     for i in range(0,n_hours)
                                ]
                            # daily observation
                            observations.append(Obs(id=99, vars_id=var_temp_point.id, history_id=history_stn1_daily.id,
                                         time=datetime.datetime(2000, 1, 2, 12), datum=10.0))
                            for sesh in generic_sesh(variable_sesh, Obs, observations):
                                yield sesh

                        @fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_one_result_per_history(results, history_stn1_hourly, history_stn1_daily):
                            assert(results.count() == 2)
                            assert(set([r.history_id for r in results]) == set([history_stn1_hourly.id, history_stn1_daily.id]))

                        def it_returns_the_expected_coverage(results):
                            assert([r.data_coverage for r in results.order_by(DailyMaxTemperature.obs_day)]
                                   == approx([n_hours/24.0, 1.0]))

    def describe_with_2_networks():

        @fixture
        def network_sesh(mod_empty_database_session, network1, network2):
            for sesh in generic_sesh(mod_empty_database_session, Network, [network1, network2]):
                yield sesh

        def describe_with_1_station_per_network():

            @fixture
            def station_sesh(network_sesh, station1, station2):
                for sesh in generic_sesh(network_sesh, Station, [station1, station2]):
                    yield sesh

            def describe_with_1_history_hourly_per_station():

                @fixture
                def history_sesh(station_sesh, history_stn1_hourly, history_stn2_hourly):
                    for sesh in generic_sesh(station_sesh, History, [history_stn1_hourly, history_stn2_hourly]):
                        yield sesh

                def describe_with_1_variable_per_network(): # temp: point

                    @fixture
                    def variable_sesh(history_sesh, var_temp_point, var_temp_point2):
                        for sesh in generic_sesh(history_sesh, Variable, [var_temp_point, var_temp_point2]):
                            yield sesh

                    def describe_with_observations_for_each_station_variable():

                        n_days = 3
                        n_hours = 4

                        @fixture
                        def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly, var_temp_point2, history_stn2_hourly):
                            observations = []
                            id = 0
                            for (var, hx) in [(var_temp_point, history_stn1_hourly), (var_temp_point2, history_stn2_hourly)]:
                                for day in range(1, n_days+1):
                                    for hour in range(0, n_hours):
                                        id += 1
                                        observations.append(
                                            Obs(id=id, vars_id=var.id, history_id=hx.id,
                                                  time=datetime.datetime(2000, 1, day, 12+hour), datum=float(id))
                                        )
                            for sesh in generic_sesh(variable_sesh, Obs, observations):
                                yield sesh

                        @fixture
                        def results(obs_sesh):
                            return obs_sesh.query(DailyMaxTemperature)

                        def it_returns_one_row_per_unique_combo_hx_var_day(results, var_temp_point, history_stn1_hourly, var_temp_point2, history_stn2_hourly):
                            assert(set([(r.history_id, r.vars_id, r.obs_day) for r in results]) ==
                                   set([(stn.id, var.id, datetime.datetime(2000, 1, day))
                                        for (var, stn) in [(var_temp_point, history_stn1_hourly), (var_temp_point2, history_stn2_hourly)]
                                        for day in range(1, n_days+1)]))