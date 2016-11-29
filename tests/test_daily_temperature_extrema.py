import datetime

from pytest import fixture, mark, approx

from pycds.util import generic_sesh
from pycds import Network, Station, History, Variable, Obs, NativeFlag, PCICFlag
from pycds import DailyMaxTemperature, DailyMinTemperature


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
                    def query(obs_sesh):
                        return obs_sesh.query

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_a_single_row(query, DailyExtremeTemperature):
                        assert query(DailyExtremeTemperature).count() == 1

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_station_variable_and_day(query, DailyExtremeTemperature, history_stn1_hourly, var_temp_point):
                        result = query(DailyExtremeTemperature).first()
                        assert result.history_id == history_stn1_hourly.id
                        assert result.vars_id == var_temp_point.id
                        assert result.obs_day == datetime.datetime(2000, 1, 1)

                    @mark.parametrize('DailyExtremeTemperature, statistic', [
                        (DailyMaxTemperature, 3.0), (DailyMinTemperature, 1.0)
                    ])
                    def it_returns_the_expected_extreme_value(query, DailyExtremeTemperature, statistic):
                        assert query(DailyExtremeTemperature).first().statistic == statistic

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_data_coverage(query, DailyExtremeTemperature):
                        assert query(DailyExtremeTemperature).first().data_coverage == approx(3.0 / 24.0)

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
                    def query(obs_sesh):
                        return obs_sesh.query

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_two_rows(query, DailyExtremeTemperature):
                        assert query(DailyExtremeTemperature).count() == 2

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_station_variables(query, DailyExtremeTemperature, history_stn1_hourly, var_temp_point):
                        for result in query(DailyExtremeTemperature):
                            assert result.history_id == history_stn1_hourly.id
                            assert result.vars_id == var_temp_point.id

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_days(query, DailyExtremeTemperature):
                        assert set([r.obs_day for r in query(DailyExtremeTemperature)]) == \
                               set([datetime.datetime(2000, 1, 1), datetime.datetime(2000, 1, 2)])

                    @mark.parametrize('DailyExtremeTemperature, statistics', [
                        (DailyMaxTemperature, [3.0, 7.0]),
                        (DailyMinTemperature, [1.0, 4.0])
                    ])
                    def it_returns_the_expected_extreme_values(query, DailyExtremeTemperature, statistics):
                        results = query(DailyExtremeTemperature).order_by(DailyExtremeTemperature.obs_day)
                        assert [r.statistic for r in results] == statistics

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_data_coverages(query, DailyExtremeTemperature):
                        results = query(DailyExtremeTemperature).order_by(DailyExtremeTemperature.obs_day)
                        assert [r.data_coverage for r in results] == approx([3.0/24.0, 4.0/24.0])

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
                            def query(flag_assoc_sesh):
                                return flag_assoc_sesh.query

                            def setup_is_correct(flag_assoc_sesh):
                                obs = flag_assoc_sesh.query(Obs)
                                obs_flagged_discard = obs.filter(Obs.native_flags.any(NativeFlag.discard == True))
                                assert obs_flagged_discard.count() == 12
                                obs_flagged_not_discard = obs.filter(Obs.native_flags.any(NativeFlag.discard == False))
                                assert obs_flagged_not_discard.count() == 12

                            @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                            def it_excludes_all_and_only_discarded_observations(query, DailyExtremeTemperature):
                                results = query(DailyExtremeTemperature)
                                assert results.count() == 1
                                result = results.first()
                                assert result.data_coverage == approx(0.5)

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
                            def query(flag_assoc_sesh):
                                return flag_assoc_sesh.query

                            def setup_is_correct(flag_assoc_sesh):
                                obs = flag_assoc_sesh.query(Obs)
                                obs_flagged_discard = obs.filter(Obs.pcic_flags.any(PCICFlag.discard == True))
                                assert obs_flagged_discard.count() == 12
                                obs_flagged_not_discard = obs.filter(Obs.pcic_flags.any(PCICFlag.discard == False))
                                assert obs_flagged_not_discard.count() == 12

                            @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                            def it_excludes_all_and_only_discarded_observations(query, DailyExtremeTemperature):
                                results = query(DailyExtremeTemperature)
                                assert results.count() == 1
                                result = results.first()
                                assert result.data_coverage == approx(0.5)

            def describe_with_many_variables():

                @fixture
                def variable_sesh(history_sesh, var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo):
                    for sesh in generic_sesh(history_sesh, Variable,
                                             [var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo]):
                        yield sesh

                def describe_with_many_observations_per_variable():

                    @fixture
                    def obs_sesh(variable_sesh, history_stn1_hourly, var_temp_point, var_temp_max, var_temp_min,
                                 var_temp_mean, var_foo):
                        observations = []
                        id = 0
                        for var in [var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo]:
                            for i in range(0,2):
                                id += 1
                                observations.append(Obs(id=id, vars_id=var.id, history_id=history_stn1_hourly.id,
                                             time=datetime.datetime(2000, 1, 1, 12, id), datum=float(id)))
                        for sesh in generic_sesh(variable_sesh, Obs, observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        return obs_sesh.query

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_exactly_the_expected_variables(
                            query, DailyExtremeTemperature,
                            var_temp_point, var_temp_max, var_temp_min, var_temp_mean
                    ):
                        expected_variables = {
                            DailyMaxTemperature: {var_temp_point.id, var_temp_max.id, var_temp_mean.id},
                            DailyMinTemperature: {var_temp_point.id, var_temp_min.id, var_temp_mean.id},
                        }
                        assert set([r.vars_id for r in query(DailyExtremeTemperature)]) == \
                               expected_variables[DailyExtremeTemperature]

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
                    def query(obs_sesh):
                        return obs_sesh.query

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_number_of_rows(query, DailyExtremeTemperature):
                        assert query(DailyExtremeTemperature).count() == n_days

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_days(query, DailyExtremeTemperature):
                        assert set([r.obs_day for r in query(DailyExtremeTemperature)]) == \
                               set([datetime.datetime(2000, 1, i+10) for i in range(0, n_days)])

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_coverage(query, DailyExtremeTemperature):
                        assert all(map(lambda r: r.data_coverage == approx(1.0), query(DailyExtremeTemperature)))

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
                                 for i in range(0, n_hours)
                            ]
                        # daily observation
                        observations.append(Obs(id=99, vars_id=var_temp_point.id, history_id=history_stn1_daily.id,
                                     time=datetime.datetime(2000, 1, 2, 12), datum=10.0))
                        for sesh in generic_sesh(variable_sesh, Obs, observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        return obs_sesh.query

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_one_result_per_history(query, DailyExtremeTemperature, history_stn1_hourly, history_stn1_daily):
                        assert query(DailyExtremeTemperature).count() == 2
                        assert set([r.history_id for r in query(DailyExtremeTemperature)]) == \
                               {history_stn1_hourly.id, history_stn1_daily.id}

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_coverage(query, DailyExtremeTemperature):
                        assert [r.data_coverage for r in query(DailyExtremeTemperature).order_by(DailyExtremeTemperature.obs_day)] \
                                == approx([n_hours/24.0, 1.0])

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
                    def query(obs_sesh):
                        return obs_sesh.query

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_one_row_per_unique_combo_hx_var_day(query, DailyExtremeTemperature,
                               var_temp_point, history_stn1_hourly, var_temp_point2, history_stn2_hourly):
                        assert set([(r.history_id, r.vars_id, r.obs_day) for r in query(DailyExtremeTemperature)]) == \
                               set([(stn.id, var.id, datetime.datetime(2000, 1, day))
                                    for (var, stn) in [(var_temp_point, history_stn1_hourly), (var_temp_point2, history_stn2_hourly)]
                                    for day in range(1, n_days+1)])
