'''Test daily temperature extrema views

Please see README for a description of the test framework used here.

Idiosyncracies:

- The various `refreshed_sesh` fixtures look as if they could exploit the autouse feature of pytest
  (http://doc.pytest.org/en/latest/fixture.html#autouse-fixtures-xunit-setup-on-steroids), but unfortunately they
  can't: Each one is the terminus of a different cascade of fixtures. (Different fixtures may have the same name
  in different contexts, but they are actually different objects, embodying different test conditions, and that
  prevents us from using autouse.
- We use a workaround for the absent but desirable ability to parametrize tests over fixtures. See
  docstring in tests/test_wa_monthly_views_common.py for a more complete explanation of this technique.
'''
import datetime

from pytest import fixture, mark, approx
from sqlalchemy.sql import text

from pycds.util import generic_sesh
from pycds import Network, Station, History, Variable, Obs, NativeFlag, PCICFlag
from pycds.weather_anomaly import DiscardedObsId
from pycds.weather_anomaly import DailyMaxTemperature, DailyMinTemperature


def describe_function_effective__day():
    expected_day = {
        'max': {
            '1-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 1)},
            '12-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 2)},
        },
        'min': {
            '1-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 1)},
            '12-hourly': {'2000-01-01 07:23': datetime.datetime(2000, 1, 1), '2000-01-01 16:18': datetime.datetime(2000, 1, 1)},
        }
    }

    @mark.parametrize('obs_time', ['2000-01-01 07:23', '2000-01-01 16:18'])  # morning and afternoon
    @mark.parametrize('freq', ['1-hourly', '12-hourly'])
    @mark.parametrize('extremum', ['max', 'min'])
    def it_returns_the_expected_day_of_observation(mod_empty_database_session, obs_time, extremum, freq):
        result = mod_empty_database_session.execute(
            text('SELECT effective_day(:obs_time, :extremum, :freq) AS eday'),
            {'obs_time': obs_time, 'extremum': extremum, 'freq': freq}
        ).fetchall()
        assert len(result) == 1
        assert result[0]['eday'] == expected_day[extremum][freq][obs_time]


views = [DiscardedObsId, DailyMaxTemperature, DailyMinTemperature]
refreshable_views = [DailyMaxTemperature, DailyMinTemperature]

@fixture(scope='module')
def with_views_sesh(mod_empty_database_session):
    sesh = mod_empty_database_session
    for view in views:
        view.create(sesh)
    yield sesh
    for view in reversed(views):
        view.drop(sesh)


def refresh_views(sesh):
    for view in refreshable_views:
        view.refresh(sesh)


def describe_with_1_network():

    @fixture
    def network_sesh(with_views_sesh, network1):
        for sesh in generic_sesh(with_views_sesh, [network1]):
            yield sesh

    def describe_with_1_station():

        @fixture
        def station_sesh(network_sesh, station1):
            for sesh in generic_sesh(network_sesh, [station1]):
                yield sesh

        def describe_with_1_history_hourly():

            @fixture
            def history_sesh(station_sesh, history_stn1_hourly):
                for sesh in generic_sesh(station_sesh, [history_stn1_hourly]):
                    yield sesh

            def describe_with_1_variable():

                @fixture
                def variable_sesh(history_sesh, var_temp_point):
                    for sesh in generic_sesh(history_sesh, [var_temp_point]):
                        yield sesh

                def describe_with_many_observations_in_one_day():

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                        observations = [
                            Obs(variable=var_temp_point, history=history_stn1_hourly,
                                time=datetime.datetime(2000, 1, 1, 12+i), datum=float(i))
                            for i in range(1, 4)
                        ]
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def refreshed_sesh(obs_sesh):
                        refresh_views(obs_sesh)
                        yield obs_sesh

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_a_single_row(refreshed_sesh, DailyExtremeTemperature):
                        assert refreshed_sesh.query(DailyExtremeTemperature).count() == 1

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_station_variable_and_day(
                            refreshed_sesh, DailyExtremeTemperature, history_stn1_hourly, var_temp_point):
                        result = refreshed_sesh.query(DailyExtremeTemperature).first()
                        assert result.history_id == history_stn1_hourly.id
                        assert result.vars_id == var_temp_point.id
                        assert result.obs_day == datetime.datetime(2000, 1, 1)

                    @mark.parametrize('DailyExtremeTemperature, statistic', [
                        (DailyMaxTemperature, 3.0), (DailyMinTemperature, 1.0)
                    ])
                    def it_returns_the_expected_extreme_value(refreshed_sesh, DailyExtremeTemperature, statistic):
                        assert refreshed_sesh.query(DailyExtremeTemperature).first().statistic == statistic

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_data_coverage(refreshed_sesh, DailyExtremeTemperature):
                        assert refreshed_sesh.query(DailyExtremeTemperature).first().data_coverage == approx(3.0 / 24.0)

                def describe_with_many_observations_on_two_different_days():

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                        observations = [Obs(variable=var_temp_point, history=history_stn1_hourly,
                                            time=datetime.datetime(2000, 1, 1, 12+i), datum=float(i))
                                        for i in range(3)] +\
                                       [Obs(variable=var_temp_point, history=history_stn1_hourly,
                                            time=datetime.datetime(2000, 1, 2, 8+i), datum=float(i))
                                        for i in range(4,8)]
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def refreshed_sesh(obs_sesh):
                        refresh_views(obs_sesh)
                        yield obs_sesh

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_two_rows(refreshed_sesh, DailyExtremeTemperature):
                        assert refreshed_sesh.query(DailyExtremeTemperature).count() == 2

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_station_variables(
                            refreshed_sesh, DailyExtremeTemperature, history_stn1_hourly, var_temp_point):
                        for result in refreshed_sesh.query(DailyExtremeTemperature):
                            assert result.history_id == history_stn1_hourly.id
                            assert result.vars_id == var_temp_point.id

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_days(refreshed_sesh, DailyExtremeTemperature):
                        assert set([r.obs_day for r in refreshed_sesh.query(DailyExtremeTemperature)]) == \
                               set([datetime.datetime(2000, 1, 1), datetime.datetime(2000, 1, 2)])

                    @mark.parametrize('DailyExtremeTemperature, statistics', [
                        (DailyMaxTemperature, [2.0, 7.0]),
                        (DailyMinTemperature, [0.0, 4.0])
                    ])
                    def it_returns_the_expected_extreme_values(refreshed_sesh, DailyExtremeTemperature, statistics):
                        results = refreshed_sesh.query(DailyExtremeTemperature).order_by(DailyExtremeTemperature.obs_day)
                        assert [r.statistic for r in results] == statistics

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_data_coverages(refreshed_sesh, DailyExtremeTemperature):
                        results = refreshed_sesh.query(DailyExtremeTemperature).order_by(DailyExtremeTemperature.obs_day)
                        assert [r.data_coverage for r in results] == approx([3.0/24.0, 4.0/24.0])

                def describe_with_many_observations_in_one_day_bis():
                    '''Set up observations for native flag tests'''

                    num_obs_for_native = 12
                    num_obs_for_pcic = 12
                    num_obs = num_obs_for_native + num_obs_for_pcic
                    pcic_offset = num_obs_for_native

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                        observations = [
                            Obs(id=i, variable=var_temp_point, history=history_stn1_hourly,
                                time=datetime.datetime(2000, 1, 1, i), datum=float(i))
                            for i in range(num_obs)
                            ]
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    # It would be better to DRY up describe_with_native_flags and describe_with_pcic_flags by
                    # parametrizing, but pytest doesn't yet support parameterization over fixtures:
                    # see https://github.com/pytest-dev/pytest/issues/349
                    def describe_with_native_flags():
                        '''2 native flags, 1 discard, 1 not discard'''

                        @fixture
                        def flag_sesh(obs_sesh, native_flag_discard, native_flag_non_discard,
                                      pcic_flag_discard, pcic_flag_non_discard):
                            for sesh in generic_sesh(obs_sesh, [native_flag_discard, native_flag_non_discard,
                                                                pcic_flag_discard, pcic_flag_non_discard]):
                                yield sesh

                        def describe_with_native_flag_associations():
                            '''m < n associations of discard native flag to observations;
                            k < n associations of not discard native flag to observations, some on same observations
                                as discard flags'''

                            # num_discarded must be different than num_obs/2 to guarantee test is unambiguous
                            num_discarded = 5
                            num_non_discarded = 5

                            @fixture
                            def flag_assoc_sesh(request, flag_sesh,
                                                native_flag_discard, native_flag_non_discard,
                                                pcic_flag_discard, pcic_flag_non_discard):
                                """This fixture is used as an indirect fixture for parametrized tests.
                                Its behaviour depends on the value of request.param, which tells whether
                                to add associations to native flags, pcic flags, or both. Associations to
                                native flags and to pcic flags do not overlap.

                                This kind of indirect parameterization is a substitute for the absent ability of pytest
                                to parametrize over fixtures, which would make this whole things somewhat simpler to
                                code and understand. In any case, it enables us to perform the same tests for several
                                different combinations of flagging of observations without repetitive code.
                                """
                                sesh = flag_sesh
                                obs = sesh.query(Obs)
                                if request.param in ['native', 'both']:
                                    for o in obs.filter((0 <= Obs.id) & (Obs.id < num_discarded)).all():
                                        o.native_flags.append(native_flag_discard)
                                    for o in obs.filter((3 <= Obs.id) & (Obs.id < 3 + num_non_discarded)).all():
                                        o.native_flags.append(native_flag_non_discard)
                                elif request.param in ['pcic', 'both']:
                                    for o in obs.filter((pcic_offset <= Obs.id) & (Obs.id < pcic_offset + num_discarded)).all():
                                        o.pcic_flags.append(pcic_flag_discard)
                                    for o in obs.filter((pcic_offset + 3 <= Obs.id) & (Obs.id < pcic_offset + 3 + num_non_discarded)).all():
                                        o.pcic_flags.append(pcic_flag_non_discard)
                                sesh.flush()
                                yield sesh
                                for o in obs.all():
                                    o.native_flags = []
                                    o.pcic_flags = []
                                sesh.flush()

                            @mark.parametrize('flag_assoc_sesh', [
                                'native',
                                'pcic'
                            ], indirect=True)
                            def setup_is_correct(flag_assoc_sesh):
                                obs = flag_assoc_sesh.query(Obs)
                                obs_flagged_discard = obs.filter(Obs.native_flags.any(NativeFlag.discard == True) |
                                                                 Obs.pcic_flags.any(PCICFlag.discard == True))
                                assert obs_flagged_discard.count() == num_discarded
                                obs_flagged_not_discard = obs.filter(Obs.native_flags.any(NativeFlag.discard == False) |
                                                                     Obs.pcic_flags.any(PCICFlag.discard == False))
                                assert obs_flagged_not_discard.count() == num_non_discarded

                            @mark.parametrize('flag_assoc_sesh', [
                                'native',
                                'pcic',
                                'both'
                            ], indirect=True)
                            @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                            def it_excludes_all_and_only_discarded_observations(flag_assoc_sesh, DailyExtremeTemperature):
                                sesh = flag_assoc_sesh
                                refresh_views(sesh)
                                results = sesh.query(DailyExtremeTemperature)
                                num_actually_discarded = sesh.query(Obs)\
                                    .filter(Obs.native_flags.any(NativeFlag.discard == True) |
                                                                 Obs.pcic_flags.any(PCICFlag.discard == True))\
                                    .count()
                                assert results.count() == 1
                                result = results.first()
                                assert result.data_coverage == approx(1 - float(num_actually_discarded)/num_obs)

            def describe_with_many_variables():

                @fixture
                def variable_sesh(history_sesh, var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo):
                    for sesh in generic_sesh(history_sesh,
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
                                observations.append(Obs(variable=var, history=history_stn1_hourly,
                                             time=datetime.datetime(2000, 1, 1, 12, id), datum=float(id)))
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def refreshed_sesh(obs_sesh):
                        refresh_views(obs_sesh)
                        yield obs_sesh

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_exactly_the_expected_variables(
                            refreshed_sesh, DailyExtremeTemperature,
                            var_temp_point, var_temp_max, var_temp_min, var_temp_mean
                    ):
                        expected_variables = {
                            DailyMaxTemperature: {var_temp_point.id, var_temp_max.id, var_temp_mean.id},
                            DailyMinTemperature: {var_temp_point.id, var_temp_min.id, var_temp_mean.id},
                        }
                        assert set([r.vars_id for r in refreshed_sesh.query(DailyExtremeTemperature)]) == \
                               expected_variables[DailyExtremeTemperature]

        def describe_with_1_history_daily():

            @fixture
            def history_sesh(station_sesh, history_stn1_daily):
                for sesh in generic_sesh(station_sesh, [history_stn1_daily]):
                    yield sesh

            def describe_with_1_variable():

                @fixture
                def variable_sesh(history_sesh, var_temp_point):
                    for sesh in generic_sesh(history_sesh, [var_temp_point]):
                        yield sesh

                def describe_with_many_observations_on_different_days():

                    n_days = 3

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_daily):
                        observations = [
                            Obs(id=i + 100, variable=var_temp_point, history=history_stn1_daily,
                                  time=datetime.datetime(2000, 1, i+10, 12), datum=float(i+10))
                             for i in range(0,n_days)
                            ]
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def refreshed_sesh(obs_sesh):
                        refresh_views(obs_sesh)
                        yield obs_sesh

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_number_of_rows(refreshed_sesh, DailyExtremeTemperature):
                        assert refreshed_sesh.query(DailyExtremeTemperature).count() == n_days

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_days(refreshed_sesh, DailyExtremeTemperature):
                        assert set([r.obs_day for r in refreshed_sesh.query(DailyExtremeTemperature)]) == \
                               set([datetime.datetime(2000, 1, i+10) for i in range(0, n_days)])

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_coverage(refreshed_sesh, DailyExtremeTemperature):
                        assert all(map(lambda r: r.data_coverage == approx(1.0), refreshed_sesh.query(DailyExtremeTemperature)))

        def describe_with_1_history_hourly_1_history_daily():

            @fixture
            def history_sesh(station_sesh, history_stn1_hourly, history_stn1_daily):
                for sesh in generic_sesh(station_sesh, [history_stn1_hourly, history_stn1_daily]):
                    yield sesh

            def describe_with_1_variable():

                @fixture
                def variable_sesh(history_sesh, var_temp_point):
                    for sesh in generic_sesh(history_sesh, [var_temp_point]):
                        yield sesh

                def describe_with_observations_in_both_histories():

                    n_hours = 4

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly, history_stn1_daily):
                        # hourly observations
                        observations = [
                            Obs(variable=var_temp_point, history=history_stn1_hourly,
                                      time=datetime.datetime(2000, 1, 1, 12+i), datum=float(i))
                                 for i in range(0, n_hours)
                            ]
                        # daily observation
                        observations.append(Obs(variable=var_temp_point, history=history_stn1_daily,
                                     time=datetime.datetime(2000, 1, 2, 12), datum=10.0))
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def refreshed_sesh(obs_sesh):
                        refresh_views(obs_sesh)
                        yield obs_sesh

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_one_result_per_history(
                            refreshed_sesh, DailyExtremeTemperature, history_stn1_hourly, history_stn1_daily):
                        assert refreshed_sesh.query(DailyExtremeTemperature).count() == 2
                        assert set([r.history_id for r in refreshed_sesh.query(DailyExtremeTemperature)]) == \
                               {history_stn1_hourly.id, history_stn1_daily.id}

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_the_expected_coverage(refreshed_sesh, DailyExtremeTemperature):
                        assert [r.data_coverage for r in
                                refreshed_sesh.query(DailyExtremeTemperature).order_by(DailyExtremeTemperature.obs_day)] \
                                == approx([n_hours/24.0, 1.0])

        def describe_with_12_hourly_history():

            @fixture
            def history_sesh(station_sesh, history_stn1_12_hourly):
                for sesh in generic_sesh(station_sesh, [history_stn1_12_hourly]):
                    yield sesh

            def describe_with_Tmax_and_Tmin_variables():

                @fixture
                def variable_sesh(history_sesh, var_temp_max, var_temp_min):
                    for sesh in generic_sesh(history_sesh, [var_temp_max, var_temp_min]):
                        yield sesh

                def describe_with_observations_for_both_variables():

                    # max and min temperature observations, by day, then hour
                    tmax = {
                        11: { 7:  0, 16:  5 },
                        12: { 7: 10, 16: 15 },
                        13: { 7: 11, 16: 20 },  # afternoon obs applies to day 14
                    }
                    tmin = {
                        11: { 7: -5, 16:   0 },
                        12: { 7:  0, 16:  10 },
                        13: { 7:  4, 16: -10 },
                    }

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_max, var_temp_min, history_stn1_12_hourly):
                        observations = [Obs(variable=var_temp_max, history=history_stn1_12_hourly,
                                            time=datetime.datetime(2000, 1, day, hour), datum=float(temp))
                                        for day, hours in iter(tmax.items())
                                        for hour, temp in iter(hours.items())
                                       ] +\
                                       [Obs(variable=var_temp_min, history=history_stn1_12_hourly,
                                                time=datetime.datetime(2000, 1, day, hour), datum=float(temp))
                                        for day, hours in iter(tmin.items())
                                        for hour, temp in iter(hours.items())
                                       ]

                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def refreshed_sesh(obs_sesh):
                        refresh_views(obs_sesh)
                        yield obs_sesh

                    @mark.parametrize('DailyExtremeTemperature, expected', [
                        # expected is (obs_day, statistic, data_coverage)
                        (DailyMaxTemperature, [
                            (datetime.datetime(2000, 1, 11),  0.0, 0.5),
                            (datetime.datetime(2000, 1, 12), 10.0, 1.0),
                            (datetime.datetime(2000, 1, 13), 15.0, 1.0),
                            (datetime.datetime(2000, 1, 14), 20.0, 0.5),
                        ]),
                        (DailyMinTemperature, [
                            (datetime.datetime(2000, 1, 11),  -5.0, 1.0),
                            (datetime.datetime(2000, 1, 12),   0.0, 1.0),
                            (datetime.datetime(2000, 1, 13), -10.0, 1.0),
                        ]),
                    ])
                    def it_returns_the_expected_days_and_temperature_extrema(refreshed_sesh, DailyExtremeTemperature, expected):
                        results = refreshed_sesh.query(DailyExtremeTemperature).order_by(DailyExtremeTemperature.obs_day)
                        assert [(r.obs_day, r.statistic, r.data_coverage) for r in results] == expected

def describe_with_2_networks():

    @fixture
    def network_sesh(with_views_sesh, network1, network2):
        for sesh in generic_sesh(with_views_sesh, [network1, network2]):
            yield sesh

    def describe_with_1_station_per_network():

        @fixture
        def station_sesh(network_sesh, station1, station2):
            for sesh in generic_sesh(network_sesh, [station1, station2]):
                yield sesh

        def describe_with_1_history_hourly_per_station():

            @fixture
            def history_sesh(station_sesh, history_stn1_hourly, history_stn2_hourly):
                for sesh in generic_sesh(station_sesh, [history_stn1_hourly, history_stn2_hourly]):
                    yield sesh

            def describe_with_1_variable_per_network(): # temp: point

                @fixture
                def variable_sesh(history_sesh, var_temp_point, var_temp_point2):
                    for sesh in generic_sesh(history_sesh, [var_temp_point, var_temp_point2]):
                        yield sesh

                def describe_with_observations_for_each_station_variable():

                    n_days = 3
                    n_hours = 4

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly,
                                 var_temp_point2, history_stn2_hourly):
                        observations = []
                        id = 0
                        for (var, hx) in [(var_temp_point, history_stn1_hourly),
                                          (var_temp_point2, history_stn2_hourly)]:
                            for day in range(1, n_days+1):
                                for hour in range(0, n_hours):
                                    id += 1
                                    observations.append(
                                        Obs(variable=var, history=hx,
                                              time=datetime.datetime(2000, 1, day, 12+hour), datum=float(id))
                                    )
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def refreshed_sesh(obs_sesh):
                        refresh_views(obs_sesh)
                        yield obs_sesh

                    @mark.parametrize('DailyExtremeTemperature', [DailyMaxTemperature, DailyMinTemperature])
                    def it_returns_one_row_per_unique_combo_hx_var_day(refreshed_sesh, DailyExtremeTemperature,
                               var_temp_point, history_stn1_hourly, var_temp_point2, history_stn2_hourly):
                        assert set([(r.history_id, r.vars_id, r.obs_day)
                                    for r in refreshed_sesh.query(DailyExtremeTemperature)]) == \
                               set([(stn.id, var.id, datetime.datetime(2000, 1, day))
                                    for (var, stn) in [(var_temp_point, history_stn1_hourly),
                                                       (var_temp_point2, history_stn2_hourly)]
                                    for day in range(1, n_days+1)])
