"""Test all features common to weather anomaly monthly temperature and precipitation views, namely

  MonthlyAverageOfDailyMaxTemperature
  MonthlyAverageOfDailyMinTemperature
  MonthlyTotalPrecipitation

Please see README for a description of the test framework used here.


Because all tests are common to the 3 views, the tests are parametrized over those views.

Temperature and precipitation views require different database contents (observations).
Ideally we could parameterize over separate fixtures giving these contents, but pytest does not yet support that
(see https://github.com/pytest-dev/pytest/issues/349).

As a workaround for this, we use argument indirection (see http://doc.pytest.org/en/latest/example/parametrize.html#apply-indirect-on-particular-arguments)
in the parameterization of fixtures. This enables a fixture (with the same name as the parameter) to intervene between
the argument (parameter value) proper and the value passed in to the test. The intervening fixture has access to the
argument value, and can therefore behave differently depending on that argument. In this case, the different behaviour
is to set up the database with different contents depending on the view to be tested.

(Note: Parametrization over database setups is not strictly necessary in this particular set of tests, since the
database contents for temperature and precipitation are completely independent and could therefore all be loaded for all
tests. However, there are situations where this is not the case and it seemed worth working this out here. And perhaps
a feeble efficiency argument could be made not to load up more information into the database per test than necessary.)
"""

import datetime

from pytest import fixture, mark, approx

from pycds.util import generic_sesh
from pycds import Network, Station, History, Variable, Obs
from pycds.weather_anomaly import \
    DailyMaxTemperature, DailyMinTemperature, \
    MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, \
    MonthlyTotalPrecipitation


views_to_refresh = [
    DailyMaxTemperature,
    DailyMinTemperature,
    MonthlyAverageOfDailyMaxTemperature,
    MonthlyAverageOfDailyMinTemperature,
    # MonthlyTotalPrecipitation,
]
views = views_to_refresh


@fixture(scope='function')
def with_views_sesh(session):
    for view in views:
        view.create(session)
    yield session
    for view in reversed(views):
        view.drop(session)


def refresh_views(sesh):
    for view in views_to_refresh:
        view.refresh(sesh)


def id(param):
    """Return a representation for a test parameter.

    The long and frequently-used view-class parameters that are the keys of `abbrev` are represented as indicated;
    otherwise None is returned, which causes the standard pytest-generated representation of the parameter to be used.
    """
    abbrev = {
        MonthlyAverageOfDailyMaxTemperature: 'Tmax',
        MonthlyAverageOfDailyMinTemperature: 'Tmin',
        MonthlyTotalPrecipitation: 'Precip',
    }
    return abbrev.get(param, None)


def test_view_definitions():
    for view in views_to_refresh:
        print()
        print('--', view.__name__)
        print(view.__selectable__.compile(compile_kwargs={"literal_binds": True}))


def describe_with_1_network():

    @fixture
    def network_sesh(with_views_sesh, network1):
        for sesh in generic_sesh(with_views_sesh , [network1]):
            yield sesh

    def describe_with_1_station():

        @fixture
        def station_sesh(network_sesh, station1):
            for sesh in generic_sesh(network_sesh , [station1]):
                yield sesh

        def describe_with_1_history_hourly():

            @fixture
            def history_sesh(station_sesh, history_stn1_hourly):
                for sesh in generic_sesh(station_sesh , [history_stn1_hourly]):
                    yield sesh

            def describe_with_1_variable():

                @fixture
                def variable_sesh(history_sesh, var_temp_point):
                    for sesh in generic_sesh(history_sesh , [var_temp_point]):
                        yield sesh

                def describe_with_a_partial_set_of_observations_for_one_month():

                    days = range(1, 32, 2)
                    hours = range(1, 24, 2)

                    @fixture
                    def obs_sesh(request, variable_sesh, var_temp_point, var_precip_net1_1, history_stn1_hourly):
                        """Yield a session with particular observations added to variable_sesh.
                        Observations added depend on the value of request.param: 
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, or MonthlyTotalPrecipitation.
                        This fixture is used as an indirect fixture for parametrized tests.
                        """
                        if request.param in [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature]:
                            observations = [Obs(variable=var_temp_point, history=history_stn1_hourly,
                                                time=datetime.datetime(2000, 1, day, hour), datum=float(hour))
                                            for day in days for hour in hours]
                        else:
                            observations = [Obs(variable=var_precip_net1_1, history=history_stn1_hourly,
                                                time=datetime.datetime(2000, 1, day, hour), datum=1.0)
                                            for day in days for hour in hours]
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @fixture
                    def variable(request, var_temp_point, var_precip_net1_1):
                        """Yield a Variable object (which is a fixture), dependent on the value of request.param:
                         MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, or MonthlyTotalPrecipitation.
                        The variable yielded is the one expected for the type of view/test indicated by request.param.
                        This fixture is used as an indirect fixture for parametrized tests.
                        """
                        variable = {
                            MonthlyAverageOfDailyMaxTemperature: var_temp_point,
                            MonthlyAverageOfDailyMinTemperature: var_temp_point,
                            MonthlyTotalPrecipitation: var_precip_net1_1,
                        }[request.param]
                        yield variable

                    @mark.parametrize('View, obs_sesh', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_a_single_row(View, obs_sesh):
                        refresh_views(obs_sesh)
                        assert obs_sesh.query(View).count() == 1

                    @mark.parametrize('View, obs_sesh, variable', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh', 'variable'], ids=id)
                    def it_returns_the_expected_history_variable_and_day(View, obs_sesh, history_stn1_hourly, variable):
                        refresh_views(obs_sesh)
                        result = obs_sesh.query(View).first()
                        assert result.history_id == history_stn1_hourly.id
                        assert result.vars_id == variable.id
                        assert result.obs_month == datetime.datetime(2000, 1, 1)

                    @mark.parametrize('View, obs_sesh, statistic', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature, max(hours)),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature, min(hours)),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation, float(len(days) * len(hours))),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_the_expected_extreme_value(View, obs_sesh, statistic):
                        refresh_views(obs_sesh)
                        assert obs_sesh.query(View).first().statistic == statistic

                    @mark.parametrize('View, obs_sesh', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_the_expected_data_coverage(View, obs_sesh):
                        refresh_views(obs_sesh)
                        assert obs_sesh.query(View).first().data_coverage == approx(len(hours)/24.0 * len(days)/31.0)

            def describe_with_many_variables():

                @fixture
                def variable_sesh(history_sesh, var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo):
                    for sesh in generic_sesh(history_sesh ,
                                             [var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo]):
                        yield sesh

                def describe_with_many_observations_per_variable():

                    days = range(1, 32, 2)
                    hours = range(1, 24, 2)

                    @fixture
                    def obs_sesh(request, variable_sesh, history_stn1_hourly,
                                 var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo,
                                 var_precip_net1_1, var_precip_net1_2):
                        """Yield a session with particular observations added to variable_sesh.
                        Observations added depend on the value of request.param: MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature, or MonthlyTotalPrecipitation.
                        This fixture is used as an indirect fixture for parametrized tests.
                        """
                        if request.param in [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature]:
                            observations = [
                                Obs(variable=var, history=history_stn1_hourly,
                                    time=datetime.datetime(2000, 1, day, hour), datum=float(hour))
                                for var in [var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo]
                                for day in days for hour in hours
                            ]
                        else:
                            observations = [
                                Obs(variable=var, history=history_stn1_hourly,
                                    time=datetime.datetime(2000, 1, day, hour), datum=1.0)
                                for var in [var_precip_net1_1, var_precip_net1_2, var_temp_point, var_foo]
                                for day in days for hour in hours
                            ]
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @mark.parametrize('View, obs_sesh', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_exactly_the_expected_variables(
                            View, obs_sesh,
                            var_temp_point, var_temp_max, var_temp_min, var_temp_mean,
                            var_precip_net1_1, var_precip_net1_2
                    ):
                        refresh_views(obs_sesh)
                        expected_variables = {
                            MonthlyAverageOfDailyMaxTemperature: {var_temp_point.id, var_temp_max.id, var_temp_mean.id},
                            MonthlyAverageOfDailyMinTemperature: {var_temp_point.id, var_temp_min.id, var_temp_mean.id},
                            MonthlyTotalPrecipitation: {var_precip_net1_1.id, var_precip_net1_2.id},
                        }
                        assert set([r.vars_id for r in obs_sesh.query(View)]) == expected_variables[View]

        def describe_with_1_history_daily():

            @fixture
            def history_sesh(station_sesh, history_stn1_daily):
                for sesh in generic_sesh(station_sesh , [history_stn1_daily]):
                    yield sesh

            def describe_with_1_variable():

                @fixture
                def variable_sesh(history_sesh, var_temp_point):
                    for sesh in generic_sesh(history_sesh , [var_temp_point]):
                        yield sesh

                def describe_with_many_observations_on_different_days():

                    months = [4, 6, 9, 11]
                    days = range(1, 5)

                    @fixture
                    def obs_sesh(request, variable_sesh, history_stn1_daily, var_temp_point, var_precip_net1_1):
                        if request.param in [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature]:
                            observations = [
                                Obs(variable=var_temp_point, history=history_stn1_daily,
                                    time=datetime.datetime(2000, month, day, 12), datum=float(day))
                                for month in months
                                for day in days
                            ]
                        else:
                            observations = [
                                Obs(variable=var_precip_net1_1, history=history_stn1_daily,
                                    time=datetime.datetime(2000, month, day, 12), datum=1.0)
                                for month in months
                                for day in days
                            ]
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @mark.parametrize('View, obs_sesh', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_the_expected_number_of_rows(View, obs_sesh):
                        refresh_views(obs_sesh)
                        assert obs_sesh.query(View).count() == len(months)

                    @mark.parametrize('View, obs_sesh', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_the_expected_months(View, obs_sesh):
                        refresh_views(obs_sesh)
                        assert set([r.obs_month for r in obs_sesh.query(View)]) == \
                               set([datetime.datetime(2000, month, 1) for month in months])

                    @mark.parametrize('View, obs_sesh', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_the_expected_coverage(View, obs_sesh):
                        refresh_views(obs_sesh)
                        assert all(map(lambda r: r.data_coverage == approx(len(days)/30.0),
                                       obs_sesh.query(View)))

def describe_with_2_networks():

    @fixture
    def network_sesh(with_views_sesh, network1, network2):
        for sesh in generic_sesh(with_views_sesh , [network1, network2]):
            yield sesh

    def describe_with_1_station_per_network():

        @fixture
        def station_sesh(network_sesh, station1, station2):
            for sesh in generic_sesh(network_sesh , [station1, station2]):
                yield sesh

        def describe_with_1_history_hourly_per_station():

            @fixture
            def history_sesh(station_sesh, history_stn1_hourly, history_stn2_hourly):
                for sesh in generic_sesh(station_sesh , [history_stn1_hourly, history_stn2_hourly]):
                    yield sesh

            def describe_with_1_variable_per_network(): # temp: point

                @fixture
                def variable_sesh(history_sesh, var_temp_point, var_temp_point2):
                    for sesh in generic_sesh(history_sesh , [var_temp_point, var_temp_point2]):
                        yield sesh

                def describe_with_observations_for_each_station_variable():

                    months = range(1, 4)
                    days = range(1, 10)
                    hours = range(4, 20)

                    @fixture
                    def obs_sesh(request, variable_sesh, history_stn1_hourly, history_stn2_hourly,
                                 var_temp_point, var_temp_point2,
                                 var_precip_net1_1, var_precip_net2_1):
                        if request.param in [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature]:
                            observations = [
                                Obs(variable=var, history=hx,
                                    time=datetime.datetime(2000, month, day, hour), datum=float(hour))
                                for (var, hx) in [(var_temp_point, history_stn1_hourly),
                                                  (var_temp_point2, history_stn2_hourly)]
                                for month in months
                                for day in days
                                for hour in hours
                            ]
                        else:
                            observations = [
                                Obs(variable=var, history=hx,
                                    time=datetime.datetime(2000, month, day, hour), datum=1.0)
                                for (var, hx) in [(var_precip_net1_1, history_stn1_hourly),
                                                  (var_precip_net2_1, history_stn2_hourly)]
                                for month in months
                                for day in days
                                for hour in hours
                            ]
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @mark.parametrize('View, obs_sesh', [
                        (MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMaxTemperature),
                        (MonthlyAverageOfDailyMinTemperature, MonthlyAverageOfDailyMinTemperature),
                        (MonthlyTotalPrecipitation, MonthlyTotalPrecipitation),
                    ], indirect=['obs_sesh'], ids=id)
                    def it_returns_one_row_per_unique_combo_hx_var_month(
                            View, obs_sesh,
                            history_stn1_hourly, history_stn2_hourly,
                            var_temp_point, var_temp_point2,
                            var_precip_net1_1, var_precip_net2_1):
                        refresh_views(obs_sesh)
                        if View in [MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature]:
                            var_stn = [(var_temp_point, history_stn1_hourly),
                                       (var_temp_point2, history_stn2_hourly)]
                        else:
                            var_stn = [(var_precip_net1_1, history_stn1_hourly),
                                       (var_precip_net2_1, history_stn2_hourly)]
                        assert set([(r.history_id, r.vars_id, r.obs_month)
                                    for r in obs_sesh.query(View)]) == \
                               set([(stn.id, var.id, datetime.datetime(2000, month, 1))
                                    for (var, stn) in var_stn
                                    for month in months])
