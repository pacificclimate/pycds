import datetime

from pytest import fixture, mark, approx

from pycds.util import generic_sesh
from pycds import Network, Station, History, Variable, Obs
from pycds.weather_anomaly import DailyMaxTemperature, DailyMinTemperature
from pycds.weather_anomaly import MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature


views = [DailyMaxTemperature, DailyMinTemperature,
         MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature]

@fixture(scope='module')
def with_views_sesh(mod_empty_database_session):
    sesh = mod_empty_database_session
    for view in views:
        view.create(sesh)
    yield sesh
    for view in reversed(views):
        view.drop(sesh)


def refresh_views(sesh):
    for view in views:
        view.refresh(sesh)


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

                def describe_with_a_full_set_of_observations_for_one_month():

                    days = range(1, 32)
                    hours = range(1, 24)

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                        observations = []
                        id = 0
                        for day in days:
                            for hour in hours:
                                id += 1
                                observations.append(
                                    Obs(id=id, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                        time=datetime.datetime(2000, 1, day, hour), datum=float(hour))
                                )
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_a_single_row(query, MonthlyAvgOfDailyExtremeTemperature):
                        assert query(MonthlyAvgOfDailyExtremeTemperature).count() == 1

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_the_expected_station_variable_and_day(
                            query, MonthlyAvgOfDailyExtremeTemperature, history_stn1_hourly, var_temp_point):
                        result = query(MonthlyAvgOfDailyExtremeTemperature).first()
                        assert result.history_id == history_stn1_hourly.id
                        assert result.vars_id == var_temp_point.id
                        assert result.obs_month == datetime.datetime(2000, 1, 1)

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature, statistic', [
                        (MonthlyAverageOfDailyMaxTemperature, max(hours)),
                        (MonthlyAverageOfDailyMinTemperature, min(hours))
                    ])
                    def it_returns_the_expected_extreme_value(query, MonthlyAvgOfDailyExtremeTemperature, statistic):
                        assert query(MonthlyAvgOfDailyExtremeTemperature).first().statistic == statistic

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_the_expected_data_coverage(query, MonthlyAvgOfDailyExtremeTemperature):
                        assert query(MonthlyAvgOfDailyExtremeTemperature).first().data_coverage == \
                               approx(len(hours)/24.0 * len(days)/31.0)

                def describe_with_a_partial_set_of_observations_for_one_month():

                    days = range(1, 32, 2)
                    hours = range(1, 24, 2)

                    @fixture
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly):
                        observations = []
                        id = 0
                        for day in days:
                            for hour in hours:
                                id += 1
                                observations.append(
                                    Obs(id=id, vars_id=var_temp_point.id, history_id=history_stn1_hourly.id,
                                        time=datetime.datetime(2000, 1, day, hour), datum=float(hour))
                                )
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature, statistic', [
                        (MonthlyAverageOfDailyMaxTemperature, max(hours)),
                        (MonthlyAverageOfDailyMinTemperature, min(hours))
                    ])
                    def it_returns_the_expected_extreme_value(query, MonthlyAvgOfDailyExtremeTemperature, statistic):
                        assert query(MonthlyAvgOfDailyExtremeTemperature).first().statistic == statistic

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_the_expected_data_coverage(query, MonthlyAvgOfDailyExtremeTemperature):
                        assert query(MonthlyAvgOfDailyExtremeTemperature).first().data_coverage == \
                               approx(len(hours)/24.0 * len(days)/31.0)

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
                    def obs_sesh(variable_sesh, history_stn1_hourly,
                                 var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo):
                        observations = []
                        id = 0
                        for var in [var_temp_point, var_temp_max, var_temp_min, var_temp_mean, var_foo]:
                            for day in days:
                                for hour in hours:
                                    id += 1
                                    observations.append(
                                        Obs(id=id, vars_id=var.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, day, hour), datum=float(hour))
                                    )
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_exactly_the_expected_variables(
                            query, MonthlyAvgOfDailyExtremeTemperature,
                            var_temp_point, var_temp_max, var_temp_min, var_temp_mean
                    ):
                        expected_variables = {
                            MonthlyAverageOfDailyMaxTemperature: {var_temp_point.id, var_temp_max.id, var_temp_mean.id},
                            MonthlyAverageOfDailyMinTemperature: {var_temp_point.id, var_temp_min.id, var_temp_mean.id},
                        }
                        assert set([r.vars_id for r in query(MonthlyAvgOfDailyExtremeTemperature)]) == \
                               expected_variables[MonthlyAvgOfDailyExtremeTemperature]

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
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_daily):
                        observations = []
                        id = 0
                        for month in months:
                            for day in days:
                                id += 1
                                observations.append(
                                    Obs(id=id + 100, vars_id=var_temp_point.id, history_id=history_stn1_daily.id,
                                        time=datetime.datetime(2000, month, day, 12), datum=float(id))
                                )
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_the_expected_number_of_rows(query, MonthlyAvgOfDailyExtremeTemperature):
                        assert query(MonthlyAvgOfDailyExtremeTemperature).count() == len(months)

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_the_expected_months(query, MonthlyAvgOfDailyExtremeTemperature):
                        assert set([r.obs_month for r in query(MonthlyAvgOfDailyExtremeTemperature)]) == \
                               set([datetime.datetime(2000, month, 1) for month in months])

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_the_expected_coverage(query, MonthlyAvgOfDailyExtremeTemperature):
                        assert all(map(lambda r: r.data_coverage == approx(len(days)/30.0),
                                       query(MonthlyAvgOfDailyExtremeTemperature)))

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
                    def obs_sesh(variable_sesh, var_temp_point, history_stn1_hourly, var_temp_point2, history_stn2_hourly):
                        observations = []
                        id = 0
                        for (var, hx) in [(var_temp_point, history_stn1_hourly), (var_temp_point2, history_stn2_hourly)]:
                            for month in months:
                                for day in days:
                                    for hour in hours:
                                        id += 1
                                        observations.append(
                                            Obs(id=id, vars_id=var.id, history_id=hx.id,
                                                time=datetime.datetime(2000, month, day, hour), datum=float(id))
                                        )
                        for sesh in generic_sesh(variable_sesh , observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    @mark.parametrize('MonthlyAvgOfDailyExtremeTemperature', [
                        MonthlyAverageOfDailyMaxTemperature, MonthlyAverageOfDailyMinTemperature
                    ])
                    def it_returns_one_row_per_unique_combo_hx_var_month(
                            query, MonthlyAvgOfDailyExtremeTemperature,
                            var_temp_point, history_stn1_hourly, var_temp_point2, history_stn2_hourly):
                        assert set([(r.history_id, r.vars_id, r.obs_month)
                                    for r in query(MonthlyAvgOfDailyExtremeTemperature)]) == \
                               set([(stn.id, var.id, datetime.datetime(2000, month, 1))
                                    for (var, stn) in [(var_temp_point, history_stn1_hourly),
                                                       (var_temp_point2, history_stn2_hourly)]
                                    for month in months])
