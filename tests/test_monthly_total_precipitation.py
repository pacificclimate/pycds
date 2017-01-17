import datetime

from pytest import fixture, mark, approx

from pycds.util import generic_sesh
from pycds import Network, Station, History, Variable, Obs, NativeFlag, PCICFlag
from pycds.weather_anomaly import MonthlyTotalPrecipitation


views = [MonthlyTotalPrecipitation]


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
                def variable_sesh(history_sesh, var_precip_net1_1):
                    for sesh in generic_sesh(history_sesh, [var_precip_net1_1]):
                        yield sesh

                def describe_with_observations_for_a_month():

                    days = range(1, 32)
                    hours = range(1, 24)

                    @fixture
                    def obs_sesh(variable_sesh, var_precip_net1_1, history_stn1_hourly):
                        observations = []
                        id = 0
                        for day in days:
                            for hour in hours:
                                id += 1
                                observations.append(
                                    Obs(id=id, vars_id=var_precip_net1_1.id, history_id=history_stn1_hourly.id,
                                        time=datetime.datetime(2000, 1, day, hour), datum=1.0)
                                )
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    def it_returns_a_single_row(query):
                        assert query(MonthlyTotalPrecipitation).count() == 1

                    def it_returns_the_expected_station_variable_and_month(
                            query, history_stn1_hourly, var_precip_net1_1):
                        result = query(MonthlyTotalPrecipitation).first()
                        assert result.history_id == history_stn1_hourly.id
                        assert result.vars_id == var_precip_net1_1.id
                        assert result.obs_month == datetime.datetime(2000, 1, 1)

                    def it_returns_the_expected_total_precip(query):
                        assert query(MonthlyTotalPrecipitation).first().statistic == 1.0 * len(days) * len(hours)

                    def it_returns_the_expected_data_coverage(query):
                        assert query(MonthlyTotalPrecipitation).first().data_coverage == \
                               approx(len(hours)/24.0 * len(days)/31.0)

            def describe_with_many_variables():

                @fixture
                def variable_sesh(history_sesh, var_precip_net1_1, var_precip_net1_2, var_temp_point, var_foo):
                    for sesh in generic_sesh(history_sesh, [
                        var_precip_net1_1, var_precip_net1_2, var_temp_point, var_foo
                    ]):
                        yield sesh

                def describe_with_many_observations_per_variable():

                    days = range(1, 32, 2)
                    hours = range(1, 24, 2)

                    @fixture
                    def obs_sesh(variable_sesh, history_stn1_hourly,
                                 var_precip_net1_1, var_precip_net1_2, var_temp_point, var_foo):
                        observations = []
                        id = 0
                        for var in [var_precip_net1_1, var_precip_net1_2, var_temp_point, var_foo]:
                            for day in days:
                                for hour in hours:
                                    id += 1
                                    observations.append(
                                        Obs(id=id, vars_id=var.id, history_id=history_stn1_hourly.id,
                                            time=datetime.datetime(2000, 1, day, hour), datum=1.0)
                                    )
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    def it_returns_exactly_the_expected_variables(
                            query, var_precip_net1_1, var_precip_net1_2
                    ):
                        assert set([r.vars_id for r in query(MonthlyTotalPrecipitation)]) == \
                               {var_precip_net1_1.id, var_precip_net1_2.id}

        def describe_with_1_history_daily():

            @fixture
            def history_sesh(station_sesh, history_stn1_daily):
                for sesh in generic_sesh(station_sesh, [history_stn1_daily]):
                    yield sesh

            def describe_with_1_variable():

                @fixture
                def variable_sesh(history_sesh, var_precip_net1_1):
                    for sesh in generic_sesh(history_sesh, [var_precip_net1_1]):
                        yield sesh

                def describe_with_many_observations_on_different_days():

                    months = [4, 6, 9, 11]
                    days = range(1, 5)

                    @fixture
                    def obs_sesh(variable_sesh, var_precip_net1_1, history_stn1_daily):
                        observations = []
                        id = 0
                        for month in months:
                            for day in days:
                                id += 1
                                observations.append(
                                    Obs(id=id + 100, vars_id=var_precip_net1_1.id, history_id=history_stn1_daily.id,
                                        time=datetime.datetime(2000, month, day, 12), datum=float(id))
                                )
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    def it_returns_the_expected_number_of_rows(query):
                        assert query(MonthlyTotalPrecipitation).count() == len(months)

                    def it_returns_the_expected_months(query):
                        assert set([r.obs_month for r in query(MonthlyTotalPrecipitation)]) == \
                               set([datetime.datetime(2000, month, 1) for month in months])

                    def it_returns_the_expected_coverage(query):
                        assert all(map(lambda r: r.data_coverage == approx(len(days)/30.0),
                                       query(MonthlyTotalPrecipitation)))

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

            def describe_with_1_variable_per_network():

                @fixture
                def variable_sesh(history_sesh, var_precip_net1_1, var_precip_net2_1):
                    for sesh in generic_sesh(history_sesh, [var_precip_net1_1, var_precip_net2_1]):
                        yield sesh

                def describe_with_observations_for_each_station_variable():

                    months = range(1, 4)
                    days = range(1, 10)
                    hours = range(4, 20)

                    @fixture
                    def obs_sesh(variable_sesh, var_precip_net1_1, history_stn1_hourly,
                                 var_precip_net2_1, history_stn2_hourly):
                        observations = []
                        id = 0
                        for (var, hx) in [(var_precip_net1_1, history_stn1_hourly),
                                          (var_precip_net2_1, history_stn2_hourly)]:
                            for month in months:
                                for day in days:
                                    for hour in hours:
                                        id += 1
                                        observations.append(
                                            Obs(id=id, vars_id=var.id, history_id=hx.id,
                                                time=datetime.datetime(2000, month, day, hour), datum=float(id))
                                        )
                        for sesh in generic_sesh(variable_sesh, observations):
                            yield sesh

                    @fixture
                    def query(obs_sesh):
                        refresh_views(obs_sesh)
                        return obs_sesh.query

                    def it_returns_one_row_per_unique_combo_hx_var_month(
                            query,
                            var_precip_net1_1, history_stn1_hourly, var_precip_net2_1, history_stn2_hourly):
                        assert set([(r.history_id, r.vars_id, r.obs_month) 
                                    for r in query(MonthlyTotalPrecipitation)]) == \
                               set([(stn.id, var.id, datetime.datetime(2000, month, 1))
                                    for (var, stn) in [(var_precip_net1_1, history_stn1_hourly), 
                                                       (var_precip_net2_1, history_stn2_hourly)]
                                    for month in months])
