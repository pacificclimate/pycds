import datetime
import pytest
import pycds
from pycds import Network, Station, History, Variable, Obs
from pycds import DailyMaxTemperature

# TODO: Remove print statements and other cruft

def describe_DailyMaxTemperature():
    def describe_with_minimal_network_and_hourly_reporting():

        # Set up 1 network with 1 variable and 1 station with 1 history record (hourly reporting)

        @pytest.fixture(scope='module')
        def minimal_network_objects():
            network1 = Network(id=1, name='Network 1')
            station1 = Station(id=1, network_id=network1.id)
            history1 = History(id=1, station_id=station1.id, station_name='Station 1',
                               sdate=datetime.datetime.min, edate=None, freq='1-hourly')
            variable1 = Variable(id=1, network_id=network1.id,
                                 standard_name='air_temperature', cell_method='time: maximum')
            return network1, station1, history1, variable1

        @pytest.fixture(scope='function')
        def minimal_network_session(mod_empty_database_session, minimal_network_objects):
            sesh = mod_empty_database_session
            network1, station1, history1, variable1 = minimal_network_objects
            sesh.add_all([network1])
            sesh.flush()
            sesh.add_all([station1])
            sesh.add_all([variable1])
            sesh.flush()
            sesh.add_all([history1])
            sesh.flush()
            yield sesh

        # @pytest.fixture(scope='module', autouse=True)
        # def transact():
        #     print('\n#### begin transaction')
        #     yield
        #     print('\n#### end transaction')


        # TODO: Remove; redundant with several obs in one day
        # def describe_with_one_observation():
        #
        #     # Extend minimal network with a single observation
        #     @pytest.fixture(scope='function')
        #     def minimal_observation_session(minimal_network_session, minimal_network_objects):
        #         print('\n#### minimal_observation_session: setup')
        #         sesh = minimal_network_session
        #         network1, station1, history1, variable1 = minimal_network_objects
        #
        #         obs1 = Obs(id=1, vars_id=variable1.id, history_id=history1.id,
        #                    time=datetime.datetime(2000, 1, 1, 12, 34, 56), datum=5.0)
        #         sesh.add_all([obs1])
        #         sesh.flush()
        #         yield sesh
        #         print('\n#### minimal_observation_session: teardown')
        #         sesh.query(Obs).delete()
        #         sesh.flush()
        #
        #     @pytest.fixture(scope='function')
        #     def results(minimal_observation_session):
        #         # print('\n### results minimal')
        #         return minimal_observation_session.query(DailyMaxTemperature)
        #
        #     def it_returns_a_single_row(results):
        #         assert(results.count() == 1)
        #
        #     def it_returns_the_expected_station_variable_and_day(results):
        #         result = results.first()
        #         assert(result.station_id == 1)
        #         assert(result.vars_id == 1)
        #         assert(result.obs_day == datetime.datetime(2000, 1, 1))
        #
        #     def it_returns_the_expected_maximum_value(results):
        #         assert(results.first().statistic == 5.0)
        #
        #     def it_returns_the_expected_data_coverage(results):
        #         assert(float(results.first().data_coverage) == 1.0 / 24.0)

        def describe_with_several_observations_in_one_day():

            @pytest.fixture(scope='function')
            def several_observations_session(minimal_network_session, minimal_network_objects):
                print('\n#### several_observations_session: setup')
                sesh = minimal_network_session
                network1, station1, history1, variable1 = minimal_network_objects
                sesh.add_all([
                    (Obs(id=1, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 1, 12), datum=1.0)),
                    (Obs(id=2, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 1, 13), datum=2.0)),
                    (Obs(id=3, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 1, 14), datum=3.0))
                ])
                sesh.flush()
                yield sesh
                print('\n#### several_observations_session: teardown')
                sesh.query(Obs).delete()
                sesh.flush()

            @pytest.fixture(scope='function')
            def results(several_observations_session):
                # print('\n### results several')
                return several_observations_session.query(DailyMaxTemperature)

            def it_returns_a_single_row(results):
                assert(results.count() == 1)

            def it_returns_the_expected_station_variable_and_day(results):
                result = results.first()
                assert(result.station_id == 1)
                assert(result.vars_id == 1)
                assert(result.obs_day == datetime.datetime(2000, 1, 1))

            def it_returns_the_expected_maximum_value(results):
                assert(results.first().statistic == 3.0)

            def it_returns_the_expected_data_coverage(results):
                assert(float(results.first().data_coverage) == 3.0 / 24.0)

        def describe_with_several_observations_on_two_different_days():

            @pytest.fixture(scope='function')
            def several_observations_session(minimal_network_session, minimal_network_objects):
                print('\n#### several_observations_session (2): setup')
                sesh = minimal_network_session
                network1, station1, history1, variable1 = minimal_network_objects
                sesh.add_all([
                    (Obs(id=1, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 1, 12), datum=1.0)),
                    (Obs(id=2, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 1, 13), datum=2.0)),
                    (Obs(id=3, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 1, 14), datum=3.0)),
                    (Obs(id=4, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 2, 12), datum=4.0)),
                    (Obs(id=5, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 2, 13), datum=5.0)),
                    (Obs(id=6, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 2, 14), datum=6.0)),
                    (Obs(id=7, vars_id=variable1.id, history_id=history1.id,
                         time=datetime.datetime(2000, 1, 2, 15), datum=7.0))
                ])
                sesh.flush()
                yield sesh
                print('\n#### several_observations_session (2): teardown')
                sesh.query(Obs).delete()
                sesh.flush()

            @pytest.fixture(scope='function')
            def results(several_observations_session):
                print('\n### results several (2)')
                return several_observations_session.query(DailyMaxTemperature)#.order_by(DailyMaxTemperature.obs_day)

            def it_returns_two_rows(results):
                assert(results.count() == 2)

            def it_returns_the_expected_station_variables(results):
                for result in results:
                    assert(result.station_id == 1)
                    assert(result.vars_id == 1)

            def it_returns_the_expected_days(results):       
                assert([r.obs_day for r in results] == [datetime.datetime(2000, 1, 1), datetime.datetime(2000, 1, 2)])

            def it_returns_the_expected_max_values(results):
                assert([r.statistic for r in results] == [3.0, 7.0])

            def it_returns_the_expected_data_coverages(results):
                assert([float(r.data_coverage) for r in results] == [3.0/24.0, 4.0/24.0])
