from pytest import fixture
import pycds
from pycds import Network
from pycds.climate_baseline_helpers import create_PCIC_derived_variable_network

@fixture
def empty_database_session(mod_blank_postgis_session):
    sesh = mod_blank_postgis_session
    engine = sesh.get_bind()
    pycds.Base.metadata.create_all(bind=engine)
    yield sesh

def describe_create__PCIC__derived__variable__network():

    name = 'PCIC Derived Variables'

    def test_creates_the_expected_new_network_record(empty_database_session):
        sesh = empty_database_session
        create_PCIC_derived_variable_network(sesh)
        results = sesh.query(Network).filter(Network.name == name)
        assert results.count() == 1
        result = results.first()
        assert result.publish == False
        assert 'PCIC' in result.long_name

    def test_creates_at_most_one_of_them(empty_database_session):
        sesh = empty_database_session
        create_PCIC_derived_variable_network(sesh)
        create_PCIC_derived_variable_network(sesh)
        results = sesh.query(Network).filter(Network.name == name)
        assert results.count() == 1
