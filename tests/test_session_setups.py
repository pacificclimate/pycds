from sqlalchemy import inspect

from pycds import Base, Network
from pycds.util import get_search_path, reset_search_path, set_search_path

# Helpers. Note argument names with trailing underscores to distinguish them
# from fixtures of the similar name.

# TODO: Move get_* into util?

def get_engine_inspector(session_):
    return inspect(session_.get_bind())


def get_schema_names(session_):
    return get_engine_inspector(session_).get_schema_names()


def get_table_names(session_, schema_name_):
    return get_engine_inspector(session_).get_table_names(schema=schema_name_)


def check_search_path(session_):
    assert get_search_path(session_) == ['public']


def check_schema_configuration(schema_name_):
    # This test will, *correctly*, fail if (a) no schema name is set, or
    # (b) the wrong schema name is set.
    # There are two ways this can occur: (a) no fixture has been run that
    # configures schema name, (b) a fixture has been run that configures
    # the wrong schema name.
    assert Base.metadata.schema == schema_name_
    assert Network.metadata.schema == schema_name_
    assert Network.__table__.schema == None


def check_schemas(session_, schema_name_):
    assert schema_name_ in get_schema_names(session_)


def check_table_schema(session_, schema_name_, table_names=('meta_network',)):
    assert set(table_names) <= set(get_table_names(session_, schema_name_))


def reset(session_):
    reset_search_path(session_)
    Base.metadata.schema = None


def check_fixture(session_, schema_name_):
    check_search_path(session_)
    check_schema_configuration(schema_name_)
    check_schemas(session_, schema_name_)
    check_table_schema(session_, schema_name_)
    reset(session_)


def check_reset():
    check_schema_configuration(None)


# Tests

def describe_database_session_fixtures():
    # Check that each database session fixture configures things related to
    # schemas correctly. Code is duplicated here because each of several
    # different fixtures must be subjected to the same tests, and pytest
    # doesn't make it easy to parametrize over fixtures.
    #
    # We do NOT test the following database fixtures, because they nominally
    # do not do the full setup:
    #   engine
    #   mod_blank_postgis_session
    #   blank_postgis_session

    def fixture_session(session, schema_name):
        check_fixture(session, schema_name)

    def fixture_per__test__session(per_test_session, schema_name):
        check_fixture(per_test_session, schema_name)

    def fixture_mod__empty__database__session(
            mod_empty_database_session, schema_name
    ):
        check_fixture(mod_empty_database_session, schema_name)

    def fixture_test__session(test_session, schema_name):
        check_fixture(test_session, schema_name)

    def fixture_large__test__session(large_test_session, schema_name):
        check_fixture(large_test_session, schema_name)
