"""Helpers for tests."""

from importlib.resources import files
from collections import namedtuple
from datetime import datetime

from sqlalchemy import text

from pycds import get_schema_name, Contact, Network, Station, History, Variable

# Fixture helpers

# The following generators abstract behavior common to many fixtures in this
# test suite. The behaviour pattern is:
#
#   def behaviour(session, ...)
#       setup(session, ...)
#       yield session
#       teardown(session, ...)
#
# Two examples of this pattern are
#
#   setup = add database objects to session
#   teardown = remove database objects from session
#
# and
#
#   setup = create views in session
#   teardown = drop views in session
#
# To use such a generator correctly, i.e., so that the teardown after the
# yield is also performed, a fixture must first yield the result of
# `next(behaviour)`, then call `next(behaviour)` again. This can be done
# in two ways:
#
#   g = behaviour(...)
#   yield next(g)
#   next(g)
#
# or, shorter and clearer:
#
#   for sesh in behaviour(...):
#       yield sesh
#
# The shorter method is preferred.


def add_then_delete_objs(sesh, sa_objects):
    """Add objects to session, yield session, drop objects from session (in
    reverse order. For correct usage, see notes above.

    Args:
        sesh (sqlalchemy.orm.session.Session): database session

        sa_objects: List of SQLAlchemy ORM objects to be added to database
            for setup and removed on teardown. Order within list is respected
            for setup and teardown, so that dependencies are respected.

    Returns:
        yields sesh after setup
    """
    for sao in sa_objects:
        sesh.add(sao)
        sesh.flush()
    yield sesh
    for sao in reversed(sa_objects):
        sesh.delete(sao)
        sesh.flush()


def create_then_drop_views(sesh, views):
    """Create views in session, yield session, drop views in session (in
    reverse order). For correct usage, see notes above.

    Args:
        sesh (sqlalchemy.orm.session.Session): database session

        views: List of views created in database on setup and dropped
            on teardown. Order within list is respected for setup and
            teardown, so that dependencies can be respected.

    Returns:
        yields sesh after setup

    """
    for view in views:
        sesh.execute(view.create())
        sesh.flush()
    yield sesh
    for view in reversed(views):
        sesh.execute(view.drop())
        sesh.flush()


# Data insertion helpers


def with_schema_name(sesh, schema_name, action):
    """Execute an action with the search path set to a specified schema name.
    Restore existing search path after action.
    """
    old_search_path = sesh.execute(text("SHOW search_path")).scalar()
    sesh.execute(text(f"SET search_path TO {schema_name}, public"))
    action(sesh)
    sesh.execute(text(f"SET search_path TO {old_search_path}"))


# Shorthand for defining various database objects

TestContact = namedtuple("TestContact", "name title organization email phone")
TestNetwork = namedtuple("TestNetwork", "name long_name color")
TestStation = namedtuple("TestStation", "native_id network histories")
TestHistory = namedtuple(
    "TestHistory", "station_name elevation sdate edate province country freq"
)
TestVariable = namedtuple(
    "TestVariable",
    "name unit standard_name cell_method precision description display_name "
    "short_name network",
)


def insert_test_data(sesh, schema_name=get_schema_name()):
    """Insert a small-ish set of test data"""

    def action(sesh):
        moti = Network(
            **TestNetwork(
                "MOTI",
                "Ministry of Transportation and Infrastructure",
                "000000",
            )._asdict()
        )
        moe = Network(
            **TestNetwork(
                "MOTI",
                "Ministry of Transportation and Infrastructure",
                "000000",
            )._asdict()
        )
        sesh.add_all([moti, moe])

        simon = Contact(
            **TestContact(
                "Simon",
                "Avalanche Guy",
                "MOTI",
                "simn@moti.bc.gov.ca",
                "250-555-1212",
            )._asdict()
        )
        simon.networks = [moti]
        ted = Contact(
            **TestContact(
                "Ted",
                "Air Quailty Guy",
                "MOE",
                "ted@moti.bc.gov.ca",
                "250-555-2121",
            )._asdict()
        )
        ted.networks = [moe]
        sesh.add_all([simon, ted])

        histories = [
            TestHistory(
                "Brandywine",
                496,
                datetime(2001, 1, 22, 13),
                datetime(2011, 4, 6, 11),
                "BC",
                "Canada",
                "1-hourly",
            ),
            TestHistory(
                "Stewart",
                15,
                datetime(2004, 1, 22, 13),
                datetime(2011, 4, 6, 11),
                "BC",
                "Canada",
                "1-hourly",
            ),
            TestHistory(
                "Cayoosh Summit",
                1350,
                datetime(1997, 1, 22, 13),
                datetime(2011, 4, 6, 11),
                "BC",
                "Canada",
                "1-hourly",
            ),
            TestHistory(
                "Boston Bar RCMP Station",
                180,
                datetime(1999, 1, 22, 13),
                datetime(2002, 4, 6, 11),
                "BC",
                "Canada",
                "1-hourly",
            ),
            TestHistory(
                "Prince Rupert",
                35,
                datetime(1990, 1, 22, 13),
                datetime(1996, 4, 6, 11),
                "BC",
                "Canada",
                "1-hourly",
            ),
            TestHistory(
                "Prince Rupert",
                36,
                datetime(1997, 1, 22, 13),
                None,
                "BC",
                "Canada",
                "1-hourly",
            ),
        ]
        histories = [History(**hist._asdict()) for hist in histories]
        sesh.add_all(histories)

        stations = [
            TestStation("11091", moti, [histories[0]]),
            TestStation("51129", moti, [histories[1]]),
            TestStation("26224", moti, [histories[2]]),
            TestStation("E238240", moe, [histories[3]]),
            TestStation("M106037", moe, histories[4:6]),
        ]

        for station in stations:
            sesh.add(Station(**station._asdict()))

        variables = [
            TestVariable(
                "air_temperature",
                "degC",
                "air_temperature",
                "time: point",
                None,
                "Instantaneous air temperature",
                "Temperature (Point)",
                "",
                moti,
            ),
            TestVariable(
                "average_direction",
                "km/h",
                "wind_from_direction",
                "time: mean",
                None,
                "Hourly average wind direction",
                "Wind Direction (Mean)",
                "",
                moti,
            ),
            TestVariable(
                "dew_point",
                "degC",
                "dew_point_temperature",
                "time: point",
                None,
                "",
                "Dew Point Temperature (Mean)",
                "",
                moti,
            ),
            TestVariable(
                "BAR_PRESS_HOUR",
                "millibar",
                "air_pressure",
                "time:point",
                None,
                "Instantaneous air pressure",
                "Air Pressure (Point)",
                "",
                moe,
            ),
        ]

        for variable in variables:
            sesh.add(Variable(**variable._asdict()))

    with_schema_name(sesh, schema_name, action)


def insert_crmp_data(sesh, schema_name=get_schema_name()):
    """Insert data from CRMP database dump into into tables in named schema."""

    def action(sesh):
        with files("pycds").joinpath("data/crmp_subset_data.sql").open("r") as f:
            data = f.read()
        sesh.execute(text(data))

    with_schema_name(sesh, schema_name, action)
