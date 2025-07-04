from pytest import fixture
from sqlalchemy.orm import sessionmaker
from pycds import Contact, Network, Station, History, Variable


@fixture
def empty_sesh(base_engine):
    """Test-function scoped database session, with no schema or content.
    All session actions are rolled back on teardown.
    """
    sesh = sessionmaker(bind=base_engine)()
    yield sesh
    sesh.rollback()
    sesh.close()


@fixture
def pycds_sesh_with_small_data(pycds_sesh):
    # TODO: Use add_then_delete_objs (which ought to be renamed) here so that objects
    #   are removed after test
    moti = Network(name="MoTIe")
    ec = Network(name="EC")
    wmb = Network(name="FLNROW-WMB")
    pycds_sesh.add_all([moti, ec, wmb])

    simon = Contact(name="Simon", networks=[moti])
    eric = Contact(name="Eric", networks=[wmb])
    pat = Contact(name="Pat", networks=[ec])
    pycds_sesh.add_all([simon, eric, pat])

    stations = [
        Station(
            native_id="11091",
            network=moti,
            histories=[
                History(
                    station_name="Brandywine",
                    the_geom="SRID=4326;POINT(-123.11806 50.05417)",
                )
            ],
        ),
        Station(
            native_id="1029",
            network=wmb,
            histories=[
                History(
                    station_name="FIVE MILE",
                    the_geom="SRID=4326;POINT(-122.68889 50.91089)",
                )
            ],
        ),
        Station(
            native_id="2100160",
            network=ec,
            histories=[
                History(
                    station_name="Beaver Creek Airport",
                    the_geom="SRID=4326;POINT(-140.866667 62.416667)",
                )
            ],
        ),
    ]
    pycds_sesh.add_all(stations)

    variables = [
        Variable(
            name="CURRENT_AIR_TEMPERATURE1",
            unit="celsius",
            standard_name="foo: bar",
            cell_method="foo_bar",
            display_name="foo bar",
            network=moti,
        ),
        Variable(
            name="precipitation",
            unit="mm",
            standard_name="foo: bar",
            cell_method="foo_bar",
            display_name="foo bar",
            network=ec,
        ),
        Variable(
            name="relative_humidity",
            unit="percent",
            standard_name="foo: bar",
            cell_method="foo_bar",
            display_name="foo bar",
            network=wmb,
        ),
    ]
    pycds_sesh.add_all(variables)

    yield pycds_sesh
