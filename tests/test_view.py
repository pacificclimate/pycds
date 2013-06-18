from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pycds

def test_crmp_network_geoserver():
    connection_string = 'postgresql://hiebert@monsoon.pcic.uvic.ca/crmp?sslmode=require'
    engine = create_engine(connection_string, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    cng = pycds.CrmpNetworkGeoserver.network_name
    q = session.query(cng)
    rv = q.all()

    # Test that the number of rows is not zero
    nrows = len(rv)
    assert nrows != 0

    # Select all rows from network FLNRO-WMB
    where_clause = "network_name = 'FLNRO-WMB'"
    q = q.filter(where_clause)
    rv = q.all()

    # Assert that number of rows is less
    filtered_nrows = len(rv)
    assert filtered_nrows < nrows
    nrows = filtered_nrows

    # Select all rows where max_obs_time is before 2000
    where_clause = "max_obs_time < '2000-01-01'"

    # Assert that number of rows is less
    rv = q.filter(where_clause).all()
    filtered_nrows = len(rv)
    assert filtered_nrows != 0
    assert filtered_nrows < nrows
