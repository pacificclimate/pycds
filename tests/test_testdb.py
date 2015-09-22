from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pycds.util import create_test_database, create_test_data
from pycds import Contact

def test_can_create_postgis_db(blank_postgis_session):
    res = blank_postgis_session.execute("SELECT PostGIS_full_version()")
    assert 'POSTGIS="2.1' in res.fetchall()[0][0]

def test_can_create_postgis_geometry_table(blank_postgis_session):
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String
    from sqlalchemy.orm import sessionmaker
    from geoalchemy2 import Geometry

    Base = declarative_base()
    class Lake(Base):
        __tablename__ = 'lake'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        geom = Column(Geometry('POLYGON'))

    Lake.__table__.create(blank_postgis_session.get_bind())

def test_can_create_test_db():
    engine = create_engine('sqlite://')
    create_test_database(engine)
    create_test_data(engine)
    # Get some data
    sesh = sessionmaker(bind=engine)()
    q = sesh.query(Contact)
    assert len(q.all()) == 2
    
