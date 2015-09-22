from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pycds.util import create_test_database, create_test_data
from pycds import Contact

def test_can_create_postgresql_db():
    import testing.postgresql
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String, create_engine
    from sqlalchemy.orm import sessionmaker
    from geoalchemy2 import Geometry

    Base = declarative_base()
    class Lake(Base):
        __tablename__ = 'lake'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        geom = Column(Geometry('POLYGON'))

    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        sesh = sessionmaker(bind=engine)()
        sesh.execute("create extension postgis")
        res = sesh.execute("SELECT PostGIS_full_version()")
        print res.fetchall()[0][0]
        Lake.__table__.create(engine)

def test_can_create_test_db():
    engine = create_engine('sqlite://')
    create_test_database(engine)
    create_test_data(engine)
    # Get some data
    sesh = sessionmaker(bind=engine)()
    q = sesh.query(Contact)
    assert len(q.all()) == 2
    
