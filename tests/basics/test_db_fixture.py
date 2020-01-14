# TODO: This entire test module seems redundant -- remove if so

def test_can_create_postgis_db(tfs_empty_sesh):
    res = tfs_empty_sesh.execute("SELECT PostGIS_full_version()")
    assert 'POSTGIS="2.' in res.fetchall()[0][0]


# TODO: Redundant? Check other tests
def test_can_create_postgis_geometry_table_model(tfs_empty_sesh):
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

    Lake.__table__.create(tfs_empty_sesh.get_bind())
    res = tfs_empty_sesh.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = [x[0] for x in res.fetchall()]
    assert 'lake' in tables


# TODO: Redundant? Check other tests
def test_can_create_postgis_geometry_table_manual(tfs_empty_sesh):
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String
    from sqlalchemy.orm import sessionmaker
    from geoalchemy2 import Geometry

    tfs_empty_sesh.execute('''CREATE TABLE lake (
    id SERIAL NOT NULL,
    name VARCHAR,
    geom geometry(POLYGON,-1),
    PRIMARY KEY (id))''')

    res = tfs_empty_sesh.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = [x[0] for x in res.fetchall()]
    assert 'lake' in tables