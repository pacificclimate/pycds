def test_can_create_postgis_db(blank_postgis_session):
    res = blank_postgis_session.execute("SELECT PostGIS_full_version()")
    assert 'POSTGIS="2.' in res.fetchall()[0][0]

def test_can_create_postgis_geometry_table_model(blank_postgis_session):
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
    res = blank_postgis_session.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = [x[0] for x in res.fetchall()]
    assert 'lake' in tables

def test_can_create_postgis_geometry_table_manual(
        blank_postgis_session, schema_name
):
    blank_postgis_session.execute('''CREATE TABLE lake (
    id SERIAL NOT NULL,
    name VARCHAR,
    geom geometry(POLYGON,-1),
    PRIMARY KEY (id))''')

    res = blank_postgis_session.execute('''
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{}'
    '''.format(schema_name))
    tables = [x[0] for x in res.fetchall()]
    assert 'lake' in tables