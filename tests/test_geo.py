from sqlalchemy import func

from pycds import History

def test_can_use_spatial_functions_sql(test_session):
    res = test_session.execute("SELECT ST_AsText(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326))").scalar()
    assert res == 'POLYGON((0 0,0 1,1 1,1 0,0 0))'

def test_can_select_spatial_functions_orm(test_session):
    res = test_session.query(func.ST_X(History.the_geom)).filter(History.station_name=='Brandywine').scalar()
    assert res == -123.11806

def test_can_select_spatial_properties(test_session):
    res = test_session.query(History).filter(History.the_geom.ST_Contains('SRID=4326;POINT(-123.11806 50.05417)'))
    assert res.count() == 1
    assert res.first().station_name == 'Brandywine'
