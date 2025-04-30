from sqlalchemy import text

def test_can_create_postgis_db(empty_sesh):
    res = empty_sesh.execute(text("SELECT PostGIS_full_version()"))
    version = res.fetchall()[0][0]
    assert 'POSTGIS="3.' in version
