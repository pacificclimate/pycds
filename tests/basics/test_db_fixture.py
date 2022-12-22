def test_can_create_postgis_db(empty_sesh):
    res = empty_sesh.execute("SELECT PostGIS_full_version()")
    version = res.fetchall()[0][0]
    assert 'POSTGIS="2.' in version or 'POSTGIS="3.' in version
