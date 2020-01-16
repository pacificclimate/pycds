def test_can_create_postgis_db(empty_sesh):
    res = empty_sesh.execute("SELECT PostGIS_full_version()")
    assert 'POSTGIS="2.' in res.fetchall()[0][0]
