def test_can_create_postgis_db(empty_sesh):
    res = empty_sesh.execute("SELECT PostGIS_full_version()")
    assert res.fetchall()[0][0].startswith("POSTGIS=")
