from pycds.functions import closest_stns_within_threshold


def test_smoke(pycds_sesh, schema_func):
    pycds_sesh.execute(closest_stns_within_threshold())
    q = pycds_sesh.query(schema_func.closest_stns_within_threshold(-120.0, 50.0, 1))
    q.all()
