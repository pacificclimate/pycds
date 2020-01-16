"""Tests of the `good_obs` query"""

from pycds import Obs
from pycds.weather_anomaly import good_obs


def test_good_obs(obs1_temp_sesh):
    """At present, there are only good observations in this session,
    so the test is simple."""
    sesh = obs1_temp_sesh
    observations = sesh.query(Obs)
    goods = sesh.query(good_obs)
    assert goods.count() == observations.count()


