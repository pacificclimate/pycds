from datetime import datetime

from sqlalchemy.exc import IntegrityError
from pycds import Obs, History, Variable, Network, NativeFlag

import pytest


def test_obs_raw_unique(tfs_pycds_sesh_with_small_data):
    sesh = tfs_pycds_sesh_with_small_data
    # Find any variable and history to link to
    hist = sesh.query(History).first()
    var = sesh.query(Variable).first()
    time = datetime(1980, 1, 1)
    # Add duplicate observations
    for _ in range(2):
        o = Obs(time=time, datum=0, history=hist, variable=var)
        sesh.add(o)

    with pytest.raises(IntegrityError):
        sesh.commit()


def test_native_flag_unique(tfs_pycds_sesh_with_small_data):
    sesh = tfs_pycds_sesh_with_small_data
    # Pick a network, any network
    net = sesh.query(Network).first()
    for _ in range(2):
        flag = NativeFlag(network=net, value='foo')
        sesh.add(flag)

    with pytest.raises(IntegrityError):
        sesh.flush()
