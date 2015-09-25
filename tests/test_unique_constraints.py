from datetime import datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pycds.util import create_test_database, create_test_data
from pycds import Obs, History, Variable, Network, NativeFlag

import pytest

def test_obs_raw_unique(test_session):
    # Find any variable and history to link to
    hist = test_session.query(History).first()
    var = test_session.query(Variable).first()
    time = datetime(1980, 1, 1)
    # Add duplicate observations
    for _ in range(2):
        o = Obs(time=time, datum=0, history=hist, variable=var)
        test_session.add(o)

    with pytest.raises(IntegrityError):
        test_session.commit()

def test_native_flag_unique(test_session):
    # Pick a network, any network
    net = test_session.query(Network).first()
    for _ in range(2):
        flag = NativeFlag(network=net, value='foo')
        test_session.add(flag)

    with pytest.raises(IntegrityError):
        test_session.commit()
