from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest

import pycds

@pytest.fixture(scope="module")
def test_session():
    dsn = 'sqlite+pysqlite:///{0}'.format(resource_filename('pycds', 'data/crmp.sqlite'))
    engine = create_engine(dsn)
    engine.echo = True
    Session = sessionmaker(bind=engine)
    return Session()

@pytest.fixture(scope="module")
def conn_params():
    return 'sqlite+pysqlite:///{0}'.format(resource_filename('pycds', 'data/crmp.sqlite'))
