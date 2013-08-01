from pkg_resources import resource_filename

import pytest
import pycds

@pytest.fixture(scope="module")
def test_session():
    return pycds.test_session()

@pytest.fixture(scope="module")
def conn_params():
    return 'sqlite:///{0}'.format(resource_filename('pycds', 'data/crmp.sqlite'))
