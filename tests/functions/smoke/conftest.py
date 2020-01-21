from pytest import fixture
from sqlalchemy import func
import pycds.functions


@fixture
def schema_func(schema_name):
    return getattr(func, schema_name)


@fixture
def functions_sesh(pycds_sesh):
    for name in pycds.functions.__all__:
        pycds_sesh.execute(getattr(pycds.functions, name)())
    yield pycds_sesh