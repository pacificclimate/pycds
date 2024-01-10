import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "5c841d2c01d1"
