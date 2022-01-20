import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "8fd8f556c548"
