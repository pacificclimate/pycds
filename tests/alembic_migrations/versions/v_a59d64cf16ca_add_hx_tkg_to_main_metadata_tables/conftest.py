import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "a59d64cf16ca"
