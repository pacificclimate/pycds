import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "e688e520d265"
