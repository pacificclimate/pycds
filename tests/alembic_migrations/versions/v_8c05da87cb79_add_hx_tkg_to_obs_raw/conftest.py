import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "8c05da87cb79"
