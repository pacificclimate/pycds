import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "0d99ba90c229"
