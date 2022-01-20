import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "4a2f1879293a"
