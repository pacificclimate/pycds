import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "78260d36e42b"
