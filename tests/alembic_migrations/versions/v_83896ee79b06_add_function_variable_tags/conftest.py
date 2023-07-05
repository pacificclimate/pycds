import pytest


@pytest.fixture(scope="module")
def target_revision():
    return "83896ee79b06"
