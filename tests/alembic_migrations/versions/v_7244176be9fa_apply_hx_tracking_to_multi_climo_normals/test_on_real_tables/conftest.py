
import pytest


@pytest.fixture(scope="module")
def target_revision():
    # Migrate initially to here
    return "758be4f4ce0f"
