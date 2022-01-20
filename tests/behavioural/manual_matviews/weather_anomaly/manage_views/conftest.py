import pytest


# All tests in this directory need fixture `new_db_left`
@pytest.fixture(autouse=True)
def autouse_new_db_left(new_db_left):
    pass
