import warnings
import pytest

from ..alembicverify_util import prepare_schema_from_migrations


@pytest.mark.usefixtures("new_db_left")
def test_warnings(uri_left, alembic_config_left, db_setup):
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        engine, script = prepare_schema_from_migrations(
            uri_left, alembic_config_left, db_setup=db_setup
        )
        # When warnings are present and being addressed, these print statements
        # are useful.
        # print(f"{len(ws)} warnings:")
        # for w in ws:
        #     print(f"\n{w}")
        assert len(ws) == 0
