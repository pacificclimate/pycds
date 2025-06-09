import warnings
import pytest

from ..alembicverify_util import prepare_schema_from_migrations


def test_warnings(alembic_runner):
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        alembic_runner.migrate_up_to("head")
        # When warnings are present and being addressed, these print statements
        # are useful.
        # print(f"{len(ws)} warnings:")
        # for w in ws:
        #     print(f"\n{w}")
        assert len(ws) == 0
