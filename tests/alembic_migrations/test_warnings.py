import warnings
import pytest

from sqlalchemy.exc import SAWarning
from .alembicverify_util import prepare_schema_from_migrations


@pytest.mark.usefixtures("new_db_left")
def test_warnings(
    uri_left, alembic_config_left, db_setup, env_config
):
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        engine, script = prepare_schema_from_migrations(
            uri_left, alembic_config_left, db_setup=db_setup
        )
        print(f"{len(ws)} warnings:")
        for w in ws:
            print(f"\n{w}")
        assert len(ws) == 6
        assert all(w.category == SAWarning for w in ws)

