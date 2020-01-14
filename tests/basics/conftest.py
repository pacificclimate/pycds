from pytest import fixture
from sqlalchemy.orm import sessionmaker


@fixture
def tfs_empty_sesh(tss_base_engine, set_search_path):
    """Test-function scoped (tfs) database session, with no content whatsoever.
    All session actions are rolled back on teardown.
    """
    sesh = sessionmaker(bind=tss_base_engine)()
    set_search_path(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()
