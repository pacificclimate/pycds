from pytest import fixture
from sqlalchemy.orm import sessionmaker
from ..helpers import generic_sesh
from ..helpers import create_then_drop_views
from .content import \
    ContentBase, \
    SimpleThingView, ThingWithDescriptionView, ThingCountView, \
    SimpleThingMatview, ThingWithDescriptionMatview, ThingCountMatview, \
    content


@fixture(scope='session')
def tst_orm_engine(tss_base_engine):
    """Database engine with test content created in it."""
    ContentBase.metadata.create_all(bind=tss_base_engine)
    yield tss_base_engine


@fixture
def tst_orm_sesh(tst_orm_engine, set_search_path):
    sesh = sessionmaker(bind=tst_orm_engine)()
    set_search_path(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()


@fixture
def view_sesh(tst_orm_sesh):
    views = [SimpleThingView, ThingWithDescriptionView, ThingCountView]
    for s0 in create_then_drop_views(tst_orm_sesh, views):
        for s1 in generic_sesh(s0, content):
            yield s1


@fixture
def matview_sesh(tst_orm_sesh):
    views = [SimpleThingMatview, ThingWithDescriptionMatview, ThingCountMatview]
    # Matviews must be created before content is added in order to test
    # refreshed functionality. The nested loops do this.
    for s0 in create_then_drop_views(tst_orm_sesh, views):
        for s1 in generic_sesh(s0, content):
            yield s1
