from pytest import fixture
from sqlalchemy.orm import sessionmaker
from ..helpers import add_then_delete_objs
from ..helpers import create_then_drop_views
from .content import (
    ContentBase,
    SimpleThingView,
    ThingWithDescriptionView,
    ThingCountView,
    SimpleThingManualMatview,
    ThingWithDescriptionManualMatview,
    ThingCountManualMatview,
    content
)


@fixture(scope='session')
def tst_orm_engine(base_engine):
    """Database engine with test content created in it."""
    ContentBase.metadata.create_all(bind=base_engine)
    yield base_engine


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
        for s1 in add_then_delete_objs(s0, content):
            yield s1


@fixture
def manual_matview_sesh(tst_orm_sesh):
    views = [
        SimpleThingManualMatview,
        ThingWithDescriptionManualMatview,
        ThingCountManualMatview
    ]
    # Matviews must be created before content is added in order to test
    # refreshed functionality. The nested loops do this.
    for s0 in create_then_drop_views(tst_orm_sesh, views):
        for s1 in add_then_delete_objs(s0, content):
            yield s1
