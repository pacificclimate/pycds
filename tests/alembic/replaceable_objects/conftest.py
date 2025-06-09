from pytest import fixture
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from tests.helpers import add_then_delete_objs
from tests.helpers import create_then_drop_views
from .content import (
    ContentBase,
    content,
    SimpleThingView,
    ThingWithDescriptionView,
    ThingCountView,
    SimpleThingNativeMatview,
    ThingWithDescriptionNativeMatview,
    ThingCountNativeMatview,
    SimpleThingManualMatview,
    ThingWithDescriptionManualMatview,
    ThingCountManualMatview,
)

@fixture
def tst_orm_engine(base_engine):
    """Database engine with test content created in it."""
    ContentBase.metadata.create_all(bind=base_engine)
    yield base_engine


@fixture
def tst_orm_sesh(tst_orm_engine):
    with Session(tst_orm_engine) as session:
        # Set the search path to public schema for the session
        # This is necessary to ensure that views and matviews are created in the correct schema
        session.execute(text(f"SET search_path TO public"))
        yield session
        session.rollback()
        session.close()


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
        ThingCountManualMatview,
    ]
    # Matviews must be created before content is added in order to test
    # refreshed functionality. The nested loops do this.
    for s0 in create_then_drop_views(tst_orm_sesh, views):
        for s1 in add_then_delete_objs(s0, content):
            yield s1


@fixture
def native_matview_sesh(tst_orm_sesh):
    views = [
        SimpleThingNativeMatview,
        ThingWithDescriptionNativeMatview,
        ThingCountNativeMatview,
    ]
    # Matviews must be created before content is added in order to test
    # refreshed functionality. The nested loops do this.
    for s0 in create_then_drop_views(tst_orm_sesh, views):
        for s1 in add_then_delete_objs(s0, content):
            yield s1
