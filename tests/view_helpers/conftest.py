from pytest import fixture
from sqlalchemy.orm import sessionmaker
from pycds.util import generic_sesh
from .content import \
    ContentBase, Thing, Description, SimpleThing, ThingWithDescription, ThingCount


@fixture  # TODO: scope?
def content_engine(tss_base_engine):
    """Database engine with test content created in it."""
    ContentBase.metadata.create_all(bind=tss_base_engine)
    yield tss_base_engine


@fixture  # TODO: scope?
def content_session(content_engine, set_search_path):
    sesh = sessionmaker(bind=content_engine)()
    set_search_path(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()


@fixture  # TODO: scope?
def view_session(content_session):
    sesh = content_session
    views = [SimpleThing, ThingWithDescription, ThingCount]
    for view in views:
        view.create(sesh)
    yield sesh
    for view in reversed(views):
        view.drop(sesh)


@fixture
def view_test_session(view_session):
    for sesh in generic_sesh(view_session, [
        Description(id=1, desc='alpha'),
        Description(id=2, desc='beta'),
        Description(id=3, desc='gamma'),
        Thing(id=1, name='one', description_id=1),
        Thing(id=2, name='two', description_id=2),
        Thing(id=3, name='three', description_id=3),
        Thing(id=4, name='four', description_id=2),
        Thing(id=5, name='five', description_id=1),
    ]):
        yield sesh

