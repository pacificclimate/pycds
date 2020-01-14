from pytest import fixture
from sqlalchemy.orm import sessionmaker
from pycds.util import generic_sesh
from .content import \
    ContentBase, Thing, Description, SimpleThingView, ThingWithDescriptionView, ThingCountView


@fixture  # TODO: scope?
def tst_orm_engine(tss_base_engine):
    """Database engine with test content created in it."""
    ContentBase.metadata.create_all(bind=tss_base_engine)
    yield tss_base_engine


@fixture  # TODO: scope?
def tst_orm_sesh(tst_orm_engine, set_search_path):
    sesh = sessionmaker(bind=tst_orm_engine)()
    set_search_path(sesh)
    yield sesh
    sesh.rollback()
    sesh.close()


@fixture  # TODO: scope?
def tst_orm_content_sesh(tst_orm_sesh):
    for sesh in generic_sesh(tst_orm_sesh, [
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


@fixture  # TODO: scope?
def view_sesh(tst_orm_content_sesh):
    sesh = tst_orm_content_sesh
    views = [SimpleThingView, ThingWithDescriptionView, ThingCountView]
    for view in views:
        view.create(sesh)
    yield sesh
    for view in reversed(views):
        view.drop(sesh)



