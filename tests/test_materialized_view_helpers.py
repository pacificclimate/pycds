import logging, logging.config

import pytest
from pytest import fixture
import testing.postgresql

from sqlalchemy import create_engine, inspect
from sqlalchemy import func
from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date, DateTime, Boolean, ForeignKey, MetaData, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, text, literal_column
from sqlalchemy.sql import column
from sqlalchemy.schema import DDL, CreateSchema
from sqlalchemy.orm import sessionmaker, Query

from pycds.util import generic_sesh
from pycds.materialized_view_helpers import MaterializedViewMixin


logging.basicConfig()
sa_logger = logging.getLogger('sqlalchemy.engine')
sa_logger.setLevel(logging.INFO)

logger = logging.getLogger('test_matviews')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


logger.info('##############')


def get_schema_names(executor):
    res = executor.execute("""
        SELECT distinct(table_schema) as name
        FROM information_schema.tables
        ORDER BY name
    """)
    return tuple(x[0] for x in res.fetchall())


def get_user_schema_names(executor):
    reserved = ('information_schema', 'pg_catalog')
    return tuple(n for n in get_schema_names(executor) if n not in reserved)


def get_table_names(schema_name, executor):
    res = executor.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}'
        ORDER BY table_name
    """)
    return tuple(x[0] for x in res.fetchall())


def print_schema_info(executor, message=None):
    if message:
        logger.info(f'### {message}')
    names = get_user_schema_names(executor)
    logger.info(f'### schemas: {names}')
    for name in names:
        logger.info(f'### schema {name}: {get_table_names(name, executor)}')


# Define some simple objects (and their tables) to test view helpers against

schema = 'schema'

# no schema specified: tests pass
# schema specified: tests fail
# metadata = MetaData()
ContentBase = declarative_base(metadata=(MetaData(schema=schema)))
ViewBase = declarative_base(metadata=(MetaData(schema=schema)))


class Thing(ContentBase):
    __tablename__ = 'things'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description_id = Column(Integer, ForeignKey('descriptions.id'))

    def __repr__(self):
        return '<Thing(id={}, name={}, desc_id={})>'.format(self.id, self.name, self.description_id)


class Description(ContentBase):
    __tablename__ = 'descriptions'
    id = Column(Integer, primary_key=True)
    desc = Column(String)

    def __repr__(self):
        return '<Description(id={}, desc={})>'.format(self.id, self.desc)


class ThingSubsetSA(ViewBase, MaterializedViewMixin):
    __selectable__ = (
        Query([
            Thing.id.label('id'),
            Thing.name.label('name'),
            Thing.description_id.label('description_id'),
        ]).filter(Thing.id <= literal_column('3'))
    ).selectable

    def __repr__(self):
        return '<ThingSubsetSA(id={}, desc={})>'.format(self.id, self.name)


class ThingSubsetText(ViewBase, MaterializedViewMixin):
    # less delightful but applicable to cases where we use text queries for
    # __selectable__
    # Note interpolation of schema name into table names
    __selectable__ = text(f"""
        SELECT *
        FROM {schema}.things
        WHERE id <= 3
    """).columns(Thing.id, Thing.name, Thing.description_id)

    def __repr__(self):
        return '<ThingSubsetText(id={}, desc={})>'.format(self.id, self.name)


class ThingWithDescription(ViewBase, MaterializedViewMixin):
    __selectable__ = (
        Query([
            Thing.id.label('id'),
            Thing.name.label('name'),
            Description.desc.label('desc'),
        ])
            .select_from(Thing)
            .join(Description, Description.id == Thing.description_id)
    ).selectable
    # __selectable__ = text('''
    #     SELECT things.id, things.name, descriptions.desc
    #     FROM things JOIN descriptions ON (things.description_id = descriptions.id)
    # ''').columns(column('id'), column('name'), column('desc'))
    # __primary_key__ = ['id']


class ThingCountByDescription(ViewBase, MaterializedViewMixin):
    __selectable__ = (
        Query([
            Description.desc.label('desc'),
            func.count(Thing.id).label('num')
        ]).select_from(Thing).join(Description)
            .group_by(Description.desc)
    ).selectable
    # __selectable__ = text("""
    #     SELECT d.desc as desc, count(things.id) as num
    #     FROM things JOIN descriptions as d ON (things.description_id = d.id)
    #     GROUP BY d.desc
    # """).columns(column('desc'), column('num'))
    __primary_key__ = ['desc']

    def __repr__(self):
        return '<ThingWithDescription(id={}, name={}, desc={})>'.format(self.id, self.name, self.desc)


################################################################################
# Fixtures

@fixture(scope='module')
def mod_blank_postgis_session1():
    logger.info('mod_blank_postgis_session1: setup')
    with testing.postgresql.Postgresql() as pg:
        logger.info('mod_blank_postgis_session1: 1')
        engine = create_engine(pg.url())
        logger.info('mod_blank_postgis_session1: 2')
        engine.execute("create extension postgis")
        logger.info('mod_blank_postgis_session1: 3')
        engine.execute(CreateSchema(schema))
        logger.info('mod_blank_postgis_session1: 4')
        sesh = sessionmaker(bind=engine)()
        logger.info('mod_blank_postgis_session1: 5')
        yield sesh
    logger.info('mod_blank_postgis_session1: teardown')


@fixture(scope="module")
def mod_empty_content_session(mod_blank_postgis_session1):
    logger.info('mod_empty_content_session: setup')
    sesh = mod_blank_postgis_session1
    engine = sesh.get_bind()
    ContentBase.metadata.create_all(bind=engine)
    yield sesh
    logger.info('mod_empty_content_session: teardown')


@fixture(scope="module")
def mod_empty_view_session(mod_empty_content_session):
    logger.info('mod_empty_view_session: setup')
    sesh = mod_empty_content_session
    print_schema_info(sesh, '### before view creation')
    views = (
        ThingSubsetSA,
        ThingWithDescription,
        ThingCountByDescription,
    )
    for view in views:
        view.create(sesh)
    print_schema_info(sesh, '### after view creation')
    yield sesh
    for view in reversed(views):
        view.drop(sesh)
    print_schema_info(sesh, '### after view drop')
    logger.info('mod_empty_view_session: teardown')


@fixture
def view_test_session(mod_empty_view_session):
    logger.info('view_test_session: setup')
    for sesh in generic_sesh(mod_empty_view_session, [
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
    logger.info('view_test_session: teardown')


#############################################################################
# Tests

@pytest.mark.parametrize('View, basename', (
        (ThingSubsetSA, 'thing_subset_sa_mv'),
        (ThingSubsetText, 'thing_subset_text_mv'),
        (ThingWithDescription, 'thing_with_description_mv'),
        (ThingCountByDescription, 'thing_count_by_description_mv'),
))
def test_viewnames(View, basename):
    assert View.base_viewname() == basename
    assert View.qualfied_viewname() == f'{schema}.{basename}'


@pytest.mark.parametrize('View', (
        ThingSubsetSA,
        ThingSubsetText,
        ThingWithDescription,
        ThingCountByDescription,
))
def test_view_lifecycle_manual(View, mod_blank_postgis_session1):
    sesh = mod_blank_postgis_session1
    assert View.base_viewname() not in get_table_names(schema, sesh)
    ContentBase.metadata.create_all(bind=sesh.get_bind())
    assert View.base_viewname() not in get_table_names(schema, sesh)
    View.create(sesh)
    assert View.base_viewname() in get_table_names(schema, sesh)
    View.refresh(sesh)
    assert View.base_viewname() in get_table_names(schema, sesh)
    View.drop(sesh)
    assert View.base_viewname() not in get_table_names(schema, sesh)


# For reasons not yet understood, ViewBase.metadata.drop_all() "stalls" after
# dropping the last table. The function never returns, and the test never
# completes. To be solved in some future episode of head-banging.
# @pytest.mark.parametrize('View', (
#     ThingSubsetSA,
#     ThingSubsetText,
#     ThingWithDescription,
#     ThingCountByDescription,
# ))
# def test_view_lifecycle_automatic(View, mod_blank_postgis_session1):
#     sesh = mod_blank_postgis_session1
#     assert View.base_viewname() not in get_table_names(schema, sesh)
#     ContentBase.metadata.create_all(bind=sesh.get_bind())
#     assert View.base_viewname() not in get_table_names(schema, sesh)
#     ViewBase.metadata.create_all(bind=sesh.get_bind())
#     assert View.base_viewname() in get_table_names(schema, sesh)
#     View.refresh(sesh)
#     assert View.base_viewname() in get_table_names(schema, sesh)
#     logger.info('### before ViewBase drop_all')
#     ViewBase.metadata.drop_all(bind=sesh.get_bind())
#     logger.info('### after ViewBase drop_all')
#     assert View.base_viewname() not in get_table_names(schema, sesh)
#     ContentBase.metadata.drop_all(bind=sesh.get_bind())


def test_thing_subset(view_test_session):
    sesh = view_test_session

    things = sesh.query(Thing)
    assert things.count() == 5

    subset_things = sesh.query(ThingSubsetSA)
    assert subset_things.count() == 0  # there is nothing in the view before refreshing it
    ThingSubsetSA.refresh(sesh)
    assert subset_things.count() == 3  # there is something after refreshing
    assert [t.id for t in subset_things.order_by(ThingSubsetSA.id)] == [1, 2, 3]


def test_thing_with_description(view_test_session):
    sesh = view_test_session
    ThingWithDescription.refresh(sesh)
    things = sesh.query(ThingWithDescription)
    assert [t.desc for t in things.order_by(ThingWithDescription.id)] \
           == ['alpha', 'beta', 'gamma', 'beta', 'alpha']

def test_thing_count_by_description(view_test_session):
    sesh = view_test_session
    ThingCountByDescription.refresh(sesh)
    counts = sesh.query(ThingCountByDescription)
    assert [(c.desc, c.num) for c in counts.order_by(ThingCountByDescription.desc)] == \
           [('alpha', 2), ('beta', 2), ('gamma', 1), ]
