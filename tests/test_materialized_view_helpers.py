import logging, logging.config

import pytest
from pytest import fixture

from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date, DateTime, Boolean, ForeignKey, MetaData, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, text, literal_column
from sqlalchemy.sql import column

from pycds.util import generic_sesh
from pycds.materialized_view_helpers import MaterializedViewMixin

# Define some simple objects (and their tables) to test view helpers against

Base = declarative_base()
metadata = Base.metadata


class Thing(Base):
    __tablename__ = 'things'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description_id = Column(Integer, ForeignKey('descriptions.id'))

    def __repr__(self):
        return '<Thing(id={}, name={}, desc_id={})>'.format(self.id, self.name, self.description_id)


class Description(Base):
    __tablename__ = 'descriptions'
    id = Column(Integer, primary_key=True)
    desc = Column(String)

    def __repr__(self):
        return '<Description(id={}, desc={})>'.format(self.id, self.desc)


class SimpleThing(Base, MaterializedViewMixin):
    # __selectable__ = select([Thing]).where(Thing.id <= literal_column('3'))  # deeelightful

    # less delightful but applicable to cases where we use text queries for selectable:
    __selectable__ = text("""
        SELECT *
        FROM things
        WHERE things.id <= 3
    """).columns(Thing.id, Thing.name, Thing.description_id)

    def __repr__(self):
        return '<SimpleThing(id={}, desc={})>'.format(self.id, self.name)


class ThingWithDescription(Base, MaterializedViewMixin):
    __selectable__ = text('''
        SELECT things.id, things.name, descriptions.desc
        FROM things JOIN descriptions ON (things.description_id = descriptions.id)
    ''').columns(column('id'), column('name'), column('desc'))
    __primary_key__ = ['id']


class ThingCount(Base, MaterializedViewMixin):
    __selectable__ = text("""
        SELECT d.desc as desc, count(things.id) as num
        FROM things JOIN descriptions as d ON (things.description_id = d.id)
        GROUP BY d.desc
    """).columns(column('desc'), column('num'))
    __primary_key__ = ['desc']

    def __repr__(self):
        return '<ThingWithDescription(id={}, name={}, desc={})>'.format(self.id, self.name, self.desc)


@fixture(scope="module")
def mod_empty_test_db_session(mod_blank_postgis_session):
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    sesh = mod_blank_postgis_session
    engine = sesh.get_bind()
    Base.metadata.create_all(bind=engine)
    views = [SimpleThing, ThingWithDescription, ThingCount]
    for view in views:
        view.create(sesh)
    yield sesh
    for view in reversed(views):
        view.drop(sesh)

@fixture
def view_test_session(mod_empty_test_db_session):
    for sesh in generic_sesh(mod_empty_test_db_session, [
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

def test_viewname():
    assert SimpleThing.viewname() == 'simple_thing_mv'

def test_simple_view(view_test_session):
    sesh = view_test_session

    things = sesh.query(Thing)
    assert things.count() == 5

    SimpleThing.refresh(sesh)
    view_things = sesh.query(SimpleThing)
    assert [t.id for t in view_things.order_by(SimpleThing.id)] == [1, 2, 3]


def test_complex_view(view_test_session):
    sesh = view_test_session
    ThingWithDescription.refresh(sesh)
    things = sesh.query(ThingWithDescription)
    assert [t.desc for t in things.order_by(ThingWithDescription.id)] \
           == ['alpha', 'beta', 'gamma', 'beta', 'alpha']

def test_counts(view_test_session):
    sesh = view_test_session
    ThingCount.refresh(sesh)
    counts = sesh.query(ThingCount)
    assert [(c.desc, c.num) for c in counts.order_by(ThingCount.desc)] == \
           [('alpha', 2), ('beta', 2), ('gamma', 1), ]
