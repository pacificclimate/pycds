import logging, logging.config

import pytest
from pytest import fixture

from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date, DateTime, Boolean, ForeignKey, MetaData, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, text, literal_column

from pycds.util import generic_sesh
from pycds.materialized_view_helpers import create_materialized_view, refresh_materialized_view, RefreshMaterializedView

# Define some simple objects (and their tables) to test view helpers against

Base = declarative_base()
metadata = Base.metadata

class Thing(Base):
    __tablename__ = 'things'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description_id = Column(Integer, ForeignKey('descriptions.id'))

    def __repr__(self):
        return '<Thing(id=%d, name=%s, desc_id=%d)>' % (self.id, self.name, self.description_id)

class Description(Base):
    __tablename__ = 'descriptions'
    id = Column(Integer, primary_key=True)
    desc = Column(String)

    def __repr__(self):
        return '<Description(id=%d, desc=%s)>' % (self.id, self.desc)

class SimpleThingView(Base):
    _t = Thing.__table__
    __table__ = create_materialized_view(
        'simple_things_mv',
        metadata,
        # select(
        #     [Thing.id.label('id'), Thing.name.label('name')]
        # ).select_from(Thing)
        # .where(Thing.id <= 3)

        text('''
            SELECT *
            FROM things
            WHERE id <= 3
        ''').columns(_t.c.id, _t.c.name, _t.c.description_id)
    )

    def __repr__(self):
        return '<SimpleThingView(id=%d, desc=%s)>' % (self.id, self.name)

class ThingWithDescription(Base):
    _t = Thing.__table__
    _d = Description.__table__
    __table__ = create_materialized_view(
            'things_with_description_mv',
            metadata,
            text('''
                SELECT things.id, things.name, descriptions.desc
                FROM things JOIN descriptions ON (things.description_id = descriptions.id)
            ''').columns(_t.c.id, _t.c.name, _d.c.desc)
    )

    def __repr__(self):
        return '<ThingWithDescription(id=%d, name=%s, desc=%s)>' % (self.id, self.name, self.desc)

@fixture(scope="module")
def mod_empty_test_db_session(mod_blank_postgis_session):
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    sesh = mod_blank_postgis_session
    engine = sesh.get_bind()
    Base.metadata.create_all(bind=engine)
    yield sesh

@fixture
def view_test_session(mod_empty_test_db_session):
    for sesh in generic_sesh(mod_empty_test_db_session, [Thing, Description], [
        [
            Description(id=1, desc='alpha'),
            Description(id=2, desc='beta'),
            Description(id=3, desc='gamma'),
        ],
        [
            Thing(id=1, name='one', description_id=1),
            Thing(id=2, name='two', description_id=2),
            Thing(id=3, name='three', description_id=3),
            Thing(id=4, name='four', description_id=2),
            Thing(id=5, name='five', description_id=1),
        ]

    ]):
        yield sesh

def query_and_print(sesh, title, query):
    print title
    result = sesh.execute(query)
    for row in result:
        print row

def test_simple_view(view_test_session):
    sesh = view_test_session
    refresh_materialized_view(sesh, SimpleThingView)

    things = sesh.query(Thing)
    assert things.count() == 5

    view_things = sesh.query(SimpleThingView)
    assert [t.id for t in view_things.order_by(SimpleThingView.id)] == [1, 2, 3]


def test_complex_view(view_test_session):
    sesh = view_test_session
    refresh_materialized_view(sesh, ThingWithDescription)

    things = sesh.query(ThingWithDescription)
    assert [t.desc for t in things.order_by(ThingWithDescription.id)] \
           == ['alpha', 'beta', 'gamma', 'beta', 'alpha']
