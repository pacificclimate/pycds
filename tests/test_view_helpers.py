import pytest

from sqlalchemy import Table, Column, Integer, BigInteger, Float, String, Date, DateTime, Boolean, ForeignKey, MetaData, Numeric, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, text, literal_column

from pycds.view_helpers import create_view

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
    __table__ = create_view(
            'simple_things_v',
            metadata,
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
    __table__ = create_view(
            'things_with_description_v',
            metadata,
            text('''
                SELECT things.id, things.name, descriptions.desc
                FROM things JOIN descriptions ON (things.description_id = descriptions.id)
            ''').columns(_t.c.id, _t.c.name, _d.c.desc)
    )

    def __repr__(self):
        return '<ThingWithDescription(id=%d, name=%s, desc=%s)>' % (self.id, self.name, self.desc)

def create_db(sesh):
    engine = sesh.get_bind()
    Base.metadata.create_all(bind=engine)

def add_Things(sesh):
    sesh.add_all([
        Thing(id=1, name='one', description_id=1),
        Thing(id=2, name='two', description_id=2),
        Thing(id=3, name='three', description_id=3),
        Thing(id=4, name='four', description_id=2),
        Thing(id=5, name='five', description_id=1),
    ])

def add_Descriptions(sesh):
    sesh.add_all([
        Description(id=1, desc='alpha'),
        Description(id=2, desc='beta'),
        Description(id=3, desc='gamma'),
    ])

@pytest.fixture(scope='function')
def view_test_session(blank_postgis_session):
    sesh = blank_postgis_session
    create_db(sesh)
    add_Descriptions(sesh)
    # flush() here is required to force the creation of the descriptions table before the things table.
    # If not, a race condition occurs and if the things table is created first, IntegrityError
    # is raised. Output of SQLAlchemy says: "raised as a result of Query-invoked autoflush; consider using a
    # session.no_autoflush block if this flush is occurring prematurely", so there may be a better way to
    # handle this problem.
    sesh.flush()
    add_Things(sesh)
    return sesh

def test_simple_view(view_test_session):
    sesh = view_test_session

    things = sesh.query(Thing)
    assert(things.count() == 5)

    view_things = sesh.query(SimpleThingView)
    assert(view_things.count() == 3)


def test_complex_view(view_test_session):
    sesh = view_test_session

    things = sesh.query(ThingWithDescription)
    assert(things.count() == 5)
    assert(things.filter_by(id=1).first().desc == 'alpha')
