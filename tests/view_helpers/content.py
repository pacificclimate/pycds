"""
Define some simple ORM objects (and their tables) to test view helpers against.
To separate view creation from creation of non-view (content) tables, they
are given different ORM declarative bases.
"""

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, text, literal_column
from sqlalchemy.sql import column

from pycds.view_helpers import ViewMixin


ContentBase = declarative_base()
ViewBase = declarative_base()


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


class SimpleThing(ViewBase, ViewMixin):
    # __selectable__ = select([Thing]).where(Thing.id <= literal_column('3'))  # deeelightful

    # less delightful but applicable to cases where we use text queries for selectable:
    __selectable__ = text("""
        SELECT *
        FROM things
        WHERE things.id <= 3
    """).columns(Thing.id, Thing.name, Thing.description_id)

    def __repr__(self):
        return '<SimpleThing(id={}, desc={})>'.format(self.id, self.name)


class ThingWithDescription(ViewBase, ViewMixin):
    __selectable__ = text('''
        SELECT things.id, things.name, descriptions.desc
        FROM things JOIN descriptions ON (things.description_id = descriptions.id)
    ''').columns(column('id'), column('name'), column('desc'))
    __primary_key__ = ['id']


class ThingCount(ViewBase, ViewMixin):
    __selectable__ = text("""
        SELECT d.desc as desc, count(things.id) as num
        FROM things JOIN descriptions as d ON (things.description_id = d.id)
        GROUP BY d.desc
    """).columns(column('desc'), column('num'))
    __primary_key__ = ['desc']

    def __repr__(self):
        return '<ThingWithDescription(id={}, name={}, desc={})>'.format(self.id, self.name, self.desc)



