"""
Define some simple ORM objects (and their tables) to test view and matview
helpers against.
To separate view creation from creation of non-view (content) tables, they
are given different ORM declarative bases.
"""

from sqlalchemy import MetaData, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, text, literal_column
from sqlalchemy.sql import column

from pycds import get_schema_name
from pycds.view_helpers import ViewMixin
from pycds.materialized_view_helpers import ManualMaterializedViewMixin

schema_name = get_schema_name()
ContentBase = declarative_base(metadata=MetaData(schema=schema_name))
ViewBase = declarative_base(metadata=MetaData(schema=schema_name))


# Tables

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


# Table objects

content = [
    Description(id=1, desc='alpha'),
    Description(id=2, desc='beta'),
    Description(id=3, desc='gamma'),
    Thing(id=1, name='one', description_id=1),
    Thing(id=2, name='two', description_id=2),
    Thing(id=3, name='three', description_id=3),
    Thing(id=4, name='four', description_id=2),
    Thing(id=5, name='five', description_id=1),
]


# Selectables for defining views and matviews

simple_thing_sqla_selectable = \
    select([Thing]).where(Thing.id <= literal_column('3'))

# less delightful but applicable to cases where we use text queries for
# selectable:
simple_thing_text_selectable = text(f'''
    SELECT *
    FROM {schema_name}.things
    WHERE things.id <= 3
''').columns(Thing.id, Thing.name, Thing.description_id)


thing_with_description_text_selectable = text(f'''
    SELECT t.id, t.name, d.desc
    FROM {schema_name}.things AS t
        JOIN {schema_name}.descriptions AS d ON (t.description_id = d.id)
''').columns(column('id'), column('name'), column('desc'))

thing_count_text_selectable = text(f'''
    SELECT d.desc as desc, count(t.id) as num
    FROM {schema_name}.things AS t 
        JOIN {schema_name}.descriptions AS d ON (t.description_id = d.id)
    GROUP BY d.desc
''').columns(column('desc'), column('num'))


# Views

class SimpleThingView(ViewBase, ViewMixin):
    __selectable__ = simple_thing_text_selectable

    def __repr__(self):
        return '<SimpleThingView(id={}, desc={})>'.format(self.id, self.name)


class ThingWithDescriptionView(ViewBase, ViewMixin):
    __selectable__ = thing_with_description_text_selectable
    __primary_key__ = ['id']


class ThingCountView(ViewBase, ViewMixin):
    __selectable__ = thing_count_text_selectable
    __primary_key__ = ['desc']

    def __repr__(self):
        return '<ThingWithDescriptionView(id={}, name={}, desc={})>'.format(self.id, self.name, self.desc)


# Materialized views

class SimpleThingMatview(ViewBase, ManualMaterializedViewMixin):
    __selectable__ = simple_thing_text_selectable

    def __repr__(self):
        return '<SimpleThingMatview(id={}, desc={})>'.format(self.id, self.name)


class ThingWithDescriptionMatview(ViewBase, ManualMaterializedViewMixin):
    __selectable__ = thing_with_description_text_selectable
    __primary_key__ = ['id']


class ThingCountMatview(ViewBase, ManualMaterializedViewMixin):
    __selectable__ = thing_count_text_selectable
    __primary_key__ = ['desc']

    def __repr__(self):
        return '<ThingWithDescriptionMatview(id={}, name={}, desc={})>'.format(self.id, self.name, self.desc)





