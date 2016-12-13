import re
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from sqlalchemy.ext.declarative import declared_attr

# View creation tools
#
# SQLAlchemy does not have out-of-the-box support for views or materialized views.
# This module adds view functionality.

# For details on view creation in SQLAlchemy see:
#   http://stackoverflow.com/questions/9766940/how-to-create-an-sql-view-with-sqlalchemy
#   https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/Views
#   https://gist.github.com/techniq/5174412

class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

class DropView(DDLElement):
    def __init__(self, name):
        self.name = name

@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (element.name, compiler.sql_compiler.process(element.selectable))

@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)

def snake_case(ident):
    """Return a snake-case version of a camel-case identifier, e.g., MyBigDeal -> my_big_deal.
    Courtesy of http://stackoverflow.com/a/12867228"""
    a = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    return a.sub(r'_\1', ident).lower()


class ViewMixin(object):
    """Mixin for ORM classes that are views. Defines the two key attributes of an ORM class,
    __table__ and __mapper_args__, based on the values of class attributes __selectable__ and __primary_key__.

    Usage:

    class Thing(Base, ViewMixin):
        __selectable__ = <SQLAlchemy selectable>
        __primary_key__ = ['primary', 'key', 'columns']

    __primary_key__ attribute is optional and may be omitted if __selectable__ already defines primary keys.
    It must be defined otherwise (e.g., text selectables with anonymous columns; see tests).

    """

    @declared_attr
    def __table__(cls):
        t = table(cls.viewname())
        for c in cls.__selectable__.c:
            c._make_proxy(t)
        return t

    @declared_attr
    def __mapper_args__(cls):
        # the return value should instead be merged into other __mapper_args__ declared for the class ...
        try:
            return {'primary_key': [cls.__table__.c[col] for col in cls.__primary_key__]}
        except AttributeError:
            return {}

    @classmethod
    def viewname(cls):
        return snake_case(cls.__name__) + '_v'

    @classmethod
    def create(cls, sesh):
        sesh.execute(CreateView(cls.viewname(), cls.__selectable__))

    @classmethod
    def drop(cls, sesh):
        sesh.execute(DropView(cls.viewname()))
