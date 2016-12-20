from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy import event
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.ext.declarative import declared_attr
from pycds.view_helpers import snake_case

# Materialized view creation tools
#
# SQLAlchemy does not have out-of-the-box support for views or materialized views.
# This module adds materialized view functionality.

# For info on materialized view creation in SQLAlchemy see:
#   http://www.jeffwidman.com/blog/847/using-sqlalchemy-to-create-and-manage-postgresql-materialized-views/
#       but note this depends on flask. Essence is the same as a view.
# For an explanation of why we use `append_column` instead of `_make_proxy`, see. This is the core stuff
# for materialized views in SQLAlchemy, from its author:
#   https://bitbucket.org/zzzeek/sqlalchemy/issues/3616/calling-index-on-a-materialized-view

class CreateMaterializedView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

class DropMaterializedView(DDLElement):
    def __init__(self, name):
        self.name = name

class RefreshMaterializedView(DDLElement):
    def __init__(self, name, concurrently=False):
        self.name = name
        self.concurrently = concurrently

@compiler.compiles(CreateMaterializedView)
def compile(element, compiler, **kw):
    return 'CREATE MATERIALIZED VIEW {} AS {}'.format(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True))

@compiler.compiles(DropMaterializedView)
def compile(element, compiler, **kw):
    return 'DROP MATERIALIZED VIEW {}'.format(element.name)

@compiler.compiles(RefreshMaterializedView)
def compile(element, compiler, **kw):
    return 'REFRESH MATERIALIZED VIEW {} {}'.format(
        ('CONCURRENTLY' if element.concurrently else ''), element.name)


class MaterializedViewMixin(object):
    """Mixin for ORM classes that are materialized views. Defines the two key attributes of an ORM class,
    __table__ and __mapper_args__, based on the values of class attributes __selectable__ and __primary_key__.

    Usage:

        class Thing(Base, MaterializedViewMixin):
            __selectable__ = <SQLAlchemy selectable>
            __primary_key__ = ['primary', 'key', 'columns']

        __primary_key__ attribute is optional and may be omitted if __selectable__ already defines primary keys.
        It must be defined otherwise (e.g., text selectables with anonymous columns; see tests).

    To create a materialized view in the database:
        Base.metadata.create_all()
    or
        Thing.create()

    To drop a materialized view from the database:
        Base.metadata.drop_all()
    or
        Thing.drop()

    To refresh a materialized view in the database:
        Thing.refresh()

    """

    @declared_attr
    def __table__(cls):
        temp_metadata = MetaData()
        t = Table(cls.viewname(), temp_metadata)
        for c in cls.__selectable__.c:
            t.append_column(Column(c.name, c.type, primary_key=c.primary_key))

        # Not sure if this will work, but it reproduces the setting above ...
        # This event liseter causes indexes defined for the materialized view to be created after the mat view
        # table is created by calling metadata.create_all().
        # However, this is no the only way to create this table, and so it may be necessary to add index creation
        # code to method .create() below.
        @event.listens_for(cls.metadata, "after_create")
        def create_indexes(target, connection, **kw):
            for idx in t.indexes:
                idx.create(connection)

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
        return snake_case(cls.__name__) + '_mv'

    @classmethod
    def create(cls, sesh):
        sesh.execute(CreateMaterializedView(cls.viewname(), cls.__selectable__))
        # TODO: Add index creation code here?

    @classmethod
    def drop(cls, sesh):
        sesh.execute(DropMaterializedView(cls.viewname()))

    @classmethod
    def refresh(cls, sesh, concurrently=False):
        sesh.execute(RefreshMaterializedView(cls.viewname(), concurrently))