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
    return "CREATE MATERIALIZED VIEW %s AS %s" % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True))

@compiler.compiles(DropMaterializedView)
def compile(element, compiler, **kw):
    return "DROP MATERIALIZED VIEW %s" % (element.name)

@compiler.compiles(RefreshMaterializedView)
def compile(element, compiler, **kw):
    return "REFRESH MATERIALIZED VIEW %s %s" % \
           (('CONCURRENTLY' if element.concurrently else ''), element.name)

def create_materialized_view(name, metadata, selectable):
    temp_metadata = MetaData()
    t = Table(name, temp_metadata)
    for c in selectable.c:
        t.append_column(Column(c.name, c.type, primary_key=c.primary_key))

    CreateMaterializedView(name, selectable).execute_at('after-create', metadata)

    @event.listens_for(metadata, "after_create")
    def create_indexes(target, connection, **kw):
        for idx in t.indexes:
            idx.create(connection)

    DropMaterializedView(name).execute_at('before-drop', metadata)

    return t

def refresh_materialized_view(sesh, object, concurrently=False):
    sesh.execute(RefreshMaterializedView(object.__table__.fullname, concurrently))


class MaterializedViewMixin(object):
    """Mixin for ORM classes that are materialized views. Defines the two key attributes of an ORM class,
    __table__ and __mapper_args__, based on the values of class attributes __selectable__ and __primary_key__.

    Usage:

    class Thing(Base, MaterializedViewMixin):
        __selectable__ = <SQLAlchemy selectable>
        __primary_key__ = ['primary', 'key', 'columns']

    __primary_key__ attribute is optional and may be omitted if __selectable__ already defines primary keys.
    It must be defined otherwise (e.g., text selectables with anonymous columns; see tests).

    """

    @declared_attr
    def __table__(cls):
        return create_materialized_view(cls.viewname(), cls.metadata, cls.__selectable__)

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
    def refresh(cls, sesh, concurrently=False):
        # TODO: Can we do without passing in sesh?
        sesh.execute(RefreshMaterializedView(cls.viewname(), concurrently))