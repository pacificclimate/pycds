from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy import event
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.sql import table

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
# TODO: Integrate with non-materialized view classes and methods (add argument `materialized`)?

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