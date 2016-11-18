from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table

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

def create_view(name, metadata, selectable):
    t = table(name)

    for c in selectable.c:
        c._make_proxy(t)

    CreateView(name, selectable).execute_at('after-create', metadata)
    DropView(name).execute_at('before-drop', metadata)

    return t

