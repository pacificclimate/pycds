from sqlalchemy.ext import compiler
from ..ddl_extensions.view_common import ViewCommonDDL


class CreateView(ViewCommonDDL):
    pass


@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    # TODO: Add escape
    # TODO: Handle replace
    body = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    return f"CREATE VIEW {element.name} AS {body}"


class DropView(ViewCommonDDL):
    pass


@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return f"DROP VIEW {element.name}"
