from sqlalchemy.ext import compiler
from ..ddl_extensions.view_common import ViewCommonDDL


class CreateView(ViewCommonDDL):
    def __init__(self, name, selectable=None, replace=False):
        super().__init__(name, selectable)
        self.replace = replace


@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    # TODO: Add escape
    body = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    command_parts = [
        "CREATE",
        element.replace and "OR REPLACE",
        f"VIEW {element.name} AS  {body}",
    ]
    return " ".join(filter(None, command_parts))


class DropView(ViewCommonDDL):
    pass


@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return f"DROP VIEW {element.name}"
