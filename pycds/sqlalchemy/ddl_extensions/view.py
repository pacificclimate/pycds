from sqlalchemy.ext import compiler

from pycds.util import compact_join
from ..ddl_extensions.view_common import ViewCommonDDL


class CreateView(ViewCommonDDL):
    def __init__(self, name, selectable=None, replace=False):
        super().__init__(name, selectable)
        self.replace = replace


@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    # TODO: Add escape
    selectable = element.selectable
    body = compiler.sql_compiler.process(
        # We permit the selectable to be deferred until create time via a function call.
        # Deferral is a way to avoid circular imports.
        selectable() if callable(selectable) else selectable, literal_binds=True
    )
    return compact_join(
        "CREATE",
        element.replace and "OR REPLACE",
        f"VIEW {element.name} AS  {body}",
    )


class DropView(ViewCommonDDL):
    def __init__(self, name, if_exists=False):
        super().__init__(name)
        self.if_exists = if_exists


@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return compact_join(
        "DROP VIEW",
        element.if_exists and "IF EXISTS",
        element.name,
    )
