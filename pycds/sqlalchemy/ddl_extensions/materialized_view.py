from sqlalchemy.ext import compiler

from pycds.util import compact_join
from ..ddl_extensions.view_common import ViewCommonDDL


# Native or manual materialized view commands
#
# We reduce repetitive code by folding both native and manual matviews
# into a single command, distinguished by a `type_` parameter. Default type is
# native. This could also be done by creating separate command classes for
# native and manual matviews. That seemed unnecessarily wordy and bulky, but one
# objection to the present approach is that native and manual matviews are
# actually two entirely different kinds of object under the same name.


class MaterializedViewDDL(ViewCommonDDL):
    def __init__(self, name, selectable=None, type_="native"):
        super().__init__(name, selectable=selectable)
        self.type_ = type_


class CreateMaterializedView(MaterializedViewDDL):
    def __init__(self, name, selectable=None, type_="native", if_not_exists=False):
        super().__init__(name, selectable=selectable, type_=type_)
        self.if_not_exists = if_not_exists


@compiler.compiles(CreateMaterializedView)
def compiles(element, compiler, **kw):
    body = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    if element.type_ == "native":
        return compact_join(
            "CREATE MATERIALIZED VIEW",
            element.if_not_exists and "IF NOT EXISTS",
            element.name,
            "AS",
            body,
        )
    if element.type_ == "manual":
        # NOTE: No if_not_exists functionality.
        return f"CREATE TABLE {element.name} AS {body}"
    raise ValueError(f"Invalid materialized view type '{element.type_}'")


class DropMaterializedView(MaterializedViewDDL):
    def __init__(self, name, selectable=None, type_="native", if_exists=False):
        super().__init__(name, selectable=selectable, type_=type_)
        self.if_exists = if_exists


@compiler.compiles(DropMaterializedView)
def compiles(element, compiler, **kw):
    if element.type_ == "native":
        return compact_join(
            "DROP MATERIALIZED VIEW",
            element.if_exists and "IF EXISTS",
            element.name,
        )
    if element.type_ == "manual":
        # NOTE: No if_exists functionality.
        return f"DROP TABLE {element.name}"
    raise ValueError(f"Invalid materialized view type '{element.type_}'")


class RefreshMaterializedView(MaterializedViewDDL):
    def __init__(self, name, selectable=None, type_="native", concurrently=False):
        super().__init__(name, selectable=selectable, type_=type_)
        self.concurrently = concurrently


@compiler.compiles(RefreshMaterializedView)
def compiles(element, compiler, **kw):
    if element.type_ == "native":
        return compact_join(
            "REFRESH MATERIALIZED VIEW",
            element.concurrently and "CONCURRENTLY",
            element.name,
        )
    if element.type_ == "manual":
        body = compiler.sql_compiler.process(element.selectable, literal_binds=True)
        return f"TRUNCATE TABLE {element.name}; INSERT INTO {element.name} {body}"
    raise ValueError(f"Invalid materialized view type '{element.type_}'")
