"""
This module defines SQLAlchemy DDLElement constructs that represent DDL
statements not built in to SQLAlchemy. Effectively, it adds new "commands" to
SQLAlchemy on a par with those such as `sqlalchemy.schema.CreateSchema`.

The definition pattern is:
- Define a subclass of `DDLElement` that represents the command.
- Define a compilation (implementation) of the command.
"""
import re
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext import compiler


# Views of all types


class ViewCommonDDL(DDLElement):
    """Base class for several varieties of view commands."""

    def __init__(self, name, selectable=None):
        self.name = name
        self.selectable = selectable


# View commands (not materialized)


class CreateView(ViewCommonDDL):
    pass


@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    body = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    return f"CREATE VIEW {element.name} AS {body}"


class DropView(ViewCommonDDL):
    pass


@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return f"DROP VIEW {element.name}"


# Native or manual materialized view commands
#
# We reduce repetitive code by folding both native and materialized matviews
# into a single command, distinguished by a `type_` parameter. Default type is
# native. This could also be done by creating separate command classes for
# native and manual matviews. That seemed unnecessarily wordy and bulky, but one
# objection to the present approach is that native and manual matviews are
# actually two entirely different kinds of object under the same name.


class MaterializedViewDDL(ViewCommonDDL):
    def __init__(self, name, selectable=None, type_="native"):
        super().__init__(name, selectable)
        self.type_ = type_


class CreateMaterializedView(MaterializedViewDDL):
    pass


@compiler.compiles(CreateMaterializedView)
def compile(element, compiler, **kw):
    body = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    if element.type_ == "native":
        return f"CREATE MATERIALIZED VIEW {element.name} AS {body}"
    if element.type_ == "manual":
        return f"CREATE TABLE {element.name} AS {body}"
    raise ValueError(f"Invalid materialized view type '{element.type_}'")


class DropMaterializedView(MaterializedViewDDL):
    pass


@compiler.compiles(DropMaterializedView)
def compile(element, compiler, **kw):
    if element.type_ == "native":
        return f"DROP MATERIALIZED VIEW {element.name}"
    if element.type_ == "manual":
        return f"DROP TABLE {element.name}"
    raise ValueError(f"Invalid materialized view type '{element.type_}'")


class RefreshMaterializedView(MaterializedViewDDL):
    def __init__(self, name, selectable=None, type_="native", concurrently=False):
        super().__init__(name, selectable=selectable, type_=type_)
        self.concurrently = concurrently


@compiler.compiles(RefreshMaterializedView)
def compile(element, compiler, **kw):
    if element.type_ == "native":
        concurrently = "CONCURRENTLY" if element.concurrently else ""
        return f"REFRESH MATERIALIZED VIEW {concurrently} {element.name}"
    if element.type_ == "manual":
        body = compiler.sql_compiler.process(element.selectable, literal_binds=True)
        return f"TRUNCATE TABLE {element.name}; INSERT INTO {element.name} {body}"
    raise ValueError(f"Invalid materialized view type '{element.type_}'")


# Stored procedure commands


class FunctionDDL(DDLElement):
    """Base class for function commands."""

    def __init__(self, identifier, definition=None):
        self.identifier = identifier
        self.definition = definition


class CreateFunction(FunctionDDL):
    def __init__(self, name, definition=None, replace=False):
        super().__init__(name, definition)
        self.replace = replace


@compiler.compiles(CreateFunction)
def compile(element, compiler, **kw):
    opt_replace = "OR REPLACE" if element.replace else ""
    return f"CREATE {opt_replace} FUNCTION {element.identifier} {element.definition}"


class DropFunction(FunctionDDL):
    pass


@compiler.compiles(DropFunction)
def compile(element, compiler, **kw):
    # PostgreSQL throws an error if the "DEFAULT ..." portion of the function
    # signature is included in the DROP FUNCTION statement. So ditch it.
    name = re.sub(
        r" (DEFAULT|=) [^,)]*([,)])",
        r"\2",
        element.identifier,
        flags=re.MULTILINE,
    )
    return f"DROP FUNCTION {name}"
