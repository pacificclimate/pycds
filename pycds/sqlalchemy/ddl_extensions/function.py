"""
DDLElements for SQL function statements.
"""

import re
from sqlalchemy.schema import DDLElement
from sqlalchemy.ext import compiler


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
