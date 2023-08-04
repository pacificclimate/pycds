"""
DDLElements for SQL SET ROLE and RESET ROLE statements.
"""

from sqlalchemy.schema import DDLElement
from sqlalchemy.ext import compiler


class SetRole(DDLElement):
    def __init__(self, role_name):
        self.role_name = role_name


@compiler.compiles(SetRole)
def compile_set_role(element, compiler, **kw):
    return f"SET ROLE '{element.role_name}'"


class ResetRole(DDLElement):
    pass


@compiler.compiles(ResetRole)
def compile_reset_role(element, compiler, **kw):
    return "RESET ROLE"
