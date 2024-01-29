"""
DDLElements for SQL GRANT statements.
"""

from sqlalchemy.schema import DDLElement
from sqlalchemy.ext import compiler


class GrantTablePrivileges(DDLElement):
    """"""

    def __init__(self, privileges, table_name, role_specification, schema=None):
        self.privileges = privileges
        self.table_name = table_name
        self.role_specification = role_specification
        self.schema = schema


@compiler.compiles(GrantTablePrivileges)
def compile_grant_table_privileges(element, compiler, **kw):
    privileges = ", ".join(element.privileges)
    schema_prefix = f"{element.schema}." if element.schema is not None else ""
    object_id = f"{schema_prefix}{element.table_name}"
    return f"GRANT {privileges} ON {object_id} TO {element.role_specification}"
