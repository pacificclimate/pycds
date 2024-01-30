"""
Plugins that add GRANT operations available to migrations in Alembic.

A plugin has 2 parts:
- an operation that is registered with Alembic; a subclass of MigrateOperation
- an implementation of the operation; a method that executes a SQL command
"""

from alembic.operations import Operations, MigrateOperation
from pycds.sqlalchemy.ddl_extensions import GrantTablePrivileges


@Operations.register_operation("grant_table_privileges")
class GrantTablePrivilegesOp(MigrateOperation):
    """Alembic operation for GRANT directive on a table-like object"""

    def __init__(self, privileges, table_name, role_specification, schema=None):
        self.privileges = privileges
        self.table_name = table_name
        self.role_specification = role_specification
        self.schema = schema

    @classmethod
    def grant_table_privileges(cls, operations, *args, **kw):
        """Issue a GRANT on table-like object command."""
        return operations.invoke(cls(*args, **kw))


@Operations.implementation_for(GrantTablePrivilegesOp)
def implement_grant_table_privileges(operations, operation):
    operations.execute(
        GrantTablePrivileges(
            operation.privileges,
            operation.table_name,
            operation.role_specification,
            schema=operation.schema,
        )
    )
