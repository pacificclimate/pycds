"""
Plugins that add role operations available to migrations in Alembic.

A plugin has 2 parts:
- an operation that is registered with Alembic; a subclass of MigrateOperation
- an implementation of the operation; a method that executes a SQL command
"""

from alembic.operations import Operations, MigrateOperation
from pycds.sqlalchemy.ddl_extensions import SetRole, ResetRole


@Operations.register_operation("set_role")
class SetRoleOp(MigrateOperation):
    """SET ROLE operation"""

    def __init__(self, role_name):
        self.role_name = role_name

    @classmethod
    def set_role(cls, operations, role_name, **kw):
        """Issue a SET ROLE command."""
        return operations.invoke(cls(role_name))


@Operations.implementation_for(SetRoleOp)
def implement_set_role(operations, operation):
    operations.execute(SetRole(operation.role_name))


@Operations.register_operation("reset_role")
class ResetRoleOp(MigrateOperation):
    """RESET ROLE operation"""

    @classmethod
    def reset_role(cls, operations, **kw):
        """Issue a RESET ROLE command."""
        return operations.invoke(cls())


@Operations.implementation_for(ResetRoleOp)
def implement_reset_role(operations, operation):
    operations.execute(ResetRole())
