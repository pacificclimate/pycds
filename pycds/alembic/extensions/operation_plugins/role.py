"""
Plugins that add role operations available to migrations in Alembic.

A plugin has 2 parts:
- an operation that is registered with Alembic; a subclass of MigrateOperation
- an implementation of the operation; a method that executes a SQL command
"""
from alembic.operations import Operations, MigrateOperation


@Operations.register_operation("set_role")
class SetRoleOp(MigrateOperation):
    """Provide SET ROLE command"""

    def __init__(self, role_name):
        self.role_name = role_name

    @classmethod
    def set_role(cls, operations, name, **kw):
        """Issue a SET ROLE command."""
        return operations.invoke(cls(name))


@Operations.implementation_for(SetRoleOp)
def set_role(operations, operation):
    # TODO: Refactor into a DDL extension.
    operations.execute(f"SET ROLE '{operation.role_name}'")


@Operations.register_operation("reset_role")
class ResetRoleOp(MigrateOperation):
    """Provide RESET ROLE command"""

    @classmethod
    def reset_role(cls, operations, **kw):
        """Issue a RESET ROLE command."""
        return operations.invoke(cls())


@Operations.implementation_for(ResetRoleOp)
def reset_role(operations, operation):
    # TODO: Refactor into a DDL extension.
    operations.execute(f"RESET ROLE")
