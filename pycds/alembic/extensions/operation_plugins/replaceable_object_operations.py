"""
Plugins that add replaceable object operations available to migrations in Alembic.

A plugin has 2 parts:
- an operation that is registered with Alembic; a subclass of MigrateOperation
- an implementation of the operation; a method that executes a SQL command

Replaceable object reversible operations

A reversible operation must know how to produce create and drop
commands for the target objects. This is done here by requiring any
target replaceable object to provide methods `create`  and
`drop` that return the requisite commands. These are invoked in the
`implementation_for` of the classes that represent the operations.

Since all replaceable objects conform to the same API, we do not need
to specialize the operations for each different kind of replaceable object
(view, matview, stored procedure). We can have just 3 generic operations
(`create_replaceable_object`, `drop_replaceable_object`, `replace_replaceable_object`)
to cover all types of replaceable object.
"""
from alembic.operations import Operations
from pycds.alembic.extensions.operation_plugins.reversible_operation import (
    ReversibleOperation,
)


@Operations.register_operation("create_replaceable_object", "invoke_for_target")
@Operations.register_operation("replace_replaceable_object", "replace")
class CreateReplaceableObjectOp(ReversibleOperation):
    """
    Class representing a reversible create operation for a replaceable object.
    This class also requires an implementation to make it executable.
    """

    def reverse(self):
        return DropReplaceableObjectOp(self.target)


@Operations.implementation_for(CreateReplaceableObjectOp)
def create_replaceable_object(operations, operation):
    operations.execute(operation.target.create())


@Operations.register_operation("drop_replaceable_object", "invoke_for_target")
class DropReplaceableObjectOp(ReversibleOperation):
    """
    Class representing a reversible drop operation for a replaceable object.
    This class also requires an implementation to make it executable.
    """

    def reverse(self):
        return CreateReplaceableObjectOp(self.target)


@Operations.implementation_for(DropReplaceableObjectOp)
def drop_replaceable_object(operations, operation):
    operations.execute(operation.target.drop())
