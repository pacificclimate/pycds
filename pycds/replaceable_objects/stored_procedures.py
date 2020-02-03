"""This module defines and registers operations for creating, replacing, and
dropping stored procedures (functions).

A stored procedures is expected to be defined using a `ReplaceableObject`.
If this changes, then the operation implementations must change correspondingly.
"""

import re
from alembic.operations import Operations
from . import ReversibleOp, schema_prefix


@Operations.register_operation("create_stored_procedure", "invoke_for_target")
@Operations.register_operation("replace_stored_procedure", "replace")
class CreateSPOp(ReversibleOp):
    def reverse(self):
        return DropSPOp(self.target, schema=self.schema)


@Operations.register_operation("drop_stored_procedure", "invoke_for_target")
class DropSPOp(ReversibleOp):
    def reverse(self):
        return CreateSPOp(self.target, schema=self.schema)


@Operations.implementation_for(CreateSPOp)
def create_stored_procedure(operations, operation):
    """Implementation of "create a stored procedure."

    `operation.target` is expected to be an instance of `ReplaceableObject`.
    """
    operations.execute(
        f"CREATE FUNCTION "
        f"{schema_prefix(operation.schema)}{operation.target.name} "
        f"{operation.target.sqltext}"
    )


@Operations.implementation_for(DropSPOp)
def drop_stored_procedure(operations, operation):
    """Implementation of "drop a stored procedure."

    `operation.target` is expected to be an instance of `ReplaceableObject`.
    """
    # PostgreSQL throws an error if the "DEFAULT ..." portion of the function
    # signature is included in the DROP FUNCTION statement. So ditch it.
    name = re.sub(
        r" (DEFAULT|=) [^,)]*([,)])",
        r"\2",
        operation.target.name,
        flags=re.MULTILINE
    )
    operations.execute(f"DROP FUNCTION {schema_prefix(operation.schema)}{name}")
