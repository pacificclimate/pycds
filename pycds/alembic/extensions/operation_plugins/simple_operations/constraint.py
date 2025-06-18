"""
Plugins that add constraint operations available to migrations in Alembic.

A plugin has 2 parts:
- an operation that is registered with Alembic; a subclass of MigrateOperation
- an implementation of the operation; a method that executes a SQL command
"""

import logging
from alembic import util
from alembic.operations import BatchOperations, Operations
from alembic.operations.ops import DropConstraintOp


logger = logging.getLogger("alembic")


@Operations.register_operation("drop_constraint_if_exists")
@BatchOperations.register_operation(
    "drop_constraint_if_exists", "batch_drop_constraint_if_exists"
)
class DropConstraintIfExistsOp(DropConstraintOp):
    """
    Represent a drop constraint if exists operation.

    This operation's class is identical to DropConstraintOp except for
    class method names which become operation names. The new names dispatch
    to the old names.
    """

    @classmethod
    @util._with_legacy_names([("type", "type_"), ("name", "constraint_name")])
    def drop_constraint_if_exists(
        cls, operations, constraint_name, table_name, type_=None, schema=None
    ):
        return cls.drop_constraint(
            operations, constraint_name, table_name, type_=type_, schema=schema
        )

    @classmethod
    def batch_drop_constraint_if_exists(
        cls, operations, constraint_name, table_name, type_=None, schema=None
    ):
        return cls.drop_constraint(
            operations, constraint_name, table_name, type_=type_, schema=schema
        )


@Operations.implementation_for(DropConstraintIfExistsOp)
def drop_constraint_if_exists(operations, operation):
    """
    Implement drop constraint if exists operation. This is a greatly simplified
    version of how operations are implemented in Alembic, where there is much
    dispatching between here and the final SQL generation.

    **This version is only guaranteed to work for PostgreSQL.**

    TODO: Refactor into a DDL extension. Maybe. Or use the built-in SQLAlchemy
      DropConstraint DDL element, and -- ack, barf -- modify its result.
    """
    schema_prefix = f"{operation.schema}." if operation.schema is not None else ""
    operations.execute(
        f"ALTER TABLE {schema_prefix}{operation.table_name} "
        f"DROP CONSTRAINT IF EXISTS {operation.constraint_name}"
    )
