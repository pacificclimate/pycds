"""
Plugins that extend index operations available to migrations in Alembic.

A plugin has 2 parts:
- an operation that is registered with Alembic; a subclass of MigrateOperation
- an implementation of the operation; a method that executes a SQL command
"""
import logging
from sqlalchemy.schema import CreateIndex
from alembic import util
from alembic.operations import BatchOperations, Operations
from alembic.operations.ops import CreateIndexOp


logger = logging.getLogger("alembic")


@Operations.register_operation("create_index_if_not_exists")
@BatchOperations.register_operation(
    "create_index_if_not_exists", "batch_create_index_if_not_exists"
)
class CreateIndexIfNotExists(CreateIndexOp):
    """
    Represent a create index if not exists operation.

    This operation's class is identical to DropConstraintOp except for
    class method names which become operation names. The new names dispatch
    to the old names.
    """

    @classmethod
    @util._with_legacy_names([("name", "index_name")])
    def create_index_if_not_exists(
        cls,
        operations,
        index_name,
        table_name,
        columns,
        schema=None,
        unique=False,
        **kw,
    ):
        logger.debug(f"create_index_if_not_exists")
        return cls.create_index(
            operations,
            index_name,
            table_name,
            columns,
            schema=schema,
            unique=unique,
            **kw,
        )

    @classmethod
    def batch_create_index_if_not_exists(cls, operations, index_name, columns, **kw):
        return cls.batch_create_index(operations, index_name, columns, **kw)


@Operations.implementation_for(CreateIndexIfNotExists)
def create_index_if_not_exists(operations, operation):
    """
    Implement drop constraint if exists operation. This is a simplified
    version of how operations are implemented in Alembic, where there is much
    dispatching between here and the final SQL generation.
    """
    index = operation.to_index(operations.migration_context)
    operations.execute(CreateIndex(index, if_not_exists=True))
