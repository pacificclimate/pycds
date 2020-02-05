"""This module defines and registers operations for creating, replacing, and
dropping materialized views.

A view is expected to be defined using an instance of a class descended from
`pycds.view_helpers.MaterializedViewMixin`.
If this changes, then the operation implementations must change correspondingly,
namely, `operation.target` will be a different kind of object.
"""

from alembic.operations import Operations
from . import ReversibleOp, schema_prefix
from pycds.materialized_view_helpers import (
    create_materialized_view, drop_materialized_view
)


@Operations.register_operation("create_materialized_view", "invoke_for_target")
@Operations.register_operation("replace_materialized_view", "replace")
class CreateMaterializedViewOp(ReversibleOp):
    def reverse(self):
        return DropMaterializedViewOp(self.target, schema=self.schema)


@Operations.register_operation("drop_materialized_view", "invoke_for_target")
class DropMaterializedViewOp(ReversibleOp):
    def reverse(self):
        return CreateMaterializedViewOp(self.target, schema=self.schema)


def target_name(operation):
    return f"{schema_prefix(operation.schema)}" \
           f"{operation.target.base_viewname()}"


@Operations.implementation_for(CreateMaterializedViewOp)
def create_materialized_view_op(operations, operation):
    operations.execute(
        create_materialized_view(
            target_name(operation),
            operation.target.__selectable__.compile(
                compile_kwargs={"literal_binds": True}
            )
        )
    )


@Operations.implementation_for(DropMaterializedViewOp)
def drop_materialized_view_op(operations, operation):
    operations.execute(drop_materialized_view(target_name(operation)))
