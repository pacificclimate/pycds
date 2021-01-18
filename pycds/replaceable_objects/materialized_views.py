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
    create_manual_materialized_view,
    drop_manual_materialized_view,
)


@Operations.register_operation("create_manual_materialized_view", "invoke_for_target")
@Operations.register_operation("replace_manual_materialized_view", "replace")
class CreateManualMaterializedViewOp(ReversibleOp):
    def reverse(self):
        return DropManualMaterializedViewOp(self.target, schema=self.schema)


@Operations.register_operation("drop_manual_materialized_view", "invoke_for_target")
class DropManualMaterializedViewOp(ReversibleOp):
    def reverse(self):
        return CreateManualMaterializedViewOp(self.target, schema=self.schema)


def target_name(operation):
    return (
        f"{schema_prefix(operation.schema)}"
        f"{operation.target.base_viewname()}"
    )


@Operations.implementation_for(CreateManualMaterializedViewOp)
def create_manual_materialized_view_op(operations, operation):
    operations.execute(
        create_manual_materialized_view(
            target_name(operation),
            operation.target.__selectable__.compile(
                compile_kwargs={"literal_binds": True}
            ),
        )
    )


@Operations.implementation_for(DropManualMaterializedViewOp)
def drop_manual_materialized_view_op(operations, operation):
    operations.execute(drop_manual_materialized_view(target_name(operation)))
