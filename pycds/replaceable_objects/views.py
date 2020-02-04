"""This module defines and registers operations for creating, replacing, and
dropping views.

A view is expected to be defined using an instance of a class descended from
`pycds.view_helpers.ViewMixin`.
If this changes, then the operation implementations must change correspondingly.
"""

from alembic.operations import Operations
from . import ReversibleOp, schema_prefix


@Operations.register_operation("create_view", "invoke_for_target")
@Operations.register_operation("replace_view", "replace")
class CreateViewOp(ReversibleOp):
    def reverse(self):
        return DropViewOp(self.target, schema=self.schema)


@Operations.register_operation("drop_view", "invoke_for_target")
class DropViewOp(ReversibleOp):
    def reverse(self):
        return CreateViewOp(self.target, schema=self.schema)


def target_name(operation):
    return f"{schema_prefix(operation.schema)}" \
           f"{operation.target.base_viewname()}"


@Operations.implementation_for(CreateViewOp)
def create_view(operations, operation):
    operations.execute(f"""
        CREATE VIEW {target_name(operation)} 
        AS {operation.target.__selectable__.compile(
                compile_kwargs={"literal_binds": True}
        )}
    """)


@Operations.implementation_for(DropViewOp)
def drop_view(operations, operation):
    operations.execute(f"DROP VIEW {target_name(operation)}")
