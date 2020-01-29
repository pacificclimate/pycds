"""Establishes a set of tools for "replaceable" schema objects, such as
stored procedures, views, and triggers, that must be created and dropped
all at once.

This code is adapted from
https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects
See that article for much more information on how this works.
"""
from alembic.operations import Operations, MigrateOperation


class ReplaceableObject(object):
    """A very simple way to represent a named set of SQL."""
    def __init__(self, name, sqltext):
        self.name = name
        self.sqltext = sqltext


class ReversibleOp(MigrateOperation):
    """Represents a reversible Alembic migration operation.

    This is the base of the “replaceable” operation, which includes not just
    a base operation for emitting CREATE and DROP instructions on a
    ReplaceableObject, it also assumes a certain model of “reversibility”
    which makes use of references to other migration files in order to refer
    to the “previous” version of an object.
    """
    def __init__(self, target):
        self.target = target

    @classmethod
    def invoke_for_target(cls, operations, target):
        op = cls(target)
        return operations.invoke(op)

    def reverse(self):
        raise NotImplementedError()

    @classmethod
    def _get_object_from_version(cls, operations, ident):
        version, objname = ident.split(".")

        module = operations.get_context().script.get_revision(version).module
        obj = getattr(module, objname)
        return obj

    @classmethod
    def replace(cls, operations, target, replaces=None, replace_with=None):
        """
        Migration upgrade uses `replaces`.
        Migration downgrade uses `replace_with`.
        """

        if replaces:
            old_obj = cls._get_object_from_version(operations, replaces)
            drop_old = cls(old_obj).reverse()
            create_new = cls(target)
        elif replace_with:
            old_obj = cls._get_object_from_version(operations, replace_with)
            drop_old = cls(target).reverse()
            create_new = cls(old_obj)
        else:
            raise TypeError("replaces or replace_with is required")

        operations.invoke(drop_old)
        operations.invoke(create_new)


# Stored procedure operations
# TODO: Move out to separate module?

@Operations.register_operation("create_sp", "invoke_for_target")
@Operations.register_operation("replace_sp", "replace")
class CreateSPOp(ReversibleOp):
    def reverse(self):
        return DropSPOp(self.target)


@Operations.register_operation("drop_sp", "invoke_for_target")
class DropSPOp(ReversibleOp):
    def reverse(self):
        return CreateSPOp(self.target)


@Operations.implementation_for(CreateSPOp)
def create_sp(operations, operation):
    operations.execute(
        f"CREATE FUNCTION {operation.target.name} {operation.target.sqltext}"
    )


@Operations.implementation_for(DropSPOp)
def drop_sp(operations, operation):
    operations.execute(f"DROP FUNCTION {operation.target.name}")
