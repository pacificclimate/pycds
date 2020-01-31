"""Establishes a set of tools for "replaceable" schema objects, such as
stored procedures, views, and triggers, that must be created and dropped
all at once.

This code is adapted from
https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects,
plus information on how to incorporate schema name gleaned from
https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins

TODO: Split out into separate modules: ReplaceableObject, ReversibleOp,
  stored_procedure operations.
"""
import re
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
    def __init__(self, target, schema=None):
        self.target = target
        self.schema = schema

    @classmethod
    def invoke_for_target(cls, operations, target, **kw):
        op = cls(target, **kw)
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
    def replace(cls, operations, target, replaces=None, replace_with=None, **kw):
        """
        Migration upgrade uses `replaces`.
        Migration downgrade uses `replace_with`.
        """

        if replaces:
            old_obj = cls._get_object_from_version(operations, replaces)
            drop_old = cls(old_obj, **kw).reverse()
            create_new = cls(target, **kw)
        elif replace_with:
            old_obj = cls._get_object_from_version(operations, replace_with)
            drop_old = cls(target, **kw).reverse()
            create_new = cls(old_obj, **kw)
        else:
            raise TypeError("replaces or replace_with is required")

        operations.invoke(drop_old)
        operations.invoke(create_new)


@Operations.register_operation("create_stored_procedure", "invoke_for_target")
@Operations.register_operation("replace_stored_procedure", "replace")
class CreateSPOp(ReversibleOp):
    def reverse(self):
        return DropSPOp(self.target, schema=self.schema)


@Operations.register_operation("drop_stored_procedure", "invoke_for_target")
class DropSPOp(ReversibleOp):
    def reverse(self):
        return CreateSPOp(self.target, schema=self.schema)


def schema_prefix(schema):
    return (schema and f"{schema}.") or ""


@Operations.implementation_for(CreateSPOp)
def create_stored_procedure(operations, operation):
    operations.execute(
        f"CREATE FUNCTION "
        f"{schema_prefix(operation.schema)}{operation.target.name} "
        f"{operation.target.sqltext}"
    )


@Operations.implementation_for(DropSPOp)
def drop_stored_procedure(operations, operation):
    # PostgreSQL throws an error if the "DEFAULT ..." portion of the function
    # signature is included in the DROP FUNCTION statement. So ditch it.
    name = re.sub(
        r" (DEFAULT|=) [^,)]*([,)])",
        r"\2",
        operation.target.name,
        flags=re.MULTILINE
    )
    operations.execute(f"DROP FUNCTION {schema_prefix(operation.schema)}{name}")
