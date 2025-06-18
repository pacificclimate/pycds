"""
Base class for reversible Alembic migration operations. This is a subclass
of MigrateOperation.

This is required because such an operation must invoke the appropriate drop/create
operation on the old object before invoking the create/drop operation on the new
object in order to replace one with the other. It does this by accessing other
migration scripts so that it can use different (previous or later) versions, enabling
an object from one revision to be replaced by its version from another revision.

Access to different versions of an object is provided by method
`_get_object_from_version`.
"""

from alembic.operations import MigrateOperation


class ReversibleOperation(MigrateOperation):
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
