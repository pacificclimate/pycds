"""
Plugins that add to the operations available in migrations.

This module provides a few simple additional operations, such as `set_role`,
but its major contribution is to add the operations `create_replaceable_object`,
`drop_replaceable_object`, and `replace_replaceable_object`.

For information on Alembic operation plugins, see
https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins
"""
import logging
from sqlalchemy.schema import CreateIndex
from alembic.operations import BatchOperations, Operations, MigrateOperation
from alembic.operations.ops import (
    CreateTableOp,
    DropConstraintOp,
    CreateIndexOp,
)
from alembic import util
from alembic.util import sqla_compat


logger = logging.getLogger("alembic")


# Extended table operations

@Operations.register_operation("drop_table_if_exists")
class DropTableIfExistsOp(MigrateOperation):
    """
    Provide DROP TABLE IF EXISTS command.

    TODO: Inherit from Alembic builtin DropTableOp? See DropConstraintIfExistsOp
    """

    def __init__(self, name, schema=None):
        self.name = name
        self.schema = schema

    @classmethod
    def drop_table_if_exists(cls, operations, name, **kw):
        """Issue a DROP TABLE IF EXISTS command."""
        op = cls(name, **kw)
        return operations.invoke(op)

    def reverse(self):
        """
        To support autogenerate.

        CAUTION: Conditional operations ("IF EXISTS") are not reliably
        reversible without memory of whether the table was actually dropped
        in the forward operation. This means a table may be incorrectly created
        in the autogenerated downgrade (I think). The user will have to add
        logic to the downgrade to solve this problem, if that is possible.
        """
        return CreateTableOp(self.name, schema=self.schema)


@Operations.implementation_for(DropTableIfExistsOp)
def drop_table_if_exists(operations, operation):
    # TODO: Refactor into a DDL extension.
    schema_prefix = (
        f"{operation.schema}." if operation.schema is not None else ""
    )
    operations.execute(f"DROP TABLE IF EXISTS {schema_prefix}{operation.name}")


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
        cls, operations, constraint_name, type_=None
    ):
        return cls.drop_constraint(operations, constraint_name, type_=type_)


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
    schema_prefix = (
        f"{operation.schema}." if operation.schema is not None else ""
    )
    operations.execute(
        f"ALTER TABLE {schema_prefix}{operation.table_name} "
        f"DROP CONSTRAINT IF EXISTS {operation.constraint_name}"
    )


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
        **kw
    ):
        logger.debug(f"create_index_if_not_exists")
        return cls.create_index(
            operations,
            index_name,
            table_name,
            columns,
            schema=schema,
            unique=unique,
            **kw
        )

    @classmethod
    def batch_create_index_if_not_exists(
        cls, operations, index_name, columns, **kw
    ):
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


# Reversible operations

class ReversibleOperation(MigrateOperation):
    """
    Base class for reversible Alembic migration operations.

    A reversible operation is one capable of emitting create and drop
    instructions for an object, and of "reversing" the creation (or dropping)
    of such an object. It does this by accessing other migration scripts in
    order to use different (previous or later) versions, enabling an object
    from one revision to be replaced by its version from another revision.
    It does this so it can invoke the appropriate drop/create operation on
    the old object before invoking the create/drop operation on the new object
    in order to replace one with the other. Access to different versions of
    an object is provided by method `_get_object_from_version`.
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


# Replaceable object reversible operations
# 
#  The reversible operations must know how to produce create and drop
#  commands for the target objects. This is done here by requiring that any 
#  target object (a replaceable object) to provide methods `create`  and
#  `drop` that return the requisite commands. These are invoked in the
# `implementation_for` of the classes that represent the operations.
#
# Since all replaceable objects conform to the same API, we do not need
# to specialize the operations for each different kind of replaceable object
# (view, matview, stored procedure). We can have just 3 generic operations
# (`create_replaceable_object`, `drop_replaceable_object`, and
# `replace_replaceable_object`) for all the different types of replaceable
# object.

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


# Miscellaneous operations

@Operations.register_operation("set_role")
class SetRoleOp(MigrateOperation):
    """Provide SET ROLE command"""

    def __init__(self, role_name):
        self.role_name = role_name

    @classmethod
    def set_role(cls, operations, name, **kw):
        """Issue a SET ROLE command."""
        return operations.invoke(cls(name))


@Operations.implementation_for(SetRoleOp)
def set_role(operations, operation):
    # TODO: Refactor into a DDL extension.
    operations.execute(f"SET ROLE '{operation.role_name}'")


@Operations.register_operation("reset_role")
class ResetRoleOp(MigrateOperation):
    """Provide RESET ROLE command"""

    @classmethod
    def reset_role(cls, operations, **kw):
        """Issue a RESET ROLE command."""
        return operations.invoke(cls())


@Operations.implementation_for(ResetRoleOp)
def reset_role(operations, operation):
    # TODO: Refactor into a DDL extension.
    operations.execute(f"RESET ROLE")
