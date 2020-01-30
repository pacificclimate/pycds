from alembic.operations import Operations, MigrateOperation


class ReplaceableObject(object):
    """A very simple way to represent a named set of SQL."""
    def __init__(self, name, sqltext):
        self.name = name
        self.sqltext = sqltext


@Operations.register_operation("create_stored_procedure")
class CreateStoredProcedureOp(MigrateOperation):
    """Create a FUNCTION."""

    def __init__(self, target, schema=None):
        self.target = target
        self.schema = schema

    @classmethod
    def create_stored_procedure(cls, operations, target, **kw):
        """Issue a "CREATE SEQUENCE" instruction."""

        op = CreateStoredProcedureOp(target, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return DropStoredProcedureOp(self.target, schema=self.schema)


@Operations.register_operation("drop_stored_procedure")
class DropStoredProcedureOp(MigrateOperation):
    """Drop a FUNCTION."""

    def __init__(self, target, schema=None):
        self.target = target
        self.schema = schema

    @classmethod
    def drop_stored_procedure(cls, operations, target, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = DropStoredProcedureOp(target, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return CreateStoredProcedureOp(self.target, schema=self.schema)


@Operations.implementation_for(CreateStoredProcedureOp)
def create_stored_procedure(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.target.name)
    else:
        name = operation.name
    operations.execute(f"CREATE FUNCTION {name} {operation.target.sqltext}")


@Operations.implementation_for(DropStoredProcedureOp)
def drop_stored_procedure(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.target.name)
    else:
        name = operation.target.name
    operations.execute(f"DROP FUNCTION {name}")