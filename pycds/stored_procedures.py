from alembic.operations import Operations, MigrateOperation


@Operations.register_operation("create_stored_procedure")
class CreateStoredProcedureOp(MigrateOperation):
    """Create a FUNCTION."""

    def __init__(self, target, schema=None):
        self.name, self.sqltext = target
        self.schema = schema

    @classmethod
    def create_stored_procedure(cls, operations, target, **kw):
        """Issue a "CREATE SEQUENCE" instruction."""

        op = CreateStoredProcedureOp(target, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return DropStoredProcedureOp((self.name, self.sqltext), schema=self.schema)


@Operations.register_operation("drop_stored_procedure")
class DropStoredProcedureOp(MigrateOperation):
    """Drop a FUNCTION."""

    def __init__(self, target, schema=None):
        self.name, self.sqltext = target
        self.schema = schema

    @classmethod
    def drop_stored_procedure(cls, operations, target, **kw):
        """Issue a "DROP SEQUENCE" instruction."""

        op = DropStoredProcedureOp(target, **kw)
        return operations.invoke(op)

    def reverse(self):
        # only needed to support autogenerate
        return CreateStoredProcedureOp((self.name, self.sqltext), schema=self.schema)


@Operations.implementation_for(CreateStoredProcedureOp)
def create_stored_procedure(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.name)
    else:
        name = operation.name
    operations.execute(f"CREATE FUNCTION {name} {operation.sqltext}")


@Operations.implementation_for(DropStoredProcedureOp)
def drop_stored_procedure(operations, operation):
    if operation.schema is not None:
        name = "%s.%s" % (operation.schema, operation.name)
    else:
        name = operation.name
    operations.execute(f"DROP FUNCTION {name}")