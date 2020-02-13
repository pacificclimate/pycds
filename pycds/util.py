import pycds


def check_migration_version(
    executor,
    schema_name=pycds.get_schema_name(),
    # `version` must be kept up to date with latest migration
    # a test checks it, however, in case you don't
    version='8fd8f556c548',
):
    """Check that the migration version of the database schema is compatible
    with the current version of this package.

    This implementation is quick and easy, relying on manual updating of the
    correct version number.
    """
    current = executor.execute(f"""
        SELECT version_num 
        FROM {schema_name}.alembic_version
    """).scalar()
    if  current != version:
        raise ValueError(
            f"Schema {schema_name} must be at Alembic version {version}; "
            f"detected version {current}."
        )


# TODO: Does this have any current utility? It is not used in any current code.
#  Also its result can be achieved with 1 line:
#   ...statement.compile(compile_kwargs={"literal_binds": True}))
# http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
def compile_query(statement, bind=None):
    """
    print a query, with values filled in
    for debugging purposes *only*
    for security, you should always separate queries from their values
    please also note that this function is quite slow
    """
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        if bind is None:
            bind = statement.session.get_bind(
                statement._mapper_zero_or_none()
            )
            statement = statement.statement
        elif bind is None:
            bind = statement.bind

        dialect = bind.dialect
        compiler = statement._compiler(dialect)

        class LiteralCompiler(compiler.__class__):

            def visit_bindparam(
                    self, bindparam, within_columns_clause=False,
                    literal_binds=False, **kwargs
            ):
                return super(LiteralCompiler, self).render_literal_bindparam(
                    bindparam, within_columns_clause=within_columns_clause,
                    literal_binds=literal_binds, **kwargs
                )

    compiler = LiteralCompiler(dialect, statement)
    return compiler.process(statement)
