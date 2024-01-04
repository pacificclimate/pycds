import re
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from pycds.context import get_schema_name


schema_func = getattr(func, get_schema_name())  # Explicitly specify schema of function


def variable_tags(Table):
    return schema_func.variable_tags(text(Table.__tablename__), type_=ARRAY(TEXT))


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
            bind = statement.session.get_bind(statement._mapper_zero_or_none())
            statement = statement.statement
        elif bind is None:
            bind = statement.bind

        dialect = bind.dialect
        compiler = statement._compiler(dialect)

        class LiteralCompiler(compiler.__class__):
            def visit_bindparam(
                self,
                bindparam,
                within_columns_clause=False,
                literal_binds=False,
                **kwargs
            ):
                return super(LiteralCompiler, self).render_literal_bindparam(
                    bindparam,
                    within_columns_clause=within_columns_clause,
                    literal_binds=literal_binds,
                    **kwargs
                )

    compiler = LiteralCompiler(dialect, statement)
    return compiler.process(statement)


def snake_case(ident):
    """
    Return a snake-case version of a camel-case identifier, e.g.,
    "MyBigDeal" -> "my_big_deal".
    Courtesy of http://stackoverflow.com/a/12867228
    """
    a = re.compile("((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))")
    return a.sub(r"_\1", ident).lower()


def ddl_escape(s):
    """
    Encode a string so that SQLAlchemy DDL will process it as intended.
    See https://docs.sqlalchemy.org/en/14/core/ddl.html#sqlalchemy.schema.DDL
    """
    return s.replace("%", "%%")


def compact_join(*parts, separator=" "):
    """
    Compact (eliminate falsy values from) arguments and join them with a separator.
    Typically used to construct a SQL statement with optional parts.
    """
    return separator.join(filter(None, parts))
