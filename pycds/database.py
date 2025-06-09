import re

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

from pycds.context import get_schema_name


def check_migration_version(
    executor, schema_name=get_schema_name(), version="758be4f4ce0f"
):
    """Check that the migration version of the database schema is compatible
    with the current version of this package.

    This implementation is quick and easy, relying on manual updating of the
    correct version number.
    """
    current = executor.execute(
        text(
            f"""
        SELECT version_num 
        FROM {schema_name}.alembic_version
    """
        )
    ).scalar()
    if current != version:
        raise ValueError(
            f"Schema {schema_name} must be at Alembic version {version}; "
            f"detected version {current}."
        )


def get_postgresql_version(engine):
    """
    Return PostgreSQL version as a pair of integers.

    This function is only works PostgreSQL. On other DBMSs, it will raise
    a ValueError.

    This has no use at present but going to leave it here in case it does again
    in future.
    """
    # print(f"dialect: {engine.dialect.name}")
    if engine.dialect.name.lower() != "postgresql":
        raise ValueError(f"We are not running on PostgreSQL.")

    with engine.begin() as conn:
        version = conn.execute(text("SELECT version()")).first()[0]
        match = re.match(r"PostgreSQL (\d+)\.(\d+)", version)
        return tuple(map(int, match.groups()))


def db_supports_statement(engine, statement):
    """
    Return a boolean indicating whether this database instance can execute the
    statement. The statement is executed in a transaction and rolled back
    afterwards, so the effect on the database is null (so long as the statement
    itself can be executed in a transaction).

    Note: We must create a session to do this test. Creating a transaction
    directly from the Alembic connection failed: the entire pre-existing Alembic
    operation transaction was rolled back, not just the one supposedly enclosing
    this test execution.
    """

    Session = sessionmaker(bind=engine)
    session = Session()
    savepoint = session.begin_nested()
    try:
        session.execute(text(statement))
        return True
    except ProgrammingError:
        return False
    finally:
        savepoint.rollback()
        session.close()


def db_supports_matviews(engine):
    return db_supports_statement(engine, "CREATE MATERIALIZED VIEW test AS SELECT 1")


# TODO: Break this up into separate functions for each item type.
# TODO: Use `inspect()` to get information.
# Note: Prefer `Inspector` methods to this function wherever possible.
# It's easy: e.g., `inspect(engine).get_table_names(schema=...)`
def get_schema_item_names(
    executor: Connection,
    item_type,
    table_name=None,
    constraint_type=None,
    schema_name=get_schema_name(),
):
    if item_type == "routines":
        r = executor.execute(
            text(
                f"""
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE specific_schema = '{schema_name}'
        """
            )
        )
    elif item_type == "tables":
        r = executor.execute(
            text(
                f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}';
        """
            )
        )
    elif item_type == "views":
        r = executor.execute(
            text(
                f"""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = '{schema_name}';
        """
            )
        )
    elif item_type == "matviews":
        r = executor.execute(
            text(
                f"""
            SELECT matviewname 
            FROM pg_matviews
            WHERE schemaname = '{schema_name}';
        """
            )
        )
    elif item_type == "indexes":
        r = executor.execute(
            text(
                f"""
            SELECT indexname 
            FROM pg_indexes
            WHERE schemaname = '{schema_name}'
            AND tablename = '{table_name}';
        """
            )
        )
    elif item_type == "constraints":
        contype = constraint_type[0]
        if contype == "e":
            contype = "x"
        sql = f"""
            SELECT conname  
            FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace            
            WHERE nsp.nspname = '{schema_name}'
            AND rel.relname = '{table_name}'
            AND con.contype = '{contype}';
        """
        r = executor.execute(text(sql))
    else:
        raise ValueError("invalid item type")
    return {x[0] for x in r.fetchall()}


def matview_exists(engine, name, schema=None):
    # TODO: Use this when we move to SQLA 2.x
    # matview_names = inspect(engine).get_materialized_view_names(schema=schema)
    matview_names = get_schema_item_names(engine, "matviews", schema_name=schema)
    return name in matview_names
