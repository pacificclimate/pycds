import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from pycds.util import get_schema_name


logger = logging.getLogger("alembic")


def get_postgresql_version(engine):
    """
    Return PostgreSQL version as a tuple of integers.

    This function is only works PostgreSQL. On other DBMSs, it will raise
    a ValueError.

    This has no use at present but going to leave it here in case it does again
    in future.
    """
    print(f"dialect: {engine.dialect.name}")
    if engine.dialect.name.lower() != "postgresql":
        raise ValueError(f"We are not running on PostgreSQL.")

    version = engine.execute(
        "SELECT setting FROM pg_settings WHERE name = " "'server_version'"
    ).first()[0]
    return tuple(int(n) for n in version.split("."))


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
    try:
        session.execute(statement)
        return True
    except ProgrammingError:
        return False
    finally:
        session.rollback()
        session.close()


def db_supports_matviews(engine):
    return db_supports_statement(
        engine, "CREATE MATERIALIZED VIEW test AS SELECT 1"
    )


def get_schema_item_names(
    executor,
    item_type,
    table_name=None,
    constraint_type=None,
    schema_name=get_schema_name(),
):
    if item_type == "routines":
        r = executor.execute(
            f"""
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE specific_schema = '{schema_name}'
        """
        )
    elif item_type == "tables":
        r = executor.execute(
            f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}';
        """
        )
    elif item_type == "views":
        r = executor.execute(
            f"""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = '{schema_name}';
        """
        )
    elif item_type == "matviews":
        r = executor.execute(
            f"""
            SELECT matviewname 
            FROM pg_matviews
            WHERE schemaname = '{schema_name}';
        """
        )
    elif item_type == "indexes":
        r = executor.execute(
            f"""
            SELECT indexname 
            FROM pg_indexes
            WHERE schemaname = '{schema_name}'
            AND tablename = '{table_name}';
        """
        )
    elif item_type == "constraints":
        contype = constraint_type[0]
        print(f"### constraints {constraint_type}, {contype}")
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
        print(f"### constraints query: \n{sql}")
        r = executor.execute(
            sql
        )
    else:
        raise ValueError("invalid item type")
    return {x[0] for x in r.fetchall()}


def create_primary_key_if_not_exists(
    op, constraint_name, table_name, columns, schema,
):
    """
    Create a primary key in a table if it does not already exist.
    SQL (and PostgreSQL) does not support an IF NOT EXISTS option for this
    operation, so we have to do this the hard way.
    """
    bind = op.get_bind()
    pkey_constraint_names = get_schema_item_names(
        bind,
        "constraints",
        table_name=table_name,
        constraint_type="primary",
        schema_name=schema,
    )
    if constraint_name in pkey_constraint_names:
        logger.info(
            f"Primary key '{constraint_name}' already exists in "
            f"table '{table_name}': skipping create."
        )
    else:
        logger.info(
            f"Creating primary key '{constraint_name}' in table '{table_name}'."
        )
        op.create_primary_key(
            constraint_name=constraint_name,
            table_name=table_name,
            columns=columns,
            schema=schema,
        )
