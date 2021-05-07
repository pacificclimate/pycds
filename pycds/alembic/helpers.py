from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


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
    executor, item_type, table_name=None, schema_name="crmp"
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
    else:
        raise ValueError("invalid item type")
    return {x[0] for x in r.fetchall()}
