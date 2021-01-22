from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


def get_postgresql_version(executor):
    """
    Return PostgreSQL version as a tuple of integers.

    This function is only works PostgreSQL. On other DBMSs, it will raise
    a ValueError.

    This has no use at present but going to leave it here in case it does again
    in future.
    """
    query = "SELECT setting FROM pg_settings WHERE name = 'server_version'"
    try:
        version = executor.execute(query).first()[0]
    except ProgrammingError:
        raise ValueError(
            f"Query '{query}' caused an error. "
            f"Possibly we are not running on PostgreSQL."
        )
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
