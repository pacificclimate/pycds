from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError


def get_postgresql_version(executor):
    """
    Return PostgreSQL version as a tuple of integers.

    This function is very rough and ready. It will fail in unknown ways if it
    is run on any other database engine than PostgreSQL.

    This has no use at present but going to leave it here in case it does again
    in future.
    """
    items = executor.execute("SELECT version();").scalar().split(" ")
    if items[0] != "PostgreSQL":
        raise ValueError("Not running on PostgreSQL! Yikes!")
    return tuple(int(n) for n in items[1].split("."))


def db_supports_statement(engine, statement):
    """
    Return a boolean indicating whether this database instance can execute the
    statement. The statement is executed in a transaction and rolled back
    afterwards, so the effect on the database is null (so long as the statement
    itself can be executed in a transaction).

    Note: We must create a session to do this test. Creating a transaction
    directly from the session failed: the entire Alembic operation transaction
    was rolled back, not just the one supposedly enclosing this test execution.
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
