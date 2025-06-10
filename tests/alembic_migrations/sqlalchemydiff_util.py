"""This is a reworking of (part of) the `sqlalchemydiff.util` module, adding
the following capabilities:

- Provide a function that sets up the database prior to applying migrations
  in `prepare_schema_from_models`. This tightens up setup code in tests.

"""

from sqlalchemy import create_engine


def prepare_schema_from_models(uri, sqlalchemy_base, schema_name, db_setup=None):
    """Creates the database schema from the ``SQLAlchemy`` models."""
    engine = create_engine(uri)
    if db_setup:
        db_setup(engine, schema_name=schema_name)
    sqlalchemy_base.metadata.create_all(engine)
