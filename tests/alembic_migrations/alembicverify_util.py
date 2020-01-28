"""This is a reworking of the `alembicverify.util` module, adding the following
capabilities:

- Provide a function that sets up the database prior to applying migrations
  in `prepare_schema_from_migrations`. This tightens up setup code in tests.

- Provide alembic EnvironmentContext.config() arguments. This allows the
  test environment to be configured in the same way as the real migration
  environment (in `env.py`).

TODO: Submit a PR to alembic-verify adding these capability.
"""

from alembic import command
from alembic.config import Config
from alembic.environment import EnvironmentContext  # pylint: disable=E0401
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine


def prepare_schema_from_migrations(uri, config, db_setup=None, revision="head"):
    """Applies migrations to a database.

    :param string uri: The URI for the database.
    :param function db_setup: A function that sets up the database prior to
        applying the migrations. Useful for creating required schema, etc.
        Called with argument `engine`.
    :param config: A :class:`alembic.config.Config` instance.
    :param revision: The revision we want to feed to the
        ``command.upgrade`` call. Normally it's either "head" or "+1".
    """
    engine = create_engine(uri)
    if db_setup:
        db_setup(engine)
    script = ScriptDirectory.from_config(config)
    command.upgrade(config, revision)
    return engine, script


def get_current_revision(config, engine, script, **kwargs):
    """Inspection helper. Get the current revision of a set of migrations. """
    return _get_revision(config, engine, script, **kwargs)


def get_head_revision(config, engine, script, **kwargs):
    """Inspection helper. Get the head revision of a set of migrations. """
    return _get_revision(config, engine, script, **kwargs, revision_type='head')


def _get_revision(config, engine, script, env_config=None, revision_type='current'):
    with engine.connect() as conn:
        with EnvironmentContext(config, script) as env_context:
            env_context.configure(
                conn,
                version_table="alembic_version",
                # version_table_schema=get_schema_name(),
                **(env_config or {})
            )
            if revision_type == 'head':
                revision = env_context.get_head_revision()
            else:
                migration_context = env_context.get_context()
                revision = migration_context.get_current_revision()
    return revision
