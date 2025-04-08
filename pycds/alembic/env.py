"""
Customized alembic environment manager.
Adapted from https://gist.github.com/twolfson/4bc5813b022178bd7034

This manager enables command-line specification of the database to migrate.
This simplifies testing and use in various environments, e.g., local dev
machine vs. compute node (with test and prod databases on compute node).

The database to migrate is specified using the command-line argument `-x`,
as follows::

    alembic -x db=<db-name> upgrade ...

This syntax is now required; i.e., `-x db=<db-name>` cannot be omitted from
the command.

The `<db-name>` section must appear in `alembic.ini`, with a
`sqlalchemy.url =` line.
For example, to use `alembic -x db=test upgrade ...`::

    [test]
    sqlalchemy.url = sqlite:////path/to/database/test.sqlite
"""

# TODO: Respect schema name for `revision --autogenerate` functionality.
#   Currently, the schema name is respected for upgrade and downgrade, but
#   Alembic fails when revision --autogenerate is used. Which is a problem.
#
# Part of the problem is for revision --autogenerate, Alembic looks for
# an `alembic_version` table, and in an empty database can't find one.
# That can be solved by running `alembic -x db=dev stamp head` (crmp schema
# implicit). Following that, revision --autogenerate works. (for crmp schema).
#
# Next test is to see if this works for other schema.
# (1) `PYCDS_SCHEMA_NAME=other alembic -x db=dev stamp head` works.
# (2) `PYCDS_SCHEMA_NAME=other alembic -x db=dev revision --autogenerate ...` works
#
# Now to test if it works when crmp contains an upgraded schema and other
# does not. This works.

from alembic import context
from sqlalchemy import engine_from_config, pool
from logging.config import fileConfig

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Existence of `config.config_file_name` is a proxy for "are we in a live
# environment or a test env?" There are better ways to do this, but this is
# expedient for working with alembic-verify.
is_live_env = config.config_file_name is not None

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if is_live_env:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
from pycds import Base

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


if is_live_env:
    # Obtain command-line specification of database to run migration against.
    cmd_kwargs = context.get_x_argument(as_dictionary=True)
    if "db" not in cmd_kwargs:
        raise Exception(
            "We couldn't find `db` in the CLI arguments. "
            "Please verify `alembic` was run with `-x db=<db_name>` "
            "(e.g. `alembic -x db=development upgrade head`)"
        )
    db_name = cmd_kwargs["db"]


def include_object(object, name, type_, reflected, compare_to):
    """Include only objects that belong to the specified schema."""
    if type_ == "table":
        object_schema = object.metadata.schema
    elif type_ == "column":
        object_schema = object.table.metadata.schema
    elif type_ == "index":
        object_schema = object.table.metadata.schema
    elif type_ == "foreign_key_constraint":
        object_schema = object.table.metadata.schema
    elif type_ == "unique_constraint":
        object_schema = object.table.metadata.schema
    else:
        print(
            f"include_object: "
            f"Unknown object type"
            f"name = {name} type_ = {type_}, "
            f"reflected = {reflected}, "
            f"object = {object}"
        )
        raise ValueError(f"Unknown object type: {type_}")

    include = not reflected and object_schema == target_metadata.schema
    print(
        f"include_object: "
        f'{"INCLUDE" if include else "EXCLUDE"} '
        f"name = {name} type_ = {type_}, "
        f"reflected = {reflected}, "
        f"schema = {object_schema}, "
        f"include = {include}"
    )
    return include


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # TODO: Use the same mechanism for loading the db config on top of the
    #   alembic config (see `run_migrations_online()`) here?
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        version_table_schema=target_metadata.schema,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Load db config on top of alembic config. This enables CLI spec of
    # database to migrate.
    alembic_config = config.get_section(config.config_ini_section)
    if is_live_env:
        db_config = config.get_section(db_name)
        for key in db_config:
            alembic_config[key] = db_config[key]

    connectable = engine_from_config(
        alembic_config, prefix="sqlalchemy.", poolclass=pool.NullPool
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=target_metadata.schema,
            include_schemas=True,
            include_object=include_object,
            # render_as_batch=True,  # ??
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
