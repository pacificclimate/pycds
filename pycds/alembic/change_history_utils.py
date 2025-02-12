"""
Utility functions supporting change history migrations.
These are Python functions, not database functions, and they are used in coding the
migration. Naming functions do correspond to equivalent database functions used by
the change history functionality.
"""
from typing import Iterable, Any

from alembic import op

from pycds import get_schema_name


schema_name = get_schema_name()


def qualified_name(name: str, schema=schema_name) -> str:
    prefix = f"{schema}." if schema else ""
    return f"{prefix}{name}"


def pri_table_name(collection_name: str, schema=schema_name) -> str:
    return qualified_name(collection_name, schema=schema)


def hx_table_name(collection_name: str, **kwargs) -> str:
    return f"{pri_table_name(collection_name, **kwargs)}_hx"


def hx_id_name(collection_name: str, **kwargs):
    return f"{collection_name}_hx_id"


def hx_id_seq_name(collection_name: str, **kwargs):
    """Return the name of the sequence that generates the primary id of the history table.
    This name is the default name generated by Postgres."""
    return f"{hx_table_name(collection_name)}_{hx_id_name(collection_name)}_seq"


def sql_array(a: Iterable[Any]) -> str:
    return f"{{{', '.join(a)}}}"


def add_history_cols_to_primary(
    collection_name: str,
    columns: tuple[str] = (
        "mod_time timestamp without time zone NOT NULL DEFAULT NOW()",
        'mod_user character varying(64) COLLATE pg_catalog."default" '
        "   NOT NULL DEFAULT CURRENT_USER",
    ),
):
    # op.add_column can add only one column at a time.
    # Tables can be large, so for efficiency we add all columns in one command.
    add_columns = ", ".join(f"ADD COLUMN {c}" for c in columns)
    op.execute(f"ALTER TABLE {pri_table_name(collection_name)} {add_columns}")


def drop_history_cols_from_primary(
    collection_name: str, columns: tuple[str] = ("mod_time", "mod_user")
):
    drop_columns = ", ".join(f"DROP COLUMN {c}" for c in columns)
    op.execute(f"ALTER TABLE {pri_table_name(collection_name)} {drop_columns}")


def create_history_table(collection_name: str, foreign_tables: list[tuple[str, str]]):
    # Create the history table. We can't use Alembic create_table here because it doesn't
    # support the LIKE syntax we need.
    columns = ", ".join(
        (
            f"  LIKE {pri_table_name(collection_name)} INCLUDING ALL EXCLUDING INDEXES",
            f"  deleted boolean DEFAULT false",
            f"  {hx_id_name(collection_name)} int "
            f"      PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY",
        )
        + tuple(
            f"{hx_id_name(ft_table_name)} int "
            f"  REFERENCES {hx_table_name(ft_table_name)}({hx_id_name(ft_table_name)})"
            for ft_table_name, _ in (foreign_tables or tuple())
        )
    )
    op.execute(f"CREATE TABLE {hx_table_name(collection_name)} ({columns})")


def drop_history_table(collection_name: str):
    # Alternative: use op.drop_table
    op.execute(f"DROP TABLE {hx_table_name(collection_name)}")


def create_history_table_indexes(
    collection_name: str,
    pri_id_name: str,
    foreign_tables: list[tuple[str, str]],
    extras=None,
):
    """
    Create indexes on the history table. For analysis on what indexes are needed,
    see https://github.com/pacificclimate/pycds/issues/228
    """

    for columns in (
        # Index on primary table primary key, mod_time, mod_user
        ([pri_id_name], ["mod_time"], ["mod_user"])
        # Index on all foreign main table primary keys
        + tuple([ft_pk_name] for _, ft_pk_name in (foreign_tables or tuple()))
        # Index on all foreign history table primary keys
        + tuple(
            [hx_id_name(ft_table_name)]
            for ft_table_name, _ in (foreign_tables or tuple())
        )
        + (extras or tuple())
    ):
        # How much do we care about index naming? SQLAlchemy uses a different pattern than
        # appears typical in CRMP.
        op.create_index(
            None,  # Use default SQLAlchemy index name.
            hx_table_name(collection_name, schema=None),
            columns,
            schema=schema_name,
        )


def populate_history_table(
    collection_name: str,
    pri_id_name: str,
    foreign_tables: list[tuple[str, str]],
    limit: int = None,
):
    """
    Populate the history table with data from the main table, in order of item id (main
    table primary key).
    That ordering guarantees that the newly generated history id's will be in the
    same order, which is required for it to be a valid history table.
    We include the history FKs in the initial population, because to do it any other
    way proves infeasible for large tables (obs_raw) in memory and/or time.
    """

    # Foreign tables are used in common table expressions (CTEs) that provide the latest
    # foreign table history id's. A series of related objects are generated from the
    # foreign table definitions: the CTE names, the CTE definitions, and their usages
    # within the query that populates the target history table.

    foreign_tables = foreign_tables or tuple()

    conditional_comma = "," if len(foreign_tables) > 0 else ""

    ft_cte_names = tuple(
        f"{ft_table_name}_latest" for ft_table_name, ft_pk_name in foreign_tables
    )
    ft_cte_name_list = ", ".join(ft_cte_names)

    ft_cte_defns = tuple(
        f"""
        {cte_name} AS (
            SELECT {ft_pk_name}, max({hx_id_name(ft_table_name)}) val 
            FROM {hx_table_name(ft_table_name)} 
            GROUP BY {ft_pk_name}
        )        
        """
        for cte_name, (ft_table_name, ft_pk_name) in zip(ft_cte_names, foreign_tables)
    )
    ft_cte_list = ", ".join(ft_cte_defns)

    ft_cte_value_list = ", ".join(f"{cte_name}.val" for cte_name in ft_cte_names)

    ft_where_conditions = tuple(
        f"main.{ft_pk_name} = {cte_name}.{ft_pk_name}"
        for cte_name, (ft_table_name, ft_pk_name) in zip(ft_cte_names, foreign_tables)
    )
    ft_where_clause = (
        f"WHERE {' AND '.join(ft_where_conditions)}"
        if len(ft_where_conditions) > 0
        else ""
    )

    stmt = f"""
        {"WITH" if len(foreign_tables) > 0 else ""}
        {ft_cte_list}
        INSERT INTO {hx_table_name(collection_name)}
        SELECT main.*, false, 
            nextval('{hx_id_seq_name(collection_name)}'::regclass) 
            {conditional_comma} {ft_cte_value_list}
        FROM {pri_table_name(collection_name)} main  
            {conditional_comma} {ft_cte_name_list}
        {ft_where_clause}
        ORDER BY main.{pri_id_name}        
    """
    op.execute(stmt)


def update_obs_raw_history_FKs(suspend_synchronous_commit: bool = False):
    """
    Update the history FKs in obs_raw, in bulk.

    This method would be easy to generalize to other tables with different FK
    collections, but at the time of writing, only obs_raw needs bulk FK updates, and we
    already have the query in hand.
    """

    synchronous_commit = op.get_bind().execute("show synchronous_commit").scalar()
    print("## synchronous_commit", synchronous_commit)
    if suspend_synchronous_commit:
        synchronous_commit = op.get_bind().execute("show synchronous_commit").scalar()
        print("## synchronous_commit", synchronous_commit)
        op.execute("SET synchronous_commit = off")

    # TODO: Rewrite as SA query?
    op.execute(
        """
        WITH v as (
            SELECT vars_id, max(meta_vars_hx_id) latest
            FROM meta_vars_hx
            GROUP BY vars_id
        ),
        h as (
            SELECT history_id, max(meta_history_hx_id) latest
            FROM meta_history_hx
            GROUP BY history_id
        )
        UPDATE obs_raw_hx o
        SET meta_vars_hx_id = v.latest, meta_history_hx_id = h.latest
        FROM v, h
        WHERE o.vars_id = v.vars_id
        AND o.history_id = h.history_id        
        """
    )

    if suspend_synchronous_commit:
        op.execute(f"SET synchronous_commit = {synchronous_commit}")


def create_primary_table_triggers(collection_name: str, prefix: str = "t100_"):
    # Trigger: Enforce mod_time and mod_user values on primary table.
    op.execute(
        f"CREATE TRIGGER {prefix}primary_control_hx_cols "
        f"    BEFORE INSERT OR DELETE OR UPDATE "
        f"    ON {pri_table_name(collection_name)} "
        f"    FOR EACH ROW "
        f"    EXECUTE FUNCTION {qualified_name('hxtk_primary_control_hx_cols')}()"
    )

    # Trigger: Append history records to history table when primary updated.
    op.execute(
        f"CREATE TRIGGER {prefix}primary_ops_to_hx "
        f"    AFTER INSERT OR DELETE OR UPDATE "
        f"    ON {pri_table_name(collection_name)} "
        f"    FOR EACH ROW "
        f"    EXECUTE FUNCTION {qualified_name('hxtk_primary_ops_to_hx')}()"
    )


def create_history_table_triggers(
    collection_name: str, foreign_tables: list, prefix: str = "t100_"
):
    # Trigger: Add foreign key values to each record inserted into history table.
    ft_args = (
        f"'{sql_array(sql_array(pair) for pair in foreign_tables)}'"
        if foreign_tables
        else ""
    )
    op.execute(
        f"CREATE TRIGGER {prefix}add_foreign_hx_keys "
        f"    BEFORE INSERT "
        f"    ON {hx_table_name(collection_name)} "
        f"    FOR EACH ROW "
        f"    EXECUTE FUNCTION {qualified_name('hxtk_add_foreign_hx_keys')}({ft_args})"
    )


def drop_history_triggers(collection_name: str, prefix: str = "t100_"):
    op.execute(
        f"DROP TRIGGER {prefix}primary_control_hx_cols "
        f"ON {pri_table_name(collection_name)}"
    )
    op.execute(
        f"DROP TRIGGER {prefix}primary_ops_to_hx "
        f"ON {pri_table_name(collection_name)}"
    )
    op.execute(
        f"DROP TRIGGER {prefix}add_foreign_hx_keys "
        f"ON {hx_table_name(collection_name)}"
    )
