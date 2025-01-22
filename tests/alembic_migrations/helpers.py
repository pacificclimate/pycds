import sqlalchemy.types
from sqlalchemy import select, func, and_, MetaData, Table, insert, update, delete
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.operators import isnot_distinct_from
from sqlalchemy.types import TIMESTAMP, VARCHAR, BOOLEAN, INTEGER

from pycds import get_schema_name
from pycds.alembic.change_history_utils import hx_id_name
from pycds.alembic.change_history_utils import pri_table_name, hx_table_name
from pycds.database import get_schema_item_names


def check_column(table, col_name, col_type=None, present=True):
    """Check that expected column is present in a table and of specified type"""
    if present:
        assert col_name in table.columns
        assert isinstance(table.columns[col_name].type, col_type)
    else:
        assert col_name not in table.columns


def check_triggers(table, expected, present=True):
    """Check that expected triggers are present on a table."""
    triggers = table.bind.execute(
        text(
            f"""
select trigger_name, array_agg(event_manipulation order by event_manipulation asc)
from information_schema.triggers
where trigger_schema = :schema_name
and event_object_schema = :schema_name
and event_object_table = :table_name
group by trigger_name
"""
        ),
        {"schema_name": table.schema, "table_name": table.name},
    ).fetchall()
    for item in expected:
        if present:
            assert item in triggers
        else:
            assert item not in triggers


def check_orm_actual_tables_match(engine, orm_table, schema_name=get_schema_name()):
    """Check that table defined in ORM matches the actual table in the database.
    This method is useful in smoke tests."""

    # Check actual table existence
    names = set(get_schema_item_names(engine, "tables", schema_name=schema_name))
    assert orm_table.__tablename__ in names

    # Reflect table
    metadata = MetaData(schema=schema_name, bind=engine)
    actual_table = Table(orm_table.__tablename__, metadata, autoload_with=engine)

    # Check that table columns match
    def type_match(class1, class2):
        return (
            # This is usually the case if they match
            issubclass(class1, class2)
            or issubclass(class2, class1)
            # Ad-hocery around dialect vs non-dialect types
            or all(issubclass(c, sqltypes._AbstractInterval) for c in (class1, class2))
        )

    # This checks column order as well as type.
    for actual_col, orm_col in zip(actual_table.columns, orm_table.__table__.columns):
        assert actual_col.name == orm_col.name
        # print(
        #     f"{actual_col.name}: actual {actual_col.type.__class__} orm:  {orm_col.type.__class__}"
        # )
        assert type_match(
            actual_col.type.__class__, orm_col.type.__class__
        ), f"{actual_col.name}: actual {actual_col.type.__class__} orm:  {orm_col.type.__class__}"


def check_history_tracking_upgrade(
    engine,
    table_name: str,
    pri_key_name: str,
    foreign_keys: list[tuple[str, str]],
    schema_name: str,
    pri_columns_added: tuple[tuple[str, sqlalchemy.types]] = (
        ("mod_time", TIMESTAMP),
        ("mod_user", VARCHAR),
    ),
    tg_prefix: str = "t100_",
):
    """Check that the results of a history tracking upgrade are as expected"""

    names = set(get_schema_item_names(engine, "tables", schema_name=schema_name))
    metadata = MetaData(schema=schema_name, bind=engine)

    # Primary table: columns added
    pri_name = pri_table_name(table_name, schema=None)
    assert pri_name in names
    pri_table = Table(pri_name, metadata, autoload_with=engine)
    for col_name, col_type in pri_columns_added:
        check_column(pri_table, col_name, col_type)

    # History table columns: primary plus additional columns
    hx_name = hx_table_name(table_name, schema=None)
    assert hx_name in names
    hx_table = Table(hx_name, metadata, autoload_with=engine)
    for col in pri_table.columns:
        check_column(hx_table, col.name, col.type.__class__)
    check_column(hx_table, "deleted", BOOLEAN)
    check_column(hx_table, hx_id_name(table_name), INTEGER)
    for fk_table_name, fk_key_name in foreign_keys:
        check_column(hx_table, fk_key_name, INTEGER)
        check_column(hx_table, hx_id_name(fk_table_name), INTEGER)

    # History table indexes. This test does not check index type, but it
    # does check what columns are in each index.
    assert {
        (pri_key_name,),
        ("mod_time",),
        ("mod_user",),
    } | {
        (hx_id_name(fk_table_name),) for fk_table_name, _ in (foreign_keys or tuple())
    } == {tuple(c.name for c in index.columns) for index in hx_table.indexes}

    # Triggers
    check_triggers(
        pri_table,
        [
            (
                f"{tg_prefix}primary_control_hx_cols",
                "{DELETE,INSERT,UPDATE}",
            ),
            (
                f"{tg_prefix}primary_ops_to_hx",
                "{DELETE,INSERT,UPDATE}",
            ),
        ],
    )

    check_triggers(
        hx_table,
        [
            (
                f"{tg_prefix}add_foreign_hx_keys",
                "{INSERT}",
            )
        ],
    )


def table_count(sesh, table):
    return sesh.execute(select(func.count("*")).select_from(table)).scalar()


def check_history_tracking_downgrade(
    engine,
    table_name: str,
    schema_name: str,
    pri_columns_dropped: tuple[str] = ("mod_time", "mod_user"),
    tg_prefix: str = "t100_",
):
    """Check that the results of a history tracking downgrade are as expected"""

    names = set(get_schema_item_names(engine, "tables", schema_name=schema_name))
    metadata = MetaData(schema=schema_name, bind=engine)

    # Primary table: columns dropped
    pri_name = pri_table_name(table_name, schema=None)
    assert pri_name in names
    pri_table = Table(pri_name, metadata, autoload_with=engine)
    for column in pri_columns_dropped:
        check_column(pri_table, column, present=False)

    # History table: dropped
    hx_name = hx_table_name(table_name, schema=None)
    assert hx_name not in names

    # Triggers (primary): dropped
    check_triggers(
        pri_table,
        [
            (
                f"{tg_prefix}primary_control_hx_cols",
                "{DELETE,INSERT,UPDATE}",
            ),
            (
                f"{tg_prefix}primary_ops_to_hx",
                "{DELETE,INSERT,UPDATE}",
            ),
        ],
        present=False,
    )


def check_history_table_initial_contents(
    sesh,
    primary,
    history,
    primary_id,
    columns,
    foreign_tables,
    schema_name,
):
    """Check that the initial contents (immediately after copying) of the history table
    are as expected."""

    # Introspect tables and show what we got
    # metadata = MetaData(schema=schema_name, bind=sesh.get_bind())
    # for table_name in (primary.__tablename__, history.__tablename__):
    #     print()
    #     print("Table", table_name)
    #     table = Table(table_name, metadata, autoload=True)
    #     for column in table.columns:
    #         print("  Column", column)
    #     for index in table.indexes:
    #         print("  Index", index)

    # Count
    pri_count = table_count(sesh, primary)
    hx_count = table_count(sesh, history)
    assert pri_count == hx_count

    # Contents: check that every specified column matches between the two tables.
    stmt = (
        select(
            func.every(
                and_(
                    isnot_distinct_from(getattr(primary, col), getattr(history, col))
                    for col in columns
                )
            )
        )
        .select_from(primary)
        .join(history, primary.id == getattr(history, primary_id))
    )
    result = sesh.execute(stmt).scalar()
    assert result

    # Order: Check that history table order by history key is the same as the order
    # by primary id.
    def hx_table_pids(order_by: str):
        return (
            sesh.query(
                func.array_agg(
                    aggregate_order_by(
                        getattr(history, primary_id), getattr(history, order_by)
                    )
                ).label("pids")
            )
            .select_from(history)
            .subquery()
        )

    t1 = hx_table_pids(primary_id)
    t2 = hx_table_pids(hx_id_name(primary.__tablename__))
    result = sesh.query(t1.c.pids == t2.c.pids).scalar()
    assert result

    # Foreign keys: Check that foreign keys are non-null (value correctness is
    # checked in the migration that installs the history tracking utilities).
    if foreign_tables:
        assert sesh.query(
            func.every(
                and_(
                    getattr(history, hx_id_name(ft.__tablename__)).is_not(None)
                    for ft in foreign_tables
                )
            )
        ).scalar()


def check_history_tracking(
    sesh,
    primary_table,
    history_table,
    insert_info,
    update_info,
    delete_info,
    schema_name,
):
    """Perform some very cursory tests that history tracking is indeed working.
    Detailed tests are performed in the tests for the migration that installs the
    history features."""

    hx_id = getattr(history_table, hx_id_name(primary_table.__tablename__))

    def do_insert():
        sesh.add(primary_table(**insert_info["values"]))

    def do_update():
        sesh.execute(
            update(primary_table)
            .where(
                *(
                    getattr(primary_table, name) == update_info["where"][name]
                    for name in update_info["where"].keys()
                )
            )
            .values(
                {
                    getattr(primary_table, name): update_info["values"][name]
                    for name in update_info["values"].keys()
                }
            )
        )

    def do_delete():
        sesh.execute(
            delete(primary_table).where(
                *(
                    getattr(primary_table, name) == delete_info["where"][name]
                    for name in delete_info["where"].keys()
                )
            )
        )

    for what, op, info in (
        ("insert", do_insert, insert_info),
        ("update", do_update, update_info),
        ("delete", do_delete, delete_info),
    ):
        hx_count_pre = table_count(sesh, history_table)
        op()
        sesh.flush()
        # Check that a new history record was inserted
        hx_count_post = table_count(sesh, history_table)
        assert hx_count_post == hx_count_pre + 1
        # Check that it contains what we expect
        hx = sesh.scalars(select(history_table).order_by(hx_id.desc())).first()
        for key, value in info["check"].items():
            assert (
                getattr(hx, key) == value
            ), f"{what}: {key} == {getattr(hx, key)}, != {value}"
