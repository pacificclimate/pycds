import pytest
from sqlalchemy.sql import text

import pycds.alembic.info
from pycds import get_schema_name


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "metadata_name, metadata_id_name, metadata_cols, operations, final_metadata",
    [
        (
            "metadata",
            "metadata_id",
            [
                ["item1", "text"],
                ["item2", "text"],
            ],
            [
                "INSERT INTO metadata(item1, item2) VALUES ('A', 'B')",
                "INSERT INTO metadata(item1, item2) VALUES ('C', 'D'), ('E', 'F')",
                "UPDATE metadata SET item1 = 'X' WHERE metadata_id = 3",
                "UPDATE metadata SET item2 = 'Y' WHERE metadata_id = 1",
                "UPDATE metadata SET item2 = 'Q' WHERE item1 <> 'X'",
                "DELETE FROM metadata WHERE item1 = 'X'",
                "INSERT INTO metadata(item1, item2) VALUES ('L', 'M')",
            ],
            [("L", "M"), ("C", "Q"), ("A", "Q")],
        ),
    ],
)
@pytest.mark.parametrize("print_info", [True])
def test_update_metadata(
    sesh_in_prepared_schema_left,
    setup_metadata_objects,
    metadata_name,
    metadata_id_name,
    metadata_cols,
    operations,
    final_metadata,
    print_info,
):
    sesh = sesh_in_prepared_schema_left

    def execute(s, print_=False):
        result = sesh.execute(text(s))
        # Commit is required to ensure that sql is executed immediately,
        # not batched. Batching causes now() to be the same instant for
        # all items, which causes big problems.
        sesh.commit()
        if print_:
            print("\n------")
            print(s)
            print("\nmetadata_hx")
            for row in sesh.execute(f"SELECT * FROM {metadata_name}_hx"):
                print(row)
            print("\nmetadata")
            for row in sesh.execute(f"SELECT * FROM {metadata_name}"):
                print(row)
        return result

    execute(f"SET search_path TO {get_schema_name()}, public")

    # Set up sequence, view and table
    setup_metadata_objects(sesh, metadata_name, metadata_id_name, metadata_cols)

    # Perform the operations
    for op in operations:
        execute(op, print_=print_info)

    # Check the final state of the metadata view
    metadata_col_names = ", ".join(name for name, _ in metadata_cols)
    result = execute(f"SELECT {metadata_col_names} FROM {metadata_name}")
    for expected, actual in zip(final_metadata, result):
        assert expected == tuple(actual)
