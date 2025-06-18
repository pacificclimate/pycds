import pytest
from pytest import param

from sqlalchemy import text

# Database operations, which are mixed and matched to construct different tests.

a_ins_1 = """
    INSERT INTO a(x) VALUES (100), (200), (300), (400);
"""

a_upd_1 = """
    UPDATE a SET x = 101 WHERE a_id = 1;
    UPDATE a SET x = 102 WHERE a_id = 1;
    DELETE FROM a WHERE a_id = 1;
    
    UPDATE a SET x = 201 WHERE a_id = 2;
    UPDATE a SET x = 202 WHERE a_id = 2;
    
    UPDATE a SET x = 301 WHERE a_id = 3;
"""

a_ins_2 = """
    INSERT INTO a(x) VALUES (500), (600);
"""

a_upd_2 = """
    UPDATE a SET x = 203 WHERE a_id = 2;
    UPDATE a SET x = 302 WHERE a_id = 3;
    UPDATE a SET x = 401 WHERE a_id = 4;
"""

b_ins_1 = """
    INSERT INTO b(a_id, y) VALUES (2, 200), (3, 300), (4, 400);
"""

b_ins_2 = """
    INSERT INTO b(a_id, y) VALUES (5, 400);
"""

b_upd_1 = """
    UPDATE b SET y = 201 WHERE b_id = 1;
    UPDATE b SET a_id = 5 WHERE b_id = 1;
    
    UPDATE b SET a_id = 2 WHERE b_id = 2;
    UPDATE b SET a_id = 3 WHERE b_id = 3;
    UPDATE b SET a_id = 4 WHERE b_id = 4;
"""

c_ins_1 = """
    INSERT INTO c(a_id, b_id, z) VALUES (2, 1, 900), (2, 3, 901), (3, 2, 902);
"""


def print_table(sesh, name, id):
    results = sesh.execute(text(f"SELECT * FROM {name} ORDER BY {id}")).fetchall()
    print()
    print(f"Table {name}")
    print("-----")
    for row in results:
        print(row)
    print("-----")


# Note: Test cases below  are given readable labels using param(..., id="label").
# Test id's are (manually) copied from the operations parameter,
# e.g., "a_ins_1 + b_ins_1". Be careful about this when adding or changing
# tests.


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "operations, query, expected",
    [
        # Test that history records are correctly inserted.
        param(
            a_ins_1 + a_upd_1 + a_ins_2 + a_upd_2,
            "SELECT a_id, x, deleted, a_hx_id FROM a_hx ORDER BY a_hx_id",
            (
                # a_ins_1
                (1, 100, False, 1),
                (2, 200, False, 2),
                (3, 300, False, 3),
                (4, 400, False, 4),
                # a_upd_1
                (1, 101, False, 5),
                (1, 102, False, 6),
                (1, 102, True, 7),
                (2, 201, False, 8),
                (2, 202, False, 9),
                (3, 301, False, 10),
                # a_ins_2
                (5, 500, False, 11),
                (6, 600, False, 12),
                # a_upd_2
                (2, 203, False, 13),
                (3, 302, False, 14),
                (4, 401, False, 15),
            ),
            id="a_hx",
        ),
        # The following cases are largely to test that the FK to a_hx in b_hx is
        # correctly set. Differently sequenced updates to collections a and b
        # exercise this.
        # Simple operations on a before b
        param(
            a_ins_1 + b_ins_1,
            "SELECT b_id, a_id, y, deleted, a_hx_id FROM b_hx ORDER BY b_hx_id",
            (
                (1, 2, 200, False, 2),
                (2, 3, 300, False, 3),
                (3, 4, 400, False, 4),
            ),
            id="b_hx: a_ins_1 + b_ins_1",
        ),
        # Simple case with operations on a following those on b; same expected result
        param(
            a_ins_1 + b_ins_1 + a_upd_1,
            "SELECT b_id, a_id, y, deleted, a_hx_id FROM b_hx ORDER BY b_hx_id",
            (
                (1, 2, 200, False, 2),
                (2, 3, 300, False, 3),
                (3, 4, 400, False, 4),
            ),
            id="b_hx: a_ins_1 + b_ins_1 + a_upd_1",
        ),
        # More operations on a before b
        param(
            a_ins_1 + a_upd_1 + b_ins_1 + a_upd_2,
            "SELECT b_id, a_id, y, deleted, a_hx_id FROM b_hx ORDER BY b_hx_id",
            (
                (1, 2, 200, False, 9),
                (2, 3, 300, False, 10),
                (3, 4, 400, False, 4),
            ),
            id="b_hx: a_ins_1 + a_upd_1 + b_ins_1 + a_upd_2",
        ),
        # Yet more operations on a before b
        param(
            a_ins_1 + a_upd_1 + a_upd_2 + b_ins_1,
            "SELECT b_id, a_id, y, deleted, a_hx_id FROM b_hx ORDER BY b_hx_id",
            (
                (1, 2, 200, False, 11),
                (2, 3, 300, False, 12),
                (3, 4, 400, False, 13),
            ),
            id="b_hx: a_ins_1 + a_upd_1 + a_upd_2  + b_ins_1",
        ),
        # Test c_hx, which has two history FKs. Primary test case goals: both filled,
        # correct values, correct order (tests must use hx FKs with different values).
        # Simple operations on a, b, then c.
        param(
            a_ins_1 + b_ins_1 + c_ins_1,
            "SELECT c_id, a_id, b_id, z, deleted, a_hx_id, b_hx_id FROM c_hx ORDER BY c_hx_id",
            (
                (1, 2, 1, 900, False, 2, 1),
                (2, 2, 3, 901, False, 2, 3),
                (3, 3, 2, 902, False, 3, 2),
            ),
            id="c_hx: a_ins_1 + b_ins_1 + c_ins_1",
        ),
        # Simple case with operations on a, b after c; same expected result.
        param(
            a_ins_1 + b_ins_1 + c_ins_1 + a_ins_2 + b_ins_2,
            "SELECT c_id, a_id, b_id, z, deleted, a_hx_id, b_hx_id FROM c_hx ORDER BY c_hx_id",
            (
                (1, 2, 1, 900, False, 2, 1),
                (2, 2, 3, 901, False, 2, 3),
                (3, 3, 2, 902, False, 3, 2),
            ),
            id="c_hx: a_ins_1 + b_ins_1 + c_ins_1 + a_ins_2 + b_ins_2",
        ),
        # More operations on a, b before c; different expected result.
        param(
            a_ins_1 + b_ins_1 + a_ins_2 + b_ins_2 + a_upd_1 + b_upd_1 + c_ins_1,
            "SELECT c_id, a_id, b_id, z, deleted, a_hx_id, b_hx_id FROM c_hx ORDER BY c_hx_id",
            (
                (1, 2, 1, 900, False, 11, 6),
                (2, 2, 3, 901, False, 11, 8),
                (3, 3, 2, 902, False, 12, 7),
            ),
            id="c_hx: a_ins_1 + b_ins_1 + a_ins_2 + b_ins_2 + a_upd_1 + b_upd_1 + c_ins_1",
        ),
    ],
)
def test_history(operations, query, expected, sesh_with_test_tables):
    """
    Test contents of history tables after operations.
    This is actually a very generic test: do ops, query, check results.
    """
    sesh = sesh_with_test_tables
    sesh.execute(text(operations))
    # Listings of these tables are useful for figuring out the expected
    # results for complex tests, such as those for c_hx.
    print_table(sesh, "a_hx", "a_hx_id")
    print_table(sesh, "b_hx", "b_hx_id")
    print_table(sesh, "c_hx", "c_hx_id")
    result = sesh.execute(text(query)).fetchall()
    assert tuple(result) == expected


@pytest.mark.usefixtures("new_db_left")
def test_mod_values_enforcement(sesh_with_test_tables):
    """
    Test enforcement of mod_time, mod_user in both primary and history table.
    """
    sesh = sesh_with_test_tables

    # Get expected values for mod_time, mod_user.
    # Note: now() returns the same value within a single transaction; a test session
    # apparently runs in a single transaction.
    expected = sesh.execute(text("SELECT now()::timestamp, current_user")).fetchone()

    # Attempt to override mod_time, mod_user
    sesh.execute(
        text(
            "INSERT INTO a(x, mod_time, mod_user) "
            "VALUES (100, '2000-01-01 00:00'::timestamp, 'naughty')"
        )
    )

    # Check that values set in INSERT are overridden with correct values.
    result = sesh.execute(text("SELECT mod_time, mod_user FROM a")).first()
    assert result == expected
    result = sesh.execute(text("SELECT mod_time, mod_user FROM a_hx")).first()
    assert result == expected
