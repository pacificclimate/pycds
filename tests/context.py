import pytest
from pycds.context import parse_standard_table_privileges


@pytest.mark.parametrize(
    "stp, expected",
    [
        ("", []),
        (
            "admin: ALL; user: SELECT,INSERT",
            [("admin", ["ALL"]), ("user", ["SELECT", "INSERT"])],
        ),
    ],
)
def test_parse_standard_table_privileges(stp, expected):
    result = list(parse_standard_table_privileges(stp))
    assert result == expected
