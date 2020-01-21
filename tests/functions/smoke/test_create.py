"""Smoke tests that the  PyCDS database functions can be created.
This does not even test whether they can be executed, let alone that they are
correct, only that their DDL does not contain a syntax or other superficial
error."""
import pytest
import pycds.functions


@pytest.mark.parametrize('name', pycds.functions.__all__)
def test_create(pycds_sesh, name):
    """Test that functions can be created.
    The test is that this does not raise an exception"""
    pycds_sesh.execute(getattr(pycds.functions, name)())


def test_schema_content(functions_sesh, schema_name):
    """Test that functions get created in the right schema with the right names.
    """
    names = functions_sesh.execute(f'''
        select routine_name 
        from information_schema.routines 
        where specific_schema = '{schema_name}'
    ''').fetchall()
    assert {row[0] for row in names} >= set(pycds.functions.__all__)


@pytest.mark.parametrize('f_name, required, substring', [
    ('daily_ts', True, '"%"'),
    ('daily_ts', False, '%%'),
    ('monthly_ts', True, '"%"'),
    ('monthly_ts', False, '%%'),
])
def test_function_definition(
        functions_sesh, schema_name,
        f_name, required, substring
):
    """Check that any wonky encoding in the function definitions resolved to
    the desired final content for the function definition.

    `required` is a Boolean specifying required substring (True) or forbidden
        substring (False)
    `substring` is a substring that must or must not appear in the function
        definition, according to `required`

    This test exists because SQLAlchemy DDL apparently runs its argument
    string through old-school Python %-formatting, which means that %'s
    desired in the function definition must be doubled up. Phew.
    """
    defn = functions_sesh.execute(f'''
        SELECT pg_catalog.pg_get_functiondef(
            '{schema_name}.{f_name}'::pg_catalog.regproc::pg_catalog.oid
        )
    ''').scalar()
    if required:
        assert substring in defn
    else:
        assert substring not in defn
