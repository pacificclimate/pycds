"""Basic tests of functions used by WA"""

def test_functions_exist(schema_name, views_sesh):
    functions = views_sesh.execute(f'''
        SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
        FROM information_schema.routines
            LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
        WHERE routines.specific_schema='{schema_name}'
        ORDER BY routines.routine_name, parameters.ordinal_position;
    ''').fetchall()
    print(f'functions in schema {schema_name}')
    for f in functions:
        print(f)
    assert {f[0] for f in functions} >= {'daysinmonth', 'effective_day'}
