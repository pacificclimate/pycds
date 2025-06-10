import pytest
from sqlalchemy import text, func
from sqlalchemy.dialects.postgresql import array
from pycds import (
    get_schema_name,
    schema_func,
    Station,
    Obs,
    History,
    Variable,
    VarsPerHistory,
    variable_tags,
)

getstationvariabletable = schema_func.getstationvariabletable


@pytest.mark.skip
@pytest.mark.usefixtures("new_db_left")
def test_query_string(sesh_with_large_data):
    """Run getstationvariabletable on all stations in the database and print the
    returned query strings."""
    sesh = sesh_with_large_data
    q = sesh.query(Station)
    stations = list(q.all())
    print(f"Station count: {len(stations)}")

    climo = False
    for station in stations:
        station_id = station.id
        q = sesh.query(getstationvariabletable(station_id, climo))
        query = q.scalar()
        print()
        print(f"getstationvariabletable({station_id}, {climo}):")
        print(query)


@pytest.mark.usefixtures("new_db_left")
@pytest.mark.parametrize(
    "station_id",
    # A random selection of stations.
    [4137, 1213, 2313, 1313, 5136, 5634],
)
@pytest.mark.parametrize("climo", [False, True])
def test_run_query(station_id, climo, sesh_with_large_data, print_details=False):
    """Test the results of getstationvariabletable against direct queries for the
    table values. Arg `print_details` is for debugging and general curiosity."""
    sesh = sesh_with_large_data

    sesh.execute(text(f"SET search_path TO {get_schema_name()}, public"))

    # Refresh contributing matviews
    sesh.execute(VarsPerHistory.refresh())

    # Get the query returned by getstationvariabletable
    q = sesh.query(getstationvariabletable(station_id, climo))
    query = q.scalar()

    if print_details:
        print()
        print(f"getstationvariabletable({station_id}, {climo}):")
        print(query)

    # Execute the query, obtaining the station variable table.
    station_variable_table = sesh.execute(text(query))
    all_column_names = station_variable_table.keys()
    station_variable_table = list(station_variable_table)

    if print_details:
        print()
        print(f"Station variable table ({len(station_variable_table)} rows)")
        print(all_column_names)
        for row in station_variable_table:
            print(row)

    # Extract all variables related to this station, respecting the `climo` arg.
    q = (
        sesh.query(Variable)
        .select_from(VarsPerHistory)
        .join(Variable, Variable.id == VarsPerHistory.vars_id)
        .join(History, History.id == VarsPerHistory.history_id)
        .join(Station, Station.id == History.station_id)
        .filter(Station.id == station_id)
        .filter(
            variable_tags(Variable).contains(
                array(["climatology" if climo else "observation"])
            )
        )
        .order_by(Variable.id)
    )
    station_variables = list(q.all())
    if print_details:
        print()
        print(f"Station variables with data in database ({len(station_variables)})")
        for variable in station_variables:
            print(f"{variable.name}")

    # For each station variable, compare the contents of the station variable table
    # with variable values directly obtained from the database.
    for variable in station_variables:
        net_var_name = str(variable.name).lower()

        if print_details:
            print()
            print(f"Variable {net_var_name}")

        # Get the values direct from the database.
        q = (
            sesh.query(Obs.time, Obs.datum)
            .select_from(Obs)
            .join(History, History.id == Obs.history_id)
            .join(Station, Station.id == History.station_id)
            .join(Variable, Variable.id == Obs.vars_id)
            .filter(Station.id == station_id)
            .filter(func.lower(Variable.name) == net_var_name.lower())
            .order_by(Obs.time)
        )
        direct_results = list(q.all())

        if print_details:
            print()
            print("Direct query results")
            print("obs_time", net_var_name)
            for row in direct_results:
                print(row)

        # Extract the values from the station variable table, discarding None values,
        # which signify no value in database.
        reduced_svt = [
            (row.obs_time, getattr(row, net_var_name))
            for row in station_variable_table
            if getattr(row, net_var_name) is not None
        ]

        if print_details:
            print()
            print("Reduced query results")
            print("obs_time", net_var_name)
            for row in reduced_svt:
                print(row)

        assert reduced_svt == direct_results
