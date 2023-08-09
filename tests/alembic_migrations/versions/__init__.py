from pycds.database import get_schema_item_names


def check_matviews(engine, matview_defns, schema_name, matviews_present):
    """
    Test helper.

    Check whether matviews are present or absent, and conversely whether tables with
    same name are absent or present.
    """
    matview_names = set(matview_defns)
    if matviews_present:
        # Check that table has been replaced with matview
        names = get_schema_item_names(engine, "tables", schema_name=schema_name)
        assert (
            names & matview_names == set()
        ), "matview(s) should not be present as table(s)"
        names = get_schema_item_names(engine, "matviews", schema_name=schema_name)
        assert names >= matview_names, "matview(s) should be present as matviews"

        # Check that indexes were installed too
        for table_name, contents in matview_defns.items():
            names = get_schema_item_names(
                engine,
                "indexes",
                table_name=table_name,
                schema_name=schema_name,
            )
            assert names == contents["indexes"], "matview indexes should be installed"
    else:
        # Check that matview is not present and table is
        names = get_schema_item_names(engine, "matviews", schema_name=schema_name)
        assert (
            names & matview_names == set()
        ), "matview(s) should not be present as matviews"
        names = get_schema_item_names(engine, "tables", schema_name=schema_name)
        assert names >= matview_names

        # Check that matview indexes are not present
        for table_name, contents in matview_defns.items():
            names = get_schema_item_names(
                engine,
                "indexes",
                table_name=table_name,
                schema_name=schema_name,
            )
            assert (
                names & contents["indexes"] == set()
            ), "table indexes should not be present"
