import pytest
from sqlalchemy.sql import text


@pytest.fixture()
def setup_metadata_objects():
    def f(sesh, metadata_name, metadata_id_name, metadata_cols):
        """
        :param sesh:
        :param metadata_name:
        :param metadata_id_name:
        :param metadata_cols:
        :return:

        metadata_cols is specified as a 2D array:
        [ [column_name, column_type], ... ]
        """

        def execute(s):
            return sesh.execute(text(s))

        metadata_hx_id_seq_name = f"{metadata_name}_hx_id_seq"
        metadata_hx_table_name = f"{metadata_name}_hx"
        metadata_col_names = [name for name, _ in metadata_cols]
        metadata_col_defns = [" ".join(col) for col in metadata_cols]

        # Sequence for the adjunct table ids
        execute(f"CREATE SEQUENCE {metadata_hx_id_seq_name}")

        # Metadata history table (adjunct table)
        execute(
            f"""
CREATE TABLE {metadata_hx_table_name}
(
    -- History-specific columns
    internal_id integer NOT NULL,
    deleted boolean NOT NULL DEFAULT FALSE,
    create_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER,
    -- Metadata columns
    {metadata_id_name} integer NOT NULL,
    {", ".join(metadata_col_defns)}
)
        """
        )

        execute(
            f"""
CREATE OR REPLACE VIEW {metadata_name} AS
SELECT DISTINCT ON ({metadata_id_name}) 
    create_time AS mod_time,
    creator,
    {metadata_id_name},
    {", ".join(metadata_col_names)}
FROM {metadata_hx_table_name}
WHERE {metadata_id_name} NOT IN (SELECT {metadata_id_name} FROM {metadata_hx_table_name} WHERE deleted)
ORDER BY {metadata_id_name} DESC,
         create_time DESC -- very important to ensure latest record for each metadata_id is retrieved
        """
        )

        execute(
            f"""
CREATE TRIGGER update_metadata2_generic
    INSTEAD OF INSERT OR UPDATE OR DELETE
    ON {metadata_name} -- the view
    FOR EACH ROW
EXECUTE FUNCTION update_metadata('{metadata_name}', '{metadata_id_name}')
        """
        )

    return f
