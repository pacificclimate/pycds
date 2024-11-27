# Utility functions

Eventually these should all go into schema `public`.
They should have a unique, short prefix (e.g., `mdhx_` to their names for this).

## Naming

```postgresql

CREATE OR REPLACE FUNCTION mdhx_collection_name_from_hx(hx_table_name text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $$
BEGIN
    RETURN regexp_replace(hx_table_name, '_hx$', '');
END;
$$;

CREATE OR REPLACE FUNCTION mdhx_view_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN 
        RETURN collection_name;
    END;
$$;

CREATE OR REPLACE FUNCTION mdhx_hx_table_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN 
        RETURN collection_name || '_hx';
    END;
$$;

CREATE OR REPLACE FUNCTION mdhx_hx_id_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN
        RETURN collection_name || '_hx_id';
    END;
$$;

CREATE OR REPLACE FUNCTION mdhx_hx_id_seq_name(collection_name text)
RETURNS text
    LANGUAGE 'plpgsql'
AS $$
    BEGIN
        RETURN mdhx_hx_table_name(collection_name) || '_'|| mdhx_hx_id_name(collection_name) || '_seq';
    END;
$$;
```

## Other

```postgresql
CREATE OR REPLACE PROCEDURE mdhx_set_history_attrs(inout rec record)
    LANGUAGE 'plpgsql'
AS
$$
BEGIN
    rec.mod_time = now();
    rec.creator = current_user;    
END;
$$;
```




